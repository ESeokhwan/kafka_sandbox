/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.cluster.Partition
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{PartitionDescription, PartitionKey}
import org.apache.kafka.image.MetadataImage
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.UnifiedLog

import java.util.concurrent.{CompletableFuture, Executor, RejectedExecutionException, ScheduledFuture}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.util.control.NonFatal

/**
 * Refreshes deletion pins on every hosted source replica, including followers and future logs.
 * Reads never acquire indexer ownership: a lagging committed prefix is safe for retention.
 * The broker owns the shared executor and scheduler. No log scan or RPC runs in a partition callback.
 */
class GlobalSequenceRetentionManager private[server](
  replicas: ReplicaManager,
  router: IndexRoutingManager,
  executor: Executor,
  scheduler: Scheduler,
  requestTimeoutMs: Int,
  refreshIntervalMs: Int
) extends AutoCloseable with Logging {
  import IndexRoutingManager.RoutedResult

  private val states = mutable.Map.empty[PartitionKey, ReplicaState]
  private var closed = false

  private class ReplicaState(val key: PartitionKey, val tp: TopicPartition, val source: Partition) {
    var stopped = false
    var pending: CompletableFuture[RoutedResult[PartitionDescription]] = _
    var timer: ScheduledFuture[_] = _
    def close(): Unit = {
      stopped = true
      if (pending != null) pending.cancel(false)
      if (timer != null) timer.cancel(false)
    }
  }

  def onMetadataUpdate(image: MetadataImage): Unit = synchronized {
    if (closed) return
    val desired = mutable.Set.empty[PartitionKey]
    image.topics().topicsById().values().asScala.foreach { topic =>
      val configs = image.configs().configProperties(new ConfigResource(ConfigResource.Type.TOPIC, topic.name()))
      if (!Topic.isInternal(topic.name()) && java.lang.Boolean.parseBoolean(configs.getProperty(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG))) {
        topic.partitions().keySet().asScala.foreach { partitionId =>
          val key = new PartitionKey(topic.id(), partitionId)
          val tp = new TopicPartition(topic.name(), partitionId)
          replicas.onlinePartition(tp).foreach { source =>
            if (source.topicId.contains(topic.id())) {
              desired.add(key)
              states.get(key) match {
                case Some(existing) if existing.source eq source =>
                case previous =>
                  previous.foreach(_.close())
                  val state = new ReplicaState(key, tp, source)
                  states.put(key, state)
                  execute { refresh(state) }
              }
            }
          }
        }
      }
    }
    states.keysIterator.filterNot(desired.contains).toList.foreach(key => states.remove(key).foreach(_.close()))
  }

  private def execute(work: => Unit): Unit = {
    try executor.execute(() => work)
    catch { case _: RejectedExecutionException => () } // Broker shutdown leaves the last safe pin installed.
  }

  private def active(state: ReplicaState): Boolean =
    !closed && !state.stopped && states.get(state.key).contains(state) &&
      replicas.onlinePartition(state.tp).exists(_ eq state.source) && state.source.topicId.contains(state.key.topicId())

  private def schedule(state: ReplicaState): Unit = {
    if (active(state)) state.timer = scheduler.scheduleOnce("global-sequence-retention-refresh",
      () => execute { refresh(state) }, refreshIntervalMs)
    else state.close()
  }

  private def onError(state: ReplicaState, error: Throwable): Unit = {
    val cause = Errors.maybeUnwrapException(error)
    if (IndexRoutingManager.isRetryable(cause)) schedule(state)
    else {
      warn(s"Stopped global sequence retention refresh for ${state.key}; keeping the last confirmed deletion pin", cause)
      state.close()
    }
  }

  private def logs(source: Partition): Seq[UnifiedLog] = (source.log.toSeq ++ source.futureLog.toSeq).distinct

  private def refresh(state: ReplicaState): Unit = synchronized {
    if (!active(state)) { state.close(); return }
    // Capture exact log objects. A reply must never update a replacement log with the same name.
    val capturedLogs = logs(state.source)
    try {
      state.pending = router.describePartition(state.key, requestTimeoutMs)
      state.pending.whenComplete { (result, exception) => execute {
        synchronized {
          state.pending = null
          if (active(state)) {
            if (exception != null) onError(state, exception)
            else {
              if (router.isCurrent(state.key, result.coordinator)) {
                val nextOffset = result.value.committedProgress().toScala.map(_.resumeOffset()).getOrElse(0L)
                val currentLogs = logs(state.source)
                capturedLogs.filter(log => currentLogs.exists(_ eq log)).foreach { log =>
                  log.updateGlobalSequenceIndexedOffset(state.key.topicId(), nextOffset)
                }
              }
              schedule(state)
            }
          }
        }
      } }
    } catch { case NonFatal(error) => onError(state, error) }
  }

  override def close(): Unit = synchronized {
    closed = true
    states.values.foreach(_.close())
    states.clear()
  }
}
