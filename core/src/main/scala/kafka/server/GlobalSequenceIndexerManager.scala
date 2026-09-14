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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.utils.{KafkaThread, ThreadUtils}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorConfig
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PartitionKey
import org.apache.kafka.image.MetadataImage
import org.apache.kafka.server.util.Scheduler

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object GlobalSequenceIndexerManager {
  def apply(brokerId: Int, config: GlobalSequenceCoordinatorConfig, replicaManager: ReplicaManager,
            router: IndexRoutingManager, scheduler: Scheduler): GlobalSequenceIndexerManager = {
    val threadId = new AtomicInteger()
    val workers = Executors.newFixedThreadPool(config.indexerNumThreads(), runnable =>
      KafkaThread.daemon(s"global-sequence-indexer-$brokerId-${threadId.getAndIncrement()}", runnable))
    new GlobalSequenceIndexerManager(brokerId, replicaManager, new GlobalSequenceSourceReader(replicaManager),
      router, workers, scheduler, config.writeTimeoutMs(), config.indexerReadMaxBytes())
  }
}

/** Owns the shared workers and one indexing lifetime for each locally led, opted-in source partition. */
class GlobalSequenceIndexerManager private[server](
  brokerId: Int,
  replicaManager: ReplicaManager,
  reader: GlobalSequenceSourceReader,
  router: IndexRoutingManager,
  workers: ExecutorService,
  scheduler: Scheduler,
  requestTimeoutMs: Int,
  readMaxBytes: Int
) extends AutoCloseable {
  private val indexers = mutable.Map.empty[PartitionKey, GlobalSequencePartitionIndexer]
  private var closed = false

  /** Called by the metadata publisher after replica and log configuration updates. No log I/O or RPC here. */
  def onMetadataUpdate(image: MetadataImage): Unit = synchronized {
    if (closed) return
    val desired = mutable.Set.empty[PartitionKey]
    image.topics().topicsById().values().asScala.foreach { topic =>
      val configs = image.configs().configProperties(new ConfigResource(ConfigResource.Type.TOPIC, topic.name()))
      if (!Topic.isInternal(topic.name()) && java.lang.Boolean.parseBoolean(configs.getProperty(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG))) {
        topic.partitions().asScala.foreach { case (partitionId, registration) =>
          if (registration.leader == brokerId) {
            val key = new PartitionKey(topic.id(), partitionId)
            replicaManager.onlinePartition(new TopicPartition(topic.name(), partitionId)).foreach { source =>
              if (source.topicId.contains(topic.id()) && source.isLeader && source.getLeaderEpoch == registration.leaderEpoch) {
                desired.add(key)
                indexers.get(key) match {
                  // Retain even a failed/fenced instance in the same lifetime. Unrelated metadata must
                  // not let an old owner steal a new generation after a CAS or fencing rejection.
                  case Some(existing) if (existing.source eq source) && existing.sourceLeaderEpoch == registration.leaderEpoch =>
                  case previous =>
                    previous.foreach(_.close())
                    val indexer = new GlobalSequencePartitionIndexer(key, source, brokerId, registration.leaderEpoch,
                      reader, router, workers, scheduler, requestTimeoutMs, readMaxBytes)
                    indexers.put(key, indexer)
                    indexer.start()
                }
              }
            }
          }
        }
      }
    }
    indexers.keysIterator.filterNot(desired.contains).toList.foreach { key => indexers.remove(key).foreach(_.close()) }
  }

  private[server] def indexer(partition: PartitionKey): Option[GlobalSequencePartitionIndexer] = synchronized {
    indexers.get(partition)
  }

  override def close(): Unit = {
    synchronized {
      closed = true
      indexers.values.foreach(_.close())
      indexers.clear()
      workers.shutdown()
    }
    ThreadUtils.shutdownExecutorServiceQuietly(workers, 5, TimeUnit.SECONDS)
  }
}
