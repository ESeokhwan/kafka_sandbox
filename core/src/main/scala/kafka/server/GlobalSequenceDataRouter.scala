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

import org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources.{Event, Scope}

import org.apache.kafka.common.{IsolationLevel, Node}
import org.apache.kafka.common.IsolationLevel.READ_UNCOMMITTED
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{CoordinatorNotAvailableException, FencedLeaderEpochException, InvalidRequestException, NotLeaderOrFollowerException, TimeoutException, UnknownTopicIdException, UnsupportedVersionException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{PartitionKey, PhysicalBatch}
import org.apache.kafka.image.MetadataImage
import org.apache.kafka.server.util.Scheduler

import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ScheduledFuture}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

private[server] object GlobalSequenceDataRouter {
  case class Location(node: Node, leaderEpoch: Int, brokerEpoch: Long)
}

/** Borrows the index router's inter-broker transport; owns only physical reads and their deadlines. */
class GlobalSequenceDataRouter private[server](brokerId: Int, listener: ListenerName, metadata: () => MetadataImage,
                                               reader: GlobalSequenceDataReader, transport: GlobalSequenceTransport,
                                               scheduler: Scheduler, time: Time, resources: GlobalSequenceResources = null,
                                               ownsTransport: Boolean = false) extends AutoCloseable {
  import GlobalSequenceDataRouter._
  import GlobalSequenceFetch.{Data, RangeResult, ReadRange}
  private val pending = ConcurrentHashMap.newKeySet[CompletableFuture[_]]()
  private val legacyBrokerEpochs = new ConcurrentHashMap[Int, java.lang.Long]()
  @volatile private var closed = false

  def readLocal(batch: PhysicalBatch, epoch: Int, deadlineNs: Long, isolation: IsolationLevel = READ_UNCOMMITTED): CompletableFuture[Data] =
    reader.read(batch, epoch, deadlineNs, isolation)

  private def resolve(key: PartitionKey): Location = {
    val image = metadata()
    val topic = image.topics().getTopic(key.topicId())
    if (topic == null) throw new UnknownTopicIdException("Mapped source topic UUID is unknown")
    val config = image.configs().configProperties(new ConfigResource(ConfigResource.Type.TOPIC, topic.name()))
    if (Topic.isInternal(topic.name()) || !java.lang.Boolean.parseBoolean(config.getProperty(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG)))
      throw new InvalidRequestException("Global sequence is not enabled for the source topic")
    val partition = topic.partitions().get(key.partition())
    if (partition == null || partition.leader < 0) throw new NotLeaderOrFollowerException("Mapped source partition has no leader")
    val broker = image.cluster().broker(partition.leader)
    if (broker == null || broker.fenced()) throw new NotLeaderOrFollowerException("Mapped source broker is unavailable")
    val node = broker.node(listener.value())
    if (node.isEmpty) throw new NotLeaderOrFollowerException("Mapped source broker has no inter-broker endpoint")
    Location(node.get(), partition.leaderEpoch, broker.epoch())
  }

  def readLocalRange(range: ReadRange, epoch: Int, deadlineNs: Long, isolation: IsolationLevel): CompletableFuture[RangeResult] =
    reader.readRange(range, epoch, deadlineNs, isolation)

  def read(batch: PhysicalBatch, deadlineNs: Long, isolation: IsolationLevel = READ_UNCOMMITTED): CompletableFuture[Data] =
    route(batch.partition(), deadlineNs) { (location, timeout, track) =>
      if (location.node.id() == brokerId) {
        val local = readLocal(batch, location.leaderEpoch, deadlineNs, isolation)
        track(local)
        local.thenApply[Data] { value =>
          if (value.sourceLeaderEpoch != location.leaderEpoch)
            throw new NotLeaderOrFollowerException("Source epoch changed while reading the mapped batch")
          GlobalSequenceFetch.validateData(batch, value, isolation)
          value
        }
      } else {
        val remote = transport.send(location.node, GlobalSequenceFetch.dataRequest(batch, location.leaderEpoch, timeout, isolation))
        track(remote)
        remote.thenApply[Data](response => GlobalSequenceFetch.decode(batch, location.leaderEpoch, response, isolation))
      }
    }

  def readRange(range: ReadRange, deadlineNs: Long, isolation: IsolationLevel): CompletableFuture[RangeResult] =
    route[RangeResult](range.partition, deadlineNs) { (location, timeout, track) =>
      if (location.node.id() == brokerId) {
        val local = readLocalRange(range, location.leaderEpoch, deadlineNs, isolation)
        track(local)
        local.thenApply[RangeResult](retryablePrefix)
      } else {
        def readLegacy(): CompletableFuture[RangeResult] = {
          // Older peers do not enforce a per-range byte limit. Read only at the next global position.
          if (!range.minOneBatch) CompletableFuture.completedFuture(RangeResult(Vector.empty))
          else {
            val first = range.batches.head
            val remaining = math.max(1L, (deadlineNs - time.nanoseconds() + 999999L) / 1000000L).toInt
            val legacy = transport.send(location.node, GlobalSequenceFetch.dataRequest(first, location.leaderEpoch, remaining, isolation))
            track(legacy)
            legacy.thenApply[RangeResult](reply => RangeResult(Vector(
              GlobalSequenceFetch.decode(first, location.leaderEpoch, reply, isolation))))
          }
        }

        if (Option(legacyBrokerEpochs.get(location.node.id())).exists(_.longValue() == location.brokerEpoch)) readLegacy()
        else {
          val remote = transport.send(location.node, GlobalSequenceFetch.rangeRequest(range, location.leaderEpoch, timeout, isolation))
          track(remote)
          remote.handle[CompletableFuture[RangeResult]] { (response, error) =>
            if (error == null)
              CompletableFuture.completedFuture(retryablePrefix(GlobalSequenceFetch.decodeRange(range, location.leaderEpoch, response, isolation)))
            else if (Errors.maybeUnwrapException(error).isInstanceOf[UnsupportedVersionException]) {
              legacyBrokerEpochs.put(location.node.id(), Long.box(location.brokerEpoch))
              readLegacy()
            } else CompletableFuture.failedFuture(error)
          }.thenCompose[RangeResult]((next: CompletableFuture[RangeResult]) => next)
        }
      }
    }

  private def retryablePrefix(value: RangeResult): RangeResult = value.error match {
    case Some(error) if isRetryable(error) =>
      if (value.data.isEmpty) throw error
      value.copy(error = None)
    case _ => value
  }

  private def isRetryable(cause: Throwable): Boolean = {
    val code = Errors.forException(cause)
    IndexRoutingManager.isRetryable(cause) || cause.isInstanceOf[FencedLeaderEpochException] ||
      code == Errors.UNKNOWN_LEADER_EPOCH || code == Errors.OFFSET_NOT_AVAILABLE
  }

  private def route[A](key: PartitionKey, deadlineNs: Long)
                      (readAt: (Location, Int, CompletableFuture[_] => Unit) => CompletableFuture[A]): CompletableFuture[A] =
    GlobalSequenceAdmission.run(resources, Scope.DATA_ROUTE, key) { readAdmitted(key, deadlineNs)(readAt) }

  private def readAdmitted[A](key: PartitionKey, deadlineNs: Long)
                             (readAt: (Location, Int, CompletableFuture[_] => Unit) => CompletableFuture[A]): CompletableFuture[A] = {
    val result = new CompletableFuture[A]()
    @volatile var timer: ScheduledFuture[_] = null
    @volatile var retryTimer: ScheduledFuture[_] = null
    @volatile var active: CompletableFuture[_] = null
    def remainingMs: Long = math.max(0L, (deadlineNs - time.nanoseconds() + 999999L) / 1000000L)
    def timeout(): Unit = result.completeExceptionally(new TimeoutException("Global physical fetch deadline expired"))
    pending.add(result)
    result.whenComplete { (_, _) =>
      pending.remove(result)
      if (timer != null) timer.cancel(false)
      if (retryTimer != null) retryTimer.cancel(false)
      if (active != null) active.cancel(false)
    }
    def retry(error: Throwable, attempt: Int): Unit = {
      val cause = Errors.maybeUnwrapException(error)
      if (isRetryable(cause)) {
        Option(resources).foreach(_.event(Event.RPC_RETRY, 0))
        if (remainingMs == 0) timeout()
        else {
          retryTimer = scheduler.scheduleOnce("global-sequence-data-retry", () => send(attempt + 1),
            math.min(remainingMs, math.min(1000L, 100L << math.min(attempt, 4))))
          if (result.isDone) retryTimer.cancel(false)
        }
      } else result.completeExceptionally(cause)
    }
    def send(attempt: Int): Unit = {
      if (result.isDone) return
      if (closed) {
        result.completeExceptionally(new CoordinatorNotAvailableException("Global data router is closed"))
        return
      }
      if (remainingMs == 0) { timeout(); return }
      try {
        val location = resolve(key)
        val operation = readAt(location, remainingMs.toInt, future => {
          active = future
          if (result.isDone) future.cancel(false)
        })
        operation.whenComplete { (value, error) =>
          if (!result.isDone) {
            if (remainingMs == 0) timeout()
            else if (error != null) retry(error, attempt)
            else try {
              if (resolve(key) != location)
                throw new NotLeaderOrFollowerException("Source route changed while reading the mapped batch")
              result.complete(value)
            } catch { case NonFatal(failure) => retry(failure, attempt) }
          }
        }
      } catch { case NonFatal(error) => retry(error, attempt) }
    }
    try {
      timer = scheduler.scheduleOnce("global-sequence-data-routing-timeout", () => timeout(), remainingMs)
      if (result.isDone) timer.cancel(false)
      send(0)
    } catch { case NonFatal(error) => result.completeExceptionally(error) }
    result
  }

  override def close(): Unit = {
    closed = true
    pending.asScala.foreach(_.completeExceptionally(new CoordinatorNotAvailableException("Global data router is closed")))
    reader.close()
    if (ownsTransport) transport.close()
  }
}
