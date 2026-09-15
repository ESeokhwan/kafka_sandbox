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

import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{CoordinatorNotAvailableException, DisconnectException, FencedLeaderEpochException, InvalidRequestException, NotCoordinatorException, TimeoutException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.internals.Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.globalsequence.{GlobalSequenceCoordinator, GlobalSequenceLookup}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard._
import org.apache.kafka.image.MetadataImage
import org.apache.kafka.server.util.Scheduler

import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ScheduledFuture}
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object IndexRoutingManager {
  /** Retain this token with a read result and recheck it before applying deferred recovery work. */
  case class CoordinatorLocation(indexTopicId: Uuid, partition: Int, node: Node, leaderEpoch: Int, brokerEpoch: Long)
  case class RoutedResult[T](value: T, coordinator: CoordinatorLocation)

  private val retryable = Set(Errors.NOT_COORDINATOR, Errors.COORDINATOR_NOT_AVAILABLE,
    Errors.COORDINATOR_LOAD_IN_PROGRESS, Errors.NOT_LEADER_OR_FOLLOWER, Errors.LEADER_NOT_AVAILABLE,
    Errors.REQUEST_TIMED_OUT, Errors.NETWORK_EXCEPTION, Errors.UNKNOWN_TOPIC_OR_PARTITION,
    Errors.KAFKA_STORAGE_ERROR, Errors.NOT_ENOUGH_REPLICAS, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)

  private[server] def isRetryable(cause: Throwable): Boolean =
    cause.isInstanceOf[DisconnectException] || retryable.contains(Errors.forException(cause))
}

/**
 * Routes immutable logical requests using the current KRaft image. RPC callbacks, metadata lookups and
 * local coordinator scheduling are non-blocking. The supplied scheduler is shared and owned by the broker.
 */
class IndexRoutingManager private[server](
  brokerId: Int,
  indexPartitionCount: Int,
  listenerName: ListenerName,
  metadata: () => MetadataImage,
  coordinator: GlobalSequenceCoordinator,
  transport: GlobalSequenceTransport,
  scheduler: Scheduler,
  time: Time,
  resources: GlobalSequenceResources = null
) extends AutoCloseable {
  import IndexRoutingManager._

  require(indexPartitionCount > 0)
  private val observedIndexId = new AtomicReference[Uuid]()
  private val indexFailure = new AtomicReference[InvalidRequestException]()
  private val pending = ConcurrentHashMap.newKeySet[CompletableFuture[_]]()
  @volatile private var started = false
  @volatile private var closed = false

  def startup(): Unit = synchronized {
    if (closed) throw new IllegalStateException("Global sequence router is closed")
    if (!started) {
      transport.startup()
      started = true
    }
  }

  private[server] def reserveLookupResponse(topicId: Uuid): Option[GlobalSequenceResources#Lease] =
    Option(resources).map(_.acquire(Scope.INDEX_RESPONSE, topicId, 128L * 1024))

  def lookupIndex(request: GlobalSequenceLookup.Request): CompletableFuture[RoutedResult[GlobalSequenceLookup.Result]] = {
    val key = new PartitionKey(request.topicId(), 0)
    val deadline = time.hiResClockMs() + request.timeoutMs()
    def remainingRequest = new GlobalSequenceLookup.Request(request.topicId(), request.startOffset(), request.endOffset(),
      request.maxBatches(), math.max(1L, deadline - time.hiResClockMs()).toInt)
    submit(key, None, request.timeoutMs(), Scope.LOOKUP_ROUTE)(
      route => coordinator.lookupIndex(remainingRequest, route.leaderEpoch),
      route => GlobalSequenceProtocol.lookupRequest(remainingRequest, route.leaderEpoch),
      response => GlobalSequenceProtocol.lookupResponse(request, response),
      (result, route) => {
        checkEpoch(result.snapshot().leaderEpoch(), route)
        if (result.snapshot().indexTopicId() != route.indexTopicId || result.snapshot().indexPartition() != route.partition)
          throw new NotCoordinatorException("Lookup response identifies a different index snapshot")
      },
      _ => false)
  }

  def describePartition(partition: PartitionKey, timeoutMs: Long): CompletableFuture[RoutedResult[PartitionDescription]] =
    submit(partition, None, timeoutMs)(
      _ => coordinator.describePartition(partition),
      _ => GlobalSequenceProtocol.describeRequest(partition),
      response => GlobalSequenceProtocol.describeResponse(partition, response),
      (_, _) => (),
      _ => false)

  def registerIndexer(request: RegistrationRequest, timeoutMs: Long): CompletableFuture[RoutedResult[RegistrationResponse]] =
    submit(request.partition(), Some((request.sourceBrokerId(), request.sourceLeaderEpoch())), timeoutMs)(
      route => coordinator.registerIndexer(request, route.leaderEpoch),
      route => GlobalSequenceProtocol.registerRequest(request, route.leaderEpoch),
      response => GlobalSequenceProtocol.registerResponse(request, response),
      (result, route) => checkEpoch(result.coordinatorLeaderEpoch(), route),
      result => !result.registered())

  def appendIndex(request: AppendRequest, timeoutMs: Long): CompletableFuture[RoutedResult[AppendResponse]] =
    submit(request.batch().partition(), Some((request.indexer().brokerId(), request.indexer().leaderEpoch())), timeoutMs)(
      route => coordinator.appendIndex(request, route.leaderEpoch),
      route => GlobalSequenceProtocol.appendRequest(request, route.leaderEpoch),
      response => GlobalSequenceProtocol.appendResponse(request, response),
      (result, route) => checkEpoch(result.coordinatorLeaderEpoch(), route),
      result => result.status() != AppendStatus.INDEXED && result.status() != AppendStatus.ALREADY_INDEXED)

  /** A callback may queue recovery work; verify its route again when that work is actually applied. */
  def isCurrent(partition: PartitionKey, location: CoordinatorLocation): Boolean = {
    if (closed || !started) false
    else try resolve(partition, None) == location catch { case NonFatal(_) => false }
  }

  private def checkEpoch(epoch: Int, route: CoordinatorLocation): Unit = {
    if (epoch != route.leaderEpoch) throw new NotCoordinatorException("Global sequence response has an obsolete coordinator epoch")
  }

  private def resolve(partition: PartitionKey, source: Option[(Int, Int)]): CoordinatorLocation = {
    val failure = indexFailure.get()
    if (failure != null) throw failure
    val image = metadata()
    val index = image.topics().getTopic(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)
    if (index == null) {
      if (observedIndexId.get() != null) failIndex("The global sequence index was deleted; restore its history")
      throw new CoordinatorNotAvailableException("Global sequence index metadata is not available")
    }
    observedIndexId.compareAndSet(null, index.id())
    if (observedIndexId.get() != index.id() || index.partitions().size() != indexPartitionCount)
      failIndex("Global sequence index identity or fixed partition count changed")
    val topic = image.topics().getTopic(partition.topicId())
    if (topic == null || !topic.partitions().containsKey(partition.partition()))
      throw new UnknownTopicOrPartitionException("Unknown global sequence data partition")
    val config = image.configs().configProperties(new ConfigResource(ConfigResource.Type.TOPIC, topic.name()))
    if (Topic.isInternal(topic.name()) || !java.lang.Boolean.parseBoolean(config.getProperty(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG)))
      throw new InvalidRequestException("Global sequence is not enabled for this data topic")
    source.foreach { case (sourceBroker, sourceEpoch) =>
      val data = topic.partitions().get(partition.partition())
      if (data.leader != sourceBroker || data.leaderEpoch != sourceEpoch)
        throw new FencedLeaderEpochException("The indexer is no longer the source leader")
    }
    val indexPartition = coordinator.partitionFor(partition.topicId())
    val leader = index.partitions().get(indexPartition)
    if (leader == null || leader.leader < 0) throw new CoordinatorNotAvailableException("Global sequence index has no leader")
    val broker = image.cluster().broker(leader.leader)
    if (broker == null || broker.fenced()) throw new CoordinatorNotAvailableException("Global sequence coordinator broker is unavailable")
    val node = broker.node(listenerName.value())
    if (node.isEmpty) throw new CoordinatorNotAvailableException("Global sequence coordinator has no inter-broker endpoint")
    CoordinatorLocation(index.id(), indexPartition, node.get(), leader.leaderEpoch, broker.epoch())
  }

  private def failIndex(message: String): Nothing = {
    indexFailure.compareAndSet(null, new InvalidRequestException(message))
    throw indexFailure.get()
  }

  private def submit[T](partition: PartitionKey, source: Option[(Int, Int)], timeoutMs: Long, scope: Scope = Scope.INDEX_ROUTE)(
    local: CoordinatorLocation => CompletableFuture[T],
    request: CoordinatorLocation => AbstractRequest.Builder[_ <: AbstractRequest],
    decode: AbstractResponse => T,
    validate: (T, CoordinatorLocation) => Unit,
    terminal: T => Boolean
  ): CompletableFuture[RoutedResult[T]] = GlobalSequenceAdmission.run(resources, scope, partition) {
    submitAdmitted(partition, source, timeoutMs)(local, request, decode, validate, terminal)
  }

  private def submitAdmitted[T](partition: PartitionKey, source: Option[(Int, Int)], timeoutMs: Long)(
    local: CoordinatorLocation => CompletableFuture[T],
    request: CoordinatorLocation => AbstractRequest.Builder[_ <: AbstractRequest],
    decode: AbstractResponse => T,
    validate: (T, CoordinatorLocation) => Unit,
    terminal: T => Boolean
  ): CompletableFuture[RoutedResult[T]] = {
    val result = new CompletableFuture[RoutedResult[T]]()
    if (timeoutMs <= 0 || timeoutMs > Int.MaxValue) {
      result.completeExceptionally(new IllegalArgumentException("timeoutMs must be between 1 and Integer.MAX_VALUE"))
      return result
    }
    pending.add(result)
    val deadlineMs = time.hiResClockMs() + timeoutMs
    @volatile var deadlineTask: ScheduledFuture[_] = null
    @volatile var retryTask: ScheduledFuture[_] = null
    @volatile var remote: CompletableFuture[AbstractResponse] = null
    result.whenComplete { (_, _) =>
      pending.remove(result)
      if (deadlineTask != null) deadlineTask.cancel(false)
      if (retryTask != null) retryTask.cancel(false)
      // Cancellation releases queued network work, never rolls back a coordinator write.
      if (remote != null) remote.cancel(false)
    }
    def timeout(): Unit = result.completeExceptionally(new TimeoutException("Global sequence routing deadline expired"))
    def schedule(delayMs: Long)(work: => Unit): ScheduledFuture[_] =
      scheduler.scheduleOnce("global-sequence-routing", () => if (!result.isDone) work, delayMs)
    def retry(error: Throwable, attempt: Int): Unit = {
      val cause = Errors.maybeUnwrapException(error)
      if (isRetryable(cause)) {
        Option(resources).foreach(_.event(Event.RPC_RETRY, 0))
        val remaining = deadlineMs - time.hiResClockMs()
        if (remaining <= 0) timeout()
        else {
          retryTask = schedule(math.min(remaining, math.min(1000L, 100L << math.min(attempt, 4)))) { send(attempt + 1) }
          if (result.isDone) retryTask.cancel(false)
        }
      } else result.completeExceptionally(cause)
    }
    def send(attempt: Int): Unit = {
      if (result.isDone) return
      if (closed || !started) {
        result.completeExceptionally(new CoordinatorNotAvailableException("Global sequence router is not running"))
        return
      }
      if (time.hiResClockMs() >= deadlineMs) { timeout(); return }
      try {
        val route = resolve(partition, source)
        val operation = if (route.node.id() == brokerId) local(route) else {
          remote = transport.send(route.node, request(route))
          if (result.isDone) remote.cancel(false)
          remote.thenApply[T](response => decode(response))
        }
        operation.whenComplete { (value, exception) =>
          if (!result.isDone) {
            if (time.hiResClockMs() >= deadlineMs) timeout()
            else if (exception != null) retry(exception, attempt)
            else try {
              validate(value, route)
              // A fencing/CAS rejection stops this logical operation even if its leader has since changed.
              // The caller must not reclaim ownership by transparently retrying a rejected registration.
              if (!terminal(value) && resolve(partition, source) != route)
                throw new NotCoordinatorException("Global sequence leader changed while the request was running")
              result.complete(RoutedResult(value, route))
            } catch { case NonFatal(error) => retry(error, attempt) }
          }
        }
      } catch { case NonFatal(error) => retry(error, attempt) }
    }
    if (closed || !started) result.completeExceptionally(new CoordinatorNotAvailableException("Global sequence router is not running"))
    else {
      deadlineTask = schedule(timeoutMs) { timeout() }
      if (result.isDone) deadlineTask.cancel(false)
      send(0)
    }
    result
  }

  override def close(): Unit = {
    synchronized {
      if (closed) return
      closed = true
    }
    pending.asScala.foreach(_.completeExceptionally(new CoordinatorNotAvailableException("Global sequence router is closed")))
    transport.close()
  }
}
