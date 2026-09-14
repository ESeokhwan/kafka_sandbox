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

import kafka.cluster.{Partition, PartitionListener}
import kafka.utils.Logging
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.errors.{FencedLeaderEpochException, InvalidRequestException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard._
import org.apache.kafka.server.util.Scheduler

import java.util.concurrent.{CompletableFuture, ConcurrentLinkedQueue, Executor, RejectedExecutionException, ScheduledFuture}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.jdk.OptionConverters._
import scala.util.control.NonFatal

/**
 * One source leader's indexing lifetime. All state transitions and source I/O run serially on the
 * shared worker pool. Partition listeners and RPC callbacks only enqueue notifications.
 * A stopped instance is never reused, including after a CAS rejection in the same source epoch.
 */
private[server] class GlobalSequencePartitionIndexer(
  val partition: PartitionKey,
  val source: Partition,
  brokerId: Int,
  val sourceLeaderEpoch: Int,
  reader: GlobalSequenceSourceReader,
  router: IndexRoutingManager,
  executor: Executor,
  scheduler: Scheduler,
  requestTimeoutMs: Int,
  readMaxBytes: Int
) extends AutoCloseable with Logging {
  import GlobalSequenceSourceReader._
  import IndexRoutingManager._

  logIdent = s"[GlobalSequenceIndexer broker=$brokerId partition=$partition epoch=$sourceLeaderEpoch] "
  private val stopped = new AtomicBoolean(false)
  private val scheduled = new AtomicBoolean(false)
  private val wakeupQueued = new AtomicBoolean(false)
  private val observedHighWatermark = new AtomicLong(-1)
  private val events = new ConcurrentLinkedQueue[() => Unit]()
  @volatile private var operation: CompletableFuture[_] = _
  @volatile private var retryTask: ScheduledFuture[_] = _
  @volatile private[server] var failure: Option[Throwable] = None

  // Accessed only by the serial worker, never by partition listeners or future completion threads.
  private var busy = true
  private var registration: RegistrationRequest = _
  private var owner: IndexerIdentity = _
  private var barrierRoute: CoordinatorLocation = _
  private var progress: Option[PhysicalBatch] = None
  private var cursor = 0L
  private var pending: Option[AppendRequest] = None
  private var recoveryError: Option[Throwable] = None
  private var recoveryCheckedAt: Option[CoordinatorLocation] = None

  private val listener = new PartitionListener {
    override def onHighWatermarkUpdated(tp: TopicPartition, offset: Long): Unit = {
      observedHighWatermark.accumulateAndGet(offset, (left, right) => math.max(left, right))
      if (wakeupQueued.compareAndSet(false, true)) enqueue {
        wakeupQueued.set(false)
        if (!busy) pump()
      }
    }
    override def onBecomingFollower(tp: TopicPartition): Unit = close()
    override def onFailed(tp: TopicPartition): Unit = close()
    override def onDeleted(tp: TopicPartition): Unit = close()
  }

  def isStopped: Boolean = stopped.get()

  def start(): Unit = {
    // Register before the initial read so a concurrent HW update cannot be missed.
    if (source.maybeAddListener(listener)) {
      if (isStopped) source.removeListener(listener)
      else enqueue { discoverOwner() }
    } else close()
  }

  private def enqueue(event: => Unit): Unit = {
    if (!isStopped) {
      events.add(() => event)
      if (isStopped) events.clear()
      else scheduleNext()
    }
  }

  private def scheduleNext(): Unit = {
    if (!isStopped && scheduled.compareAndSet(false, true)) {
      try executor.execute(() => {
        try {
          val event = events.poll()
          if (!isStopped && event != null) event()
        } catch { case NonFatal(error) => fail(error) }
        finally {
          scheduled.set(false)
          // One event (at most one source read) per turn lets other partitions use the pool.
          if (!events.isEmpty) scheduleNext()
        }
      }) catch { case _: RejectedExecutionException => close() }
    }
  }

  private def fail(cause: Throwable): Unit = {
    failure = Some(cause)
    cause match {
      case _: FencedLeaderEpochException => info("Stopped after losing source/indexer ownership", cause)
      case _ => error("Indexing stopped; the source cursor has not been skipped", cause)
    }
    close()
  }

  /** Retry the identical logical operation after routing's deadline; an accepted write is not cancelled. */
  private def call[T](request: => CompletableFuture[RoutedResult[T]])(accept: RoutedResult[T] => Unit): Unit = {
    busy = true
    def retryOrFail(error: Throwable): Unit = {
      val cause = Errors.maybeUnwrapException(error)
      if (IndexRoutingManager.isRetryable(cause)) {
        retryTask = scheduler.scheduleOnce("global-sequence-indexer-retry", () => enqueue { call(request)(accept) }, 100)
        if (isStopped) retryTask.cancel(false)
      } else fail(cause)
    }
    if (!isStopped) try {
      val future = request
      operation = future
      if (isStopped) future.cancel(false)
      future.whenComplete { (result, error) => enqueue {
        operation = null
        if (error == null) accept(result) else retryOrFail(error)
      } }
    } catch { case NonFatal(error) => retryOrFail(error) }
  }

  private def discoverOwner(): Unit = call(router.describePartition(partition, requestTimeoutMs)) { result =>
    if (!router.isCurrent(partition, result.coordinator)) discoverOwner()
    else {
      val expectedGeneration = result.value.currentIndexer().toScala.map(_.generation()).getOrElse(-1L)
      registration = new RegistrationRequest(partition, brokerId, sourceLeaderEpoch, expectedGeneration, Uuid.randomUuid())
      owner = new IndexerIdentity(brokerId, sourceLeaderEpoch, expectedGeneration + 1, registration.registrationId())
      register()
    }
  }

  private def register(): Unit = call(router.registerIndexer(registration, requestTimeoutMs)) { result =>
    if (!result.value.registered() || !result.value.indexer().toScala.contains(owner))
      fail(new FencedLeaderEpochException("Global sequence registration was rejected"))
    else if (!router.isCurrent(partition, result.coordinator)) register()
    else {
      barrierRoute = result.coordinator
      loadProgress()
    }
  }

  private def loadProgress(): Unit = call(router.describePartition(partition, requestTimeoutMs)) { result =>
    if (result.coordinator != barrierRoute || !router.isCurrent(partition, result.coordinator)) register()
    else if (!result.value.currentIndexer().toScala.contains(owner))
      fail(new FencedLeaderEpochException("Global sequence ownership changed after registration"))
    else {
      val committed = result.value.committedProgress().toScala
      if (committed.map(_.lastOffset()).getOrElse(-1L) < progress.map(_.lastOffset()).getOrElse(-1L))
        throw new InvalidRequestException("Committed global sequence progress regressed after a registration barrier")
      progress = committed
      // Preserve already validated control-only progress within this source lifetime.
      // A new lifetime still starts at the durable cursor (or zero).
      cursor = math.max(cursor, committed.map(_.resumeOffset()).getOrElse(0L))
      pending.foreach { request =>
        if (committed.exists(_.lastOffset() >= request.batch().lastOffset())) pending = None
        else if (request.predecessorBaseOffset() != committed.map(_.baseOffset()).getOrElse(-1L))
          throw new InvalidRequestException("Pending global sequence append disagrees with committed progress")
      }
      recoveryCheckedAt = recoveryError.map(_ => result.coordinator)
      enqueue { pump() }
    }
  }

  private def recover(cause: Throwable): Unit = {
    if (recoveryCheckedAt.exists(router.isCurrent(partition, _))) fail(cause)
    else {
      recoveryError = Some(cause)
      recoveryCheckedAt = None
      register()
    }
  }

  private def pump(): Unit = {
    busy = true
    pending match {
      case Some(request) => append(request)
      case None =>
        val read = try reader.read(partition, sourceLeaderEpoch, cursor, readMaxBytes)
        catch {
          case gap: SourceLogGapException =>
            if (!isStopped) recover(gap)
            return
        }
        recoveryError = None
        recoveryCheckedAt = None
        if (!isStopped) read.status match {
          case BATCH =>
            val request = new AppendRequest(read.batch.get, progress.map(_.baseOffset()).getOrElse(-1L), read.dataHighWatermark, owner)
            pending = Some(request)
            append(request)
          case CONTINUE =>
            cursor = read.nextPhysicalOffset
            enqueue { pump() }
          case AWAIT_HIGH_WATERMARK =>
            cursor = read.nextPhysicalOffset
            busy = false
            // If the HW notification arrived during the read, its captured HW may be obsolete.
            // Comparing HW (not cursor) also avoids spinning when HW is in the middle of a batch.
            if (observedHighWatermark.get() > read.dataHighWatermark) {
              busy = true
              enqueue { pump() }
            }
        }
    }
  }

  private def append(request: AppendRequest): Unit = call(router.appendIndex(request, requestTimeoutMs)) { result =>
    result.value.status() match {
      case AppendStatus.FENCED => fail(new FencedLeaderEpochException("Global sequence append was fenced"))
      case AppendStatus.OWNER_NOT_COMMITTED => register()
      case AppendStatus.OUT_OF_ORDER =>
        recover(new InvalidRequestException("Global sequence coordinator rejected the source predecessor after progress recovery"))
      case AppendStatus.INDEXED | AppendStatus.ALREADY_INDEXED =>
        if (!router.isCurrent(partition, result.coordinator)) register()
        else {
          progress = result.value.indexedThrough().toScala
          cursor = progress.get.resumeOffset()
          pending = None
          recoveryError = None
          recoveryCheckedAt = None
          enqueue { pump() }
        }
    }
  }

  override def close(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      source.removeListener(listener)
      // Cancel only our routing wait. This does not roll back a write accepted by the coordinator.
      val outstanding = operation
      if (outstanding != null) outstanding.cancel(false)
      if (retryTask != null) retryTask.cancel(false)
      events.clear()
    }
  }
}
