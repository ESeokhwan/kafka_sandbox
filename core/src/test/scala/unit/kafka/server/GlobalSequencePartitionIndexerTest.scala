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
import org.apache.kafka.common.{Node, TopicPartition, Uuid}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, FencedLeaderEpochException, InvalidRequestException, TimeoutException}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard._
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.UnifiedLog
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito.{mock, verify, when}

import java.util.{Optional, OptionalLong}
import java.util.concurrent.{AbstractExecutorService, CompletableFuture, TimeUnit}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

private[server] class GlobalSequenceTestExecutor extends AbstractExecutorService {
  val tasks: mutable.Queue[Runnable] = mutable.Queue.empty
  private var closed = false
  var reject = false
  override def execute(command: Runnable): Unit = {
    if (reject || closed) throw new java.util.concurrent.RejectedExecutionException()
    tasks.enqueue(command)
  }
  def runAll(): Unit = {
    var count = 0
    while (tasks.nonEmpty) {
      count += 1
      assertTrue(count < 1000, "Indexer spun without waiting for HW or an RPC")
      tasks.dequeue().run()
    }
  }
  override def shutdown(): Unit = { closed = true; tasks.clear() }
  override def shutdownNow(): java.util.List[Runnable] = { val result = tasks.toList.asJava; shutdown(); result }
  override def isShutdown: Boolean = closed
  override def isTerminated: Boolean = closed
  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = closed
}

class GlobalSequencePartitionIndexerTest {
  import GlobalSequenceSourceReader._
  import IndexRoutingManager._

  private val key = new PartitionKey(new Uuid(1, 2), 0)
  private val tp = new TopicPartition("ordered", 0)
  private val route = CoordinatorLocation(new Uuid(3, 4), 1, new Node(2, "localhost", 9002), 10, 7)
  private val a = new PhysicalBatch(key, 0, 2, 3)
  private val b = new PhysicalBatch(key, 3, 4, 2)

  private class Context(val executor: GlobalSequenceTestExecutor = new GlobalSequenceTestExecutor,
                        sourceKey: PartitionKey = key) extends AutoCloseable {
    val time = new MockTime(0, 0)
    val source: Partition = mock(classOf[Partition])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    var hw = 20L
    when(source.isLeader).thenReturn(true)
    when(source.getLeaderEpoch).thenReturn(3)
    when(source.topicId).thenReturn(Some(sourceKey.topicId()))
    when(source.log).thenReturn(Some(log))
    when(log.highWatermark).thenAnswer(_ => hw)
    val reader: GlobalSequenceSourceReader = mock(classOf[GlobalSequenceSourceReader])
    val router: IndexRoutingManager = mock(classOf[IndexRoutingManager])
    var listener: PartitionListener = _
    var currentRoute = route
    val descriptions = mutable.ArrayBuffer.empty[CompletableFuture[RoutedResult[PartitionDescription]]]
    val registrations = mutable.ArrayBuffer.empty[(RegistrationRequest, CompletableFuture[RoutedResult[RegistrationResponse]])]
    val appends = mutable.ArrayBuffer.empty[(AppendRequest, CompletableFuture[RoutedResult[AppendResponse]])]
    val reads = mutable.ArrayBuffer.empty[Long]
    var read: Long => ReadResult = offset => ReadResult(sourceKey, 3, offset, offset, None, AWAIT_HIGH_WATERMARK)
    when(source.maybeAddListener(any[PartitionListener])).thenAnswer { invocation =>
      listener = invocation.getArgument[PartitionListener](0)
      true
    }
    when(router.isCurrent(any[PartitionKey], any[CoordinatorLocation])).thenAnswer { invocation =>
      invocation.getArgument[CoordinatorLocation](1) == currentRoute
    }
    when(router.describePartition(any[PartitionKey], anyLong())).thenAnswer { _ =>
      val result = new CompletableFuture[RoutedResult[PartitionDescription]]()
      descriptions += result
      result
    }
    when(router.registerIndexer(any[RegistrationRequest], anyLong())).thenAnswer { invocation =>
      val result = new CompletableFuture[RoutedResult[RegistrationResponse]]()
      registrations += ((invocation.getArgument[RegistrationRequest](0), result))
      result
    }
    when(router.appendIndex(any[AppendRequest], anyLong())).thenAnswer { invocation =>
      val result = new CompletableFuture[RoutedResult[AppendResponse]]()
      appends += ((invocation.getArgument[AppendRequest](0), result))
      result
    }
    when(reader.read(any[PartitionKey], anyInt(), anyLong(), anyInt())).thenAnswer { invocation =>
      val offset = invocation.getArgument[Long](2)
      reads += offset
      read(offset)
    }
    val indexer = new GlobalSequencePartitionIndexer(sourceKey, source, 1, 3, reader, router, executor, time.scheduler, 1000, 1024, time)
    def owner: IndexerIdentity = {
      val request = registrations.head._1
      new IndexerIdentity(1, 3, request.expectedGeneration() + 1, request.registrationId())
    }
    def describe(progress: Option[PhysicalBatch], current: Option[IndexerIdentity]): Unit = {
      descriptions.last.complete(RoutedResult(new PartitionDescription(progress.toJava, current.toJava), currentRoute))
      executor.runAll()
    }
    def registered(): Unit = {
      registrations.last._2.complete(RoutedResult(new RegistrationResponse(true, Optional.of(owner), currentRoute.leaderEpoch), currentRoute))
      executor.runAll()
    }
    def start(progress: Option[PhysicalBatch] = None): Unit = {
      indexer.start()
      assertTrue(descriptions.isEmpty, "start must not call the router on the caller thread")
      executor.runAll()
      describe(None, None)
      assertTrue(reads.isEmpty, "Do not read before the registration fence commits")
      registered()
      assertTrue(reads.isEmpty, "Do not read before post-barrier progress arrives")
      describe(progress, Some(owner))
    }
    def batch(value: PhysicalBatch): ReadResult = ReadResult(sourceKey, 3, 20, value.resumeOffset(), Some(value), BATCH)
    def completeAppend(status: AppendStatus = AppendStatus.INDEXED): Unit = {
      val request = appends.last._1
      val global = if (status == AppendStatus.INDEXED) OptionalLong.of(0) else OptionalLong.empty()
      appends.last._2.complete(RoutedResult(new AppendResponse(status, request.batch(), global, Optional.of(request.batch()),
        currentRoute.leaderEpoch), currentRoute))
      executor.runAll()
    }
    override def close(): Unit = { indexer.close(); executor.shutdown(); time.scheduler.clear() }
  }

  @Test
  def testSourceReadDeadlineRetriesTheSameCursor(): Unit = {
    val c = new Context
    try {
      c.read = _ => throw new TimeoutException("slow source read")
      c.start()
      assertFalse(c.indexer.isStopped)
      c.read = _ => c.batch(a)
      c.time.sleep(100)
      c.executor.runAll()
      assertEquals(Seq(0L, 0L), c.reads.toSeq)
      assertEquals(a, c.appends.head._1.batch())
    } finally c.close()
  }

  @Test
  def testWorkerAndRpcOverloadRetainTheIdenticalAppendAndResume(): Unit = {
    val c = new Context
    try {
      c.read = offset => if (offset == 0) c.batch(a) else ReadResult(key, 3, 20, offset, None, AWAIT_HIGH_WATERMARK)
      c.start()
      val original = c.appends.head._1
      c.executor.reject = true
      c.appends.head._2.completeExceptionally(new org.apache.kafka.common.errors.ThrottlingQuotaExceededException(100, "full"))
      for (_ <- 0 until 1000) c.listener.onHighWatermarkUpdated(tp, 20)
      assertFalse(c.indexer.isStopped)
      assertEquals(0, c.executor.tasks.size)
      c.time.sleep(100)
      assertFalse(c.indexer.isStopped)
      c.executor.reject = false
      c.time.sleep(100)
      c.executor.runAll()
      c.time.sleep(100)
      c.executor.runAll()
      assertEquals(2, c.appends.size)
      assertSame(original, c.appends.last._1)
      c.completeAppend()
      assertEquals(Seq(0L, 3L), c.reads.toSeq)
      assertTrue(c.indexer.awaitIndexed(2, TimeUnit.SECONDS.toNanos(1)).isDone)
    } finally c.close()
  }

  @Test
  def testProduceWaiterCompletesAfterIndexCommitIncludingAlreadyIndexed(): Unit = {
    for (status <- Seq(AppendStatus.INDEXED, AppendStatus.ALREADY_INDEXED)) {
      val c = new Context
      try {
        c.read = offset => if (offset == 0) c.batch(a)
          else ReadResult(key, 3, 20, offset, None, AWAIT_HIGH_WATERMARK)
        c.start()
        val wait = c.indexer.awaitIndexed(a.lastOffset(), TimeUnit.SECONDS.toNanos(1))
        assertFalse(wait.isDone)
        c.completeAppend(status)
        assertTrue(wait.isDone)
        assertFalse(wait.isCompletedExceptionally)
        assertTrue(c.indexer.awaitIndexed(a.lastOffset(), TimeUnit.SECONDS.toNanos(1)).isDone)
      } finally c.close()
    }
  }

  @Test
  def testRecoveredProduceWaiterWaitsForSourceHighWatermarkNotification(): Unit = {
    val c = new Context
    try {
      c.hw = 0
      c.start(Some(a))
      val wait = c.indexer.awaitIndexed(a.lastOffset(), TimeUnit.SECONDS.toNanos(1))
      assertFalse(wait.isDone)
      c.hw = a.resumeOffset()
      c.listener.onHighWatermarkUpdated(tp, c.hw)
      assertFalse(wait.isDone)
      c.executor.runAll()
      assertTrue(wait.isDone)
      assertFalse(wait.isCompletedExceptionally)
    } finally c.close()
  }

  @Test
  def testProduceTimeoutDoesNotCancelPendingIndexAppend(): Unit = {
    val c = new Context
    try {
      c.read = offset => if (offset == 0) c.batch(a)
        else ReadResult(key, 3, 20, offset, None, AWAIT_HIGH_WATERMARK)
      c.start()
      val wait = c.indexer.awaitIndexed(a.lastOffset(), TimeUnit.MILLISECONDS.toNanos(20))
      c.time.sleep(20)
      assertFutureThrows(classOf[TimeoutException], wait)
      assertFalse(c.appends.head._2.isDone)
      assertFalse(c.indexer.isStopped)
      c.completeAppend()
      assertTrue(c.indexer.awaitIndexed(a.lastOffset(), TimeUnit.SECONDS.toNanos(1)).isDone)
    } finally c.close()
  }

  @Test
  def testOnlyReadNextBatchAfterIndexCommitAndCoalesceWakeups(): Unit = {
    val c = new Context
    try {
      c.read = offset => if (offset == 0) c.batch(a) else if (offset == 3) c.batch(b)
        else ReadResult(key, 3, 20, offset, None, AWAIT_HIGH_WATERMARK)
      c.start()
      assertEquals(Seq(0L), c.reads.toSeq)
      assertEquals(-1L, c.appends.head._1.predecessorBaseOffset())
      for (_ <- 0 until 100) c.listener.onHighWatermarkUpdated(tp, 20)
      assertEquals(1, c.executor.tasks.size)
      c.executor.runAll()
      assertEquals(Seq(0L), c.reads.toSeq)
      assertEquals(1, c.appends.size)
      c.completeAppend()
      assertEquals(Seq(0L, 3L), c.reads.toSeq)
      assertEquals(a.baseOffset(), c.appends.last._1.predecessorBaseOffset())
      c.completeAppend(AppendStatus.ALREADY_INDEXED)
      assertEquals(Seq(0L, 3L, 5L), c.reads.toSeq)
      assertFalse(c.indexer.isStopped)
    } finally c.close()
  }

  @Test
  def testAppendTimeoutRetriesIdenticalBatchPredecessorAndIdentity(): Unit = {
    val c = new Context
    try {
      c.read = _ => c.batch(a)
      c.start()
      val original = c.appends.head._1
      c.appends.head._2.completeExceptionally(new TimeoutException("response lost"))
      c.executor.runAll()
      c.listener.onHighWatermarkUpdated(tp, 100)
      c.executor.runAll()
      assertEquals(1, c.appends.size)
      c.time.sleep(100)
      c.executor.runAll()
      assertEquals(2, c.appends.size)
      assertSame(original, c.appends.last._1)
      assertEquals(Seq(0L), c.reads.toSeq)
    } finally c.close()
  }

  @Test
  def testRegistrationTimeoutDoesNotAcquireAnotherGeneration(): Unit = {
    val c = new Context
    try {
      c.indexer.start()
      c.executor.runAll()
      val previous = new IndexerIdentity(1, 3, 7, Uuid.randomUuid())
      c.describe(None, Some(previous))
      val request = c.registrations.head._1
      assertEquals(7, request.expectedGeneration())
      c.registrations.head._2.completeExceptionally(new TimeoutException("fence may already be appended"))
      c.executor.runAll()
      c.time.sleep(100)
      c.executor.runAll()
      assertSame(request, c.registrations.last._1)
      assertEquals(1, c.descriptions.size)
      assertTrue(c.reads.isEmpty)
      c.registered()
      c.describe(None, Some(c.owner))
      assertEquals(8, c.owner.generation())
    } finally c.close()
  }

  @Test
  def testRegistrationCasRejectionStopsWithoutReclaimingOwnership(): Unit = {
    val c = new Context
    try {
      c.indexer.start()
      c.executor.runAll()
      c.describe(None, None)
      c.registrations.head._2.complete(RoutedResult(new RegistrationResponse(false, Optional.of(c.owner), 10), route))
      c.executor.runAll()
      c.listener.onHighWatermarkUpdated(tp, 100)
      c.time.sleep(10000)
      c.executor.runAll()
      assertTrue(c.indexer.isStopped)
      assertTrue(c.indexer.failure.get.isInstanceOf[FencedLeaderEpochException])
      assertEquals(1, c.registrations.size)
      assertTrue(c.reads.isEmpty)
      verify(c.source).removeListener(c.listener)
    } finally c.close()
  }

  @Test
  def testResumeCommittedProgressAndSkipControlOnlyReads(): Unit = {
    val c = new Context
    try {
      val previous = new PhysicalBatch(key, 4, 6, 3)
      val next = new PhysicalBatch(key, 9, 10, 2)
      c.read = offset => if (offset == 7) ReadResult(key, 3, 20, 9, None, CONTINUE) else c.batch(next)
      c.start(Some(previous))
      assertEquals(Seq(7L, 9L), c.reads.toSeq)
      assertEquals(1, c.appends.size)
      assertEquals(next, c.appends.head._1.batch())
      assertEquals(4, c.appends.head._1.predecessorBaseOffset())
    } finally c.close()
  }

  @Test
  def testHighWatermarkUpdateDuringReadIsNotLostAndDoesNotRunInline(): Unit = {
    val c = new Context
    try {
      c.read = offset => {
        if (c.reads.size == 1) {
          c.listener.onHighWatermarkUpdated(tp, 3)
          assertEquals(1, c.reads.size)
          ReadResult(key, 3, 0, offset, None, AWAIT_HIGH_WATERMARK)
        } else c.batch(a)
      }
      c.start()
      assertEquals(Seq(0L, 0L), c.reads.toSeq)
      assertEquals(1, c.appends.size)
    } finally c.close()
  }

  @Test
  def testMidBatchHighWatermarkWaitsForAnotherNotification(): Unit = {
    val c = new Context
    try {
      c.read = offset => ReadResult(key, 3, 2, offset, None, AWAIT_HIGH_WATERMARK)
      c.start()
      assertEquals(Seq(0L), c.reads.toSeq)
      c.read = _ => c.batch(a)
      c.listener.onHighWatermarkUpdated(tp, 3)
      assertEquals(1, c.reads.size)
      c.executor.runAll()
      assertEquals(Seq(0L, 0L), c.reads.toSeq)
    } finally c.close()
  }

  @Test
  def testObsoleteQueuedProgressRequiresSameRegistrationBarrier(): Unit = {
    val c = new Context
    try {
      c.indexer.start()
      c.executor.runAll()
      c.describe(None, None)
      c.registered()
      c.descriptions.last.complete(RoutedResult(new PartitionDescription(Optional.empty(), Optional.of(c.owner)), route))
      c.currentRoute = route.copy(leaderEpoch = 11)
      c.executor.runAll()
      assertEquals(2, c.registrations.size)
      assertSame(c.registrations.head._1, c.registrations.last._1)
      assertTrue(c.reads.isEmpty)
      c.registered()
      c.describe(Some(a), Some(c.owner))
      assertEquals(Seq(3L), c.reads.toSeq)
    } finally c.close()
  }

  @Test
  def testOwnerNotCommittedReattachesToBarrierAndRetainsPendingAppend(): Unit = {
    val c = new Context
    try {
      c.read = _ => c.batch(a)
      c.start()
      val request = c.appends.head._1
      c.completeAppend(AppendStatus.OWNER_NOT_COMMITTED)
      assertSame(c.registrations.head._1, c.registrations.last._1)
      c.registered()
      c.describe(None, Some(c.owner))
      assertSame(request, c.appends.last._1)
      assertEquals(Seq(0L), c.reads.toSeq)
    } finally c.close()
  }

  @Test
  def testLateAppendCallbackCannotReadAfterFollowerTransition(): Unit = {
    val c = new Context
    try {
      c.read = _ => c.batch(a)
      c.start()
      c.appends.last._2.complete(RoutedResult(new AppendResponse(AppendStatus.INDEXED, a, OptionalLong.of(0), Optional.of(a), 10), route))
      c.listener.onBecomingFollower(tp)
      c.executor.runAll()
      assertTrue(c.indexer.isStopped)
      assertEquals(Seq(0L), c.reads.toSeq)
      assertEquals(1, c.appends.size)
    } finally c.close()
  }

  @Test
  def testFailureAndDeletionCancelPendingRoutingWaits(): Unit = {
    for (deleted <- Seq(false, true)) {
      val c = new Context
      try {
        c.indexer.start()
        c.executor.runAll()
        if (deleted) c.listener.onDeleted(tp) else c.listener.onFailed(tp)
        assertTrue(c.descriptions.head.isCancelled)
        c.executor.runAll()
        assertTrue(c.registrations.isEmpty)
      } finally c.close()
    }
  }

  @Test
  def testFencedAppendStopsWithoutSkippingOrRetrying(): Unit = {
    val c = new Context
    try {
      c.read = _ => c.batch(a)
      c.start()
      c.completeAppend(AppendStatus.FENCED)
      c.time.sleep(10000)
      c.executor.runAll()
      assertTrue(c.indexer.isStopped)
      assertEquals(1, c.appends.size)
      assertEquals(Seq(0L), c.reads.toSeq)
    } finally c.close()
  }

  @Test
  def testAuthorizationAndSourceReadFailuresStop(): Unit = {
    val c = new Context
    try {
      c.indexer.start()
      c.executor.runAll()
      c.descriptions.head.completeExceptionally(new ClusterAuthorizationException("denied"))
      c.executor.runAll()
      assertTrue(c.indexer.isStopped)
      assertTrue(c.registrations.isEmpty)
    } finally c.close()
    val d = new Context
    try {
      d.read = _ => throw new InvalidRequestException("missing source data")
      d.start()
      assertTrue(d.indexer.isStopped)
      assertTrue(d.appends.isEmpty)
    } finally d.close()
  }
  @Test
  def testSharedWorkerCanProgressAnotherIndexerWhileAppendIsPending(): Unit = {
    val workers = new GlobalSequenceTestExecutor
    val first = new Context(workers)
    val secondKey = new PartitionKey(key.topicId(), 1)
    val secondBatch = new PhysicalBatch(secondKey, 0, 1, 2)
    val second = new Context(workers, secondKey)
    try {
      first.read = _ => first.batch(a)
      second.read = _ => second.batch(secondBatch)
      first.start()
      assertFalse(first.appends.head._2.isDone)
      second.start()
      assertEquals(1, second.appends.size)
      assertEquals(secondBatch, second.appends.head._1.batch())
      assertFalse(first.appends.head._2.isDone)
    } finally {
      first.close()
      second.close()
    }
  }

  @Test
  def testSourceStopsDuringReadWithoutSubmittingTheResult(): Unit = {
    val c = new Context
    try {
      c.read = _ => {
        c.listener.onBecomingFollower(tp)
        c.batch(a)
      }
      c.start()
      assertTrue(c.indexer.isStopped)
      assertTrue(c.appends.isEmpty)
    } finally c.close()
  }

  @Test
  def testObsoleteQueuedAppendSuccessUsesBarrierProgressBeforeReadingNextBatch(): Unit = {
    val c = new Context
    try {
      c.read = offset => if (offset == 0) c.batch(a) else c.batch(b)
      c.start()
      c.appends.last._2.complete(RoutedResult(new AppendResponse(AppendStatus.INDEXED, a, OptionalLong.of(0), Optional.of(a), 10), route))
      c.currentRoute = route.copy(leaderEpoch = 11)
      c.executor.runAll()
      assertEquals(Seq(0L), c.reads.toSeq)
      assertSame(c.registrations.head._1, c.registrations.last._1)
      c.registered()
      c.describe(Some(a), Some(c.owner))
      assertEquals(Seq(0L, 3L), c.reads.toSeq)
      assertEquals(b, c.appends.last._1.batch())
      assertEquals(0L, c.appends.last._1.predecessorBaseOffset())
    } finally c.close()
  }

  @Test
  def testSourceGapRechecksAuthoritativeProgressBeforeResuming(): Unit = {
    val c = new Context
    try {
      c.read = offset => if (offset == 0) throw new SourceLogGapException(key, 0, 3, 3, 5, "retained prefix moved") else c.batch(b)
      c.start()
      assertFalse(c.indexer.isStopped)
      assertEquals(Seq(0L), c.reads.toSeq)
      assertSame(c.registrations.head._1, c.registrations.last._1)
      c.registered()
      c.describe(Some(a), Some(c.owner))
      assertEquals(Seq(0L, 3L), c.reads.toSeq)
      assertEquals(b, c.appends.last._1.batch())
      assertEquals(0L, c.appends.last._1.predecessorBaseOffset())
    } finally c.close()
  }

  @Test
  def testUnrecoverableGapStopsAfterBarrierAndRecheckWithoutJumpingToLogStart(): Unit = {
    val c = new Context
    try {
      c.read = offset => throw new SourceLogGapException(key, offset, 3, 3, 5, "missing required batch")
      c.start()
      c.registered()
      c.describe(None, Some(c.owner))
      assertTrue(c.indexer.isStopped)
      assertTrue(c.indexer.failure.get.isInstanceOf[SourceLogGapException])
      assertEquals(Seq(0L, 0L), c.reads.toSeq)
      assertTrue(c.appends.isEmpty)
      assertEquals(2, c.registrations.size)
    } finally c.close()
  }

  @Test
  def testOrderingRejectionRechecksProgressThenRetriesTheSamePendingRequest(): Unit = {
    val c = new Context
    try {
      c.read = _ => c.batch(a)
      c.start()
      val request = c.appends.head._1
      c.completeAppend(AppendStatus.OUT_OF_ORDER)
      assertFalse(c.indexer.isStopped)
      c.registered()
      c.describe(None, Some(c.owner))
      assertSame(request, c.appends.last._1)
      c.completeAppend(AppendStatus.OUT_OF_ORDER)
      assertTrue(c.indexer.isStopped)
      assertTrue(c.indexer.failure.get.isInstanceOf[InvalidRequestException])
      assertEquals(Seq(0L), c.reads.toSeq)
    } finally c.close()
  }

  @Test
  def testLeaderChangeDuringGapRecheckRequiresAnotherCurrentBarrier(): Unit = {
    val c = new Context
    try {
      c.read = offset => throw new SourceLogGapException(key, offset, 3, 3, 5, "missing prefix")
      c.start()
      c.registered()
      c.descriptions.last.complete(RoutedResult(new PartitionDescription(Optional.empty(), Optional.of(c.owner)), route))
      c.currentRoute = route.copy(leaderEpoch = 11)
      c.executor.runAll()
      assertFalse(c.indexer.isStopped)
      assertEquals(Seq(0L), c.reads.toSeq)
      c.registered()
      c.describe(None, Some(c.owner))
      assertTrue(c.indexer.isStopped)
      assertEquals(Seq(0L, 0L), c.reads.toSeq)
    } finally c.close()
  }

  @Test
  def testGapRecoveryCanFenceTheOldOwnerWithoutReadingAgain(): Unit = {
    val c = new Context
    try {
      c.read = offset => throw new SourceLogGapException(key, offset, 3, 3, 5, "missing prefix")
      c.start()
      c.registrations.last._2.complete(RoutedResult(new RegistrationResponse(false, Optional.of(c.owner), 10), route))
      c.executor.runAll()
      assertTrue(c.indexer.isStopped)
      assertEquals(Seq(0L), c.reads.toSeq)
    } finally c.close()
  }

  @Test
  def testGapRecoveryPreservesValidatedControlOnlyCursorWithinTheSameSourceLifetime(): Unit = {
    val c = new Context
    try {
      c.read = offset => if (offset == 0) ReadResult(key, 3, 5, 3, None, CONTINUE)
        else throw new SourceLogGapException(key, offset, 4, 4, 5, "missing data after control prefix")
      c.start()
      assertEquals(Seq(0L, 3L), c.reads.toSeq)
      c.registered()
      c.describe(None, Some(c.owner))
      assertTrue(c.indexer.isStopped)
      assertEquals(Seq(0L, 3L, 3L), c.reads.toSeq)
      assertTrue(c.appends.isEmpty)
    } finally c.close()
  }

}
