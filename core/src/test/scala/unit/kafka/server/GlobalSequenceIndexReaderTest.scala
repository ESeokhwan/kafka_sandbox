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
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.{CoordinatorNotAvailableException, CorruptRecordException, FencedLeaderEpochException, KafkaStorageException, NotCoordinatorException, TimeoutException}
import org.apache.kafka.common.record.{MemoryRecords, Records, SimpleRecord}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.{newBatchIndexRecord, newTopicMetadataRecord}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordSerde
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookup.{Request, Result, Snapshot}
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.{FetchDataInfo, LogOffsetMetadata, LogReadInfo, UnifiedLog}
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, anyLong}
import org.mockito.Mockito.{mock, when}

import java.util.Optional
import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.jdk.CollectionConverters._

class GlobalSequenceIndexReaderTest {
  private val topic = new Uuid(1, 2)
  private val other = new Uuid(5, 6)
  private val index = new Uuid(3, 4)
  private val serde = new GlobalSequenceCoordinatorRecordSerde
  private def allocation(id: Uuid, global: Long, partition: Int, base: Long, count: Int): Seq[CoordinatorRecord] = Seq(
    newBatchIndexRecord(id, partition, base, base + count - 1, count, global), newTopicMetadataRecord(id, global + count))
  private val history = allocation(topic, 0, 0, 10, 3) ++ allocation(other, 0, 0, 0, 2) ++
    allocation(topic, 3, 1, 0, 2) ++ allocation(topic, 5, 0, 13, 1)
  private def records(entries: Seq[CoordinatorRecord], offset: Long = 0): MemoryRecords =
    MemoryRecords.withRecords(offset, Compression.NONE,
      entries.map(record => new SimpleRecord(serde.serializeKey(record), serde.serializeValue(record))): _*)

  private class Context(initial: Records = records(history), snapshotHw: Long = 6, globalEnd: Long = 5, scanBytes: Int = Int.MaxValue) extends AutoCloseable {
    val time = new MockTime(0, 0)
    val workers = new GlobalSequenceTestExecutor
    val replicas: ReplicaManager = mock(classOf[ReplicaManager])
    val partition: Partition = mock(classOf[Partition])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    var hw = 8L
    var start = 0L
    var fenced = false
    var reads = 0
    var onRead: () => Unit = () => ()
    when(replicas.getPartitionOrException(any[TopicIdPartition])).thenReturn(partition)
    when(log.topicId()).thenReturn(Optional.of(index))
    when(log.logEndOffset()).thenReturn(8L)
    when(partition.localLogWithEpochOrThrow(any(), anyBoolean())).thenAnswer { _ =>
      if (fenced) throw new FencedLeaderEpochException("moved")
      log
    }
    when(partition.fetchRecords(any(), any(), anyLong(), anyInt(), anyBoolean(), anyBoolean())).thenAnswer { call =>
      val request = call.getArgument[FetchRequest.PartitionData](1)
      assertEquals(Optional.of(Int.box(10)), request.currentLeaderEpoch)
      assertFalse(call.getArgument[Boolean](5))
      val data = if (call.getArgument[Int](3) == 0) MemoryRecords.EMPTY else {
        reads += 1
        onRead()
        initial
      }
      new LogReadInfo(new FetchDataInfo(new LogOffsetMetadata(request.fetchOffset), data), Optional.empty(), hw, start, 8L, 0L)
    }
    val limits = new GlobalSequenceTestResources(time, Map(
      org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorConfig.LOOKUP_SCAN_MAX_BYTES_CONFIG -> Int.box(scanBytes)))
    val reader = new GlobalSequenceIndexReader(replicas, time.scheduler, time, workers, if (scanBytes == Int.MaxValue) null else limits.resources)
    val snapshot = new Snapshot(index, 1, 10, snapshotHw, globalEnd)
    def read(startOffset: Long = 0, endOffset: Long = 100, maxBatches: Int = 100): CompletableFuture[Result] = {
      val request = new Request(topic, startOffset, endOffset, maxBatches, 100)
      reader.read(request, snapshot, time.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(100))
    }
    def finish(future: CompletableFuture[Result]): Result = { workers.runAll(); future.join() }
    override def close(): Unit = { reader.close(); limits.close(); time.scheduler.clear() }
  }

  @Test
  def testScanLimitAndCancellationKeepQueuedAndRunningWorkBounded(): Unit = {
    import org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources.Scope.INDEX_READ
    val c = new Context(scanBytes = 1)
    try {
      val first = c.read()
      assertFutureThrows(classOf[org.apache.kafka.common.errors.ThrottlingQuotaExceededException], c.read())
      first.cancel(false)
      assertEquals(0L, c.limits.resources.used(INDEX_READ))
      val running = c.read()
      c.onRead = () => {
        running.cancel(false)
        assertEquals(1L, c.limits.resources.used(INDEX_READ), "Canceled active I/O must hold admission until its worker returns")
        assertFutureThrows(classOf[org.apache.kafka.common.errors.ThrottlingQuotaExceededException], c.read())
      }
      c.workers.runAll()
      assertEquals(0L, c.limits.resources.used(INDEX_READ))
      c.onRead = () => ()
      val overBudget = c.read()
      c.workers.runAll()
      assertFutureThrows(classOf[org.apache.kafka.common.errors.ThrottlingQuotaExceededException], overBudget)
      assertEquals(0L, c.limits.resources.used(INDEX_READ))
    } finally c.close()
  }

  @Test
  def testPageSelectionAcrossInterleavedPartitionsAndTopics(): Unit = {
    val c = new Context
    try {
      val first = c.finish(c.read(1, 100, 1))
      assertEquals(Seq(0L), first.mappings().asScala.map(_.globalBaseOffset()).toSeq)
      assertEquals(10L, first.mappings().get(0).batch().baseOffset())
      assertEquals(3L, first.nextGlobalOffset())
      assertEquals(5L, first.snapshot().committedGlobalEnd())
      val second = c.finish(c.read(first.nextGlobalOffset(), 4, 1))
      assertEquals(1, second.mappings().get(0).batch().partition().partition())
      assertEquals(3L, second.mappings().get(0).globalBaseOffset())
      assertEquals(4L, second.nextGlobalOffset())
      val full = c.finish(c.read())
      assertEquals(Seq(0L, 3L), full.mappings().asScala.map(_.globalBaseOffset()).toSeq)
      assertEquals(5L, full.nextGlobalOffset())
      assertEquals(full, c.finish(c.read()), "Cold repeat lookups must produce the same mappings")
    } finally c.close()
  }

  @Test
  def testSnapshotDoesNotExpandWhenHighWatermarkAdvances(): Unit = {
    val c = new Context
    try {
      c.hw = 6
      c.onRead = () => c.hw = 8
      val result = c.finish(c.read())
      assertEquals(6L, result.snapshot().indexHighWatermark())
      assertEquals(5L, result.nextGlobalOffset())
      assertEquals(2, result.mappings().size())
    } finally c.close()
  }

  @Test
  def testEmptyRangeAndCommittedEndNeedNoPayloadRead(): Unit = {
    val c = new Context
    try {
      assertTrue(c.finish(c.read(2, 2)).mappings().isEmpty)
      val end = c.finish(c.read(5, 100))
      assertTrue(end.mappings().isEmpty)
      assertEquals(5L, end.nextGlobalOffset())
      assertEquals(0, c.reads)
    } finally c.close()
  }

  @Test
  def testMissingAllocationPairCannotSatisfyCommittedSnapshot(): Unit = {
    val c = new Context(records(history.take(1)), snapshotHw = 1, globalEnd = 3)
    try {
      val result = c.read()
      c.workers.runAll()
      assertFutureThrows(classOf[CorruptRecordException], result)
    } finally c.close()
  }

  @Test
  def testMismatchedAllocationAndMissingGlobalRangeAreRejected(): Unit = {
    val invalid = Seq(
      records(Seq(history.head, newTopicMetadataRecord(topic, 4))),
      records(allocation(topic, 1, 0, 10, 3)))
    invalid.foreach { data =>
      val c = new Context(data, snapshotHw = 2, globalEnd = 4)
      try {
        val result = c.read()
        c.workers.runAll()
        assertFutureThrows(classOf[CorruptRecordException], result)
      } finally c.close()
    }
  }

  @Test
  def testLeadershipChangeDuringScanRejectsLateResult(): Unit = {
    val c = new Context
    try {
      c.onRead = () => c.fenced = true
      val result = c.read()
      c.workers.runAll()
      assertFutureThrows(classOf[NotCoordinatorException], result)
    } finally c.close()
  }

  @Test
  def testMissingHistoryAndRegressedHighWatermark(): Unit = {
    val c = new Context
    try {
      c.start = 2
      val missing = c.read()
      c.workers.runAll()
      assertFutureThrows(classOf[KafkaStorageException], missing)
      c.start = 0
      c.hw = 4
      val regressed = c.read()
      c.workers.runAll()
      assertFutureThrows(classOf[NotCoordinatorException], regressed)
    } finally c.close()
  }

  @Test
  def testQueuedTimeoutAndCancellationDoNotReadTheLog(): Unit = {
    val c = new Context
    try {
      val timedOut = c.read()
      val cancelled = c.read()
      cancelled.cancel(false)
      c.time.sleep(100)
      c.workers.runAll()
      assertFutureThrows(classOf[TimeoutException], timedOut)
      assertTrue(cancelled.isCancelled)
      assertEquals(0, c.reads)
    } finally c.close()
  }

  @Test
  def testDeadlineWhileScanningAndReaderShutdown(): Unit = {
    val c = new Context
    try {
      c.onRead = () => c.time.sleep(100)
      val timedOut = c.read()
      c.workers.runAll()
      assertFutureThrows(classOf[TimeoutException], timedOut)
      val pending = c.read()
      c.reader.close()
      assertFutureThrows(classOf[CoordinatorNotAvailableException], pending)
      assertFutureThrows(classOf[CoordinatorNotAvailableException], c.read())
    } finally c.close()
  }
}
