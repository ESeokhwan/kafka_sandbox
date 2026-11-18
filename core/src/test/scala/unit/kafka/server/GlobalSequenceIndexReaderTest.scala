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
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookup.{Position, Request, Result, Snapshot}
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.{FetchDataInfo, LogOffsetMetadata, LogReadInfo, UnifiedLog}
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, anyLong}
import org.mockito.Mockito.{mock, when}

import java.util.Optional
import java.nio.ByteBuffer
import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.collection.mutable.ArrayBuffer
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
  private def records(entries: Seq[CoordinatorRecord], offset: Long = 0, compression: Compression = Compression.NONE): MemoryRecords =
    MemoryRecords.withRecords(offset, compression,
      entries.map(record => new SimpleRecord(serde.serializeKey(record), serde.serializeValue(record))): _*)

  private def combine(records: MemoryRecords*): MemoryRecords = {
    val buffer = ByteBuffer.allocate(records.map(_.sizeInBytes()).sum)
    records.foreach(data => buffer.put(data.buffer().duplicate()))
    buffer.flip()
    MemoryRecords.readableRecords(buffer)
  }

  private class Context(initial: Records = records(history), snapshotHw: Long = 6, globalEnd: Long = 5, scanBytes: Int = Int.MaxValue,
                        cacheEntries: Int = 100000, cachePerTopic: Int = 10000) extends AutoCloseable {
    val time = new MockTime(0, 0)
    val workers = new GlobalSequenceTestExecutor
    val replicas: ReplicaManager = mock(classOf[ReplicaManager])
    val partition: Partition = mock(classOf[Partition])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    var currentLog: UnifiedLog = log
    var contents = initial
    var generation: AnyRef = new Object
    var hw = 8L
    var start = 0L
    var fenced = false
    var reads = 0
    var bytesRead = 0L
    val readOffsets = ArrayBuffer.empty[Long]
    var onRead: () => Unit = () => ()
    var onBoundsRead: () => Unit = () => ()
    when(replicas.getPartitionOrException(any[TopicIdPartition])).thenReturn(partition)
    when(log.topicId()).thenReturn(Optional.of(index))
    when(log.logEndOffset()).thenReturn(8L)
    when(partition.localLogWithEpochOrThrow(any(), anyBoolean())).thenAnswer { _ =>
      if (fenced) throw new FencedLeaderEpochException("moved")
      currentLog
    }
    when(partition.fetchRecords(any(), any(), anyLong(), anyInt(), anyBoolean(), anyBoolean())).thenAnswer { call =>
      val request = call.getArgument[FetchRequest.PartitionData](1)
      assertEquals(Optional.of(Int.box(snapshot.leaderEpoch())), request.currentLeaderEpoch)
      assertFalse(call.getArgument[Boolean](5))
      val data = if (call.getArgument[Int](3) == 0) { onBoundsRead(); MemoryRecords.EMPTY } else {
        reads += 1
        readOffsets += request.fetchOffset
        onRead()
        val batches = contents.batches().asScala.filter(_.lastOffset() >= request.fetchOffset).toList
        val buffer = ByteBuffer.allocate(batches.map(_.sizeInBytes()).sum)
        batches.foreach(_.writeTo(buffer))
        buffer.flip()
        val data = MemoryRecords.readableRecords(buffer)
        bytesRead += data.sizeInBytes()
        data
      }
      new LogReadInfo(new FetchDataInfo(new LogOffsetMetadata(request.fetchOffset), data), Optional.empty(), hw, start, 8L, 0L)
    }
    val limits = new GlobalSequenceTestResources(time, Map(
      org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorConfig.LOOKUP_SCAN_MAX_BYTES_CONFIG -> Int.box(scanBytes)))
    val cache = new GlobalSequenceIndexCache(cacheEntries, cachePerTopic)
    val reader = new GlobalSequenceIndexReader(replicas, time.scheduler, time, workers,
      if (scanBytes == Int.MaxValue) null else limits.resources, cache)
    var snapshot = new Snapshot(index, 1, 10, snapshotHw, globalEnd)
    def read(startOffset: Long = 0, endOffset: Long = 100, maxBatches: Int = 100,
             position: Position = Position.START): CompletableFuture[Result] = {
      val request = new Request(topic, startOffset, endOffset, maxBatches, 100)
      reader.read(request, snapshot, position, generation, time.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(100))
    }
    def finish(future: CompletableFuture[Result]): Result = { workers.runAll(); future.join() }
    override def close(): Unit = { reader.close(); limits.close(); time.scheduler.clear() }
  }

  @Test
  def testCacheHitsCoverBatchInteriorAndPartialMissResumesAfterCachedPair(): Unit = {
    val c = new Context
    val cold = new Context(cacheEntries = 0)
    try {
      c.finish(c.read(1, 3, 1))
      val before = c.reads
      assertEquals(cold.finish(cold.read(1, 2)), c.finish(c.read(1, 2)))
      assertEquals(before, c.reads, "A complete hit must not fetch index payload")
      val page = c.finish(c.read(1, 5))
      assertEquals(cold.finish(cold.read(1, 5)), page)
      assertEquals(Seq(0L, 2L), c.readOffsets.toSeq, "A partial hit resumes after the validated allocation pair")
      val warmed = c.reads
      assertEquals(page, c.finish(c.read(1, 5)))
      assertEquals(warmed, c.reads)
      val first = c.finish(c.read(1, 5, 1))
      assertEquals(3L, first.nextGlobalOffset())
      assertEquals(4L, c.finish(c.read(first.nextGlobalOffset(), 4)).nextGlobalOffset())
    } finally { c.close(); cold.close() }
  }

  @Test
  def testCacheOnlyStoresSelectedMappingsAndEvictedMappingsCanBeRestored(): Unit = {
    val c = new Context(cacheEntries = 1, cachePerTopic = 1)
    try {
      c.finish(c.read(3, 5))
      assertEquals(1, c.cache.size)
      c.finish(c.read(0, 3))
      assertEquals(2, c.reads, "Scanning past a mapping must not warm it")
      c.finish(c.read(0, 3))
      assertEquals(2, c.reads)
      c.finish(c.read(3, 5))
      assertEquals(3, c.reads, "An evicted mapping must be restored from the index")
      assertEquals(1, c.cache.size)
    } finally c.close()
  }

  @Test
  def testNewerCachedMappingsCannotExpandAnOlderSnapshot(): Unit = {
    val c = new Context(snapshotHw = 8, globalEnd = 6)
    try {
      c.finish(c.read(0, 6))
      c.snapshot = new Snapshot(index, 1, 10, 6, 5)
      val reads = c.reads
      assertEquals(5L, c.finish(c.read(0, 100)).nextGlobalOffset())
      assertEquals(reads, c.reads)
      // The [3, 5) mapping ends at internal offset 6 and is too new for this HW.
      c.snapshot = new Snapshot(index, 1, 10, 4, 5)
      val invalid = c.read(3, 5)
      c.workers.runAll()
      assertFutureThrows(classOf[CorruptRecordException], invalid)
      assertEquals(reads + 1, c.reads, "An ineligible cache entry must not hide missing committed history")
    } finally c.close()
  }

  @Test
  def testShardLogAndLeaderChangesRequireColdLookupAndHitsAreRevalidated(): Unit = {
    val c = new Context
    try {
      c.finish(c.read())
      c.generation = new Object
      c.finish(c.read())
      assertEquals(2, c.reads)
      val replacement = mock(classOf[UnifiedLog])
      when(replacement.topicId()).thenReturn(Optional.of(index))
      when(replacement.logEndOffset()).thenReturn(8L)
      c.currentLog = replacement
      c.finish(c.read())
      assertEquals(3, c.reads)
      c.snapshot = new Snapshot(index, 1, 11, 6, 5)
      c.finish(c.read())
      assertEquals(4, c.reads)
      var checks = 0
      c.onBoundsRead = () => {
        checks += 1
        if (checks == 2) c.fenced = true
      }
      val fenced = c.read()
      c.workers.runAll()
      assertFutureThrows(classOf[NotCoordinatorException], fenced)
      assertEquals(4, c.reads, "A full cache hit still validates its log and leader")
    } finally c.close()
  }

  @Test
  def testFailedPageDoesNotPopulateTheCache(): Unit = {
    val bad = records(history.take(5) :+ newTopicMetadataRecord(topic, 99))
    val c = new Context(bad)
    try {
      val result = c.read()
      c.workers.runAll()
      assertFutureThrows(classOf[CorruptRecordException], result)
      assertEquals(0, c.cache.size)
      c.contents = records(history)
      c.finish(c.read())
      assertEquals(2, c.reads)
    } finally c.close()
  }

  @Test
  def testSparseSeekInsideCompressedKafkaBatchMatchesColdScanAndReadsLess(): Unit = {
    val data = combine(records(history.take(2)),
      records(history.slice(2, 6), 2, Compression.gzip().build()), records(history.drop(6), 6))
    val cold = new Context(data)
    val sparse = new Context(data)
    try {
      val expected = cold.finish(cold.read(4, 5, 1))
      val result = sparse.finish(sparse.read(4, 5, 1, new Position(3, 4)))
      assertEquals(expected, result)
      assertEquals(Seq(4L), sparse.readOffsets.toSeq)
      assertEquals(Seq(0L), cold.readOffsets.toSeq)
      assertTrue(sparse.bytesRead < cold.bytesRead)
      assertEquals(3L, result.mappings().get(0).globalBaseOffset())
      assertEquals(5L, result.nextGlobalOffset())
    } finally { cold.close(); sparse.close() }
  }

  @Test
  def testSparseSeekRetainsPaginationAndSnapshotBoundary(): Unit = {
    val c = new Context(snapshotHw = 8, globalEnd = 6)
    try {
      val first = c.finish(c.read(4, 100, 1, new Position(3, 4)))
      assertEquals(Seq(3L), first.mappings().asScala.map(_.globalBaseOffset()).toSeq)
      assertEquals(5L, first.nextGlobalOffset())
      val second = c.finish(c.read(first.nextGlobalOffset(), 100, 1, new Position(5, 6)))
      assertEquals(Seq(5L), second.mappings().asScala.map(_.globalBaseOffset()).toSeq)
      assertEquals(6L, second.nextGlobalOffset())
    } finally c.close()
    val old = new Context
    try {
      // A position learned after this snapshot cannot expand its visible range.
      val result = old.finish(old.read(4, 100, 100, new Position(5, 6)))
      assertEquals(Seq(0L), old.readOffsets.toSeq)
      assertEquals(5L, result.nextGlobalOffset())
      assertEquals(Seq(3L), result.mappings().asScala.map(_.globalBaseOffset()).toSeq)
    } finally old.close()
  }

  @Test
  def testSparsePositionMustIdentifyTheRequestedTopicsCompleteAllocation(): Unit = {
    for (position <- Seq(new Position(0, 2), new Position(4, 4), new Position(3, 5))) {
      val c = new Context(snapshotHw = 8, globalEnd = 6)
      try {
        val result = c.read(4, 6, 100, position)
        c.workers.runAll()
        assertFutureThrows(classOf[CorruptRecordException], result)
      } finally c.close()
    }
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
