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

import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.{OffsetOutOfRangeException, RecordTooLargeException}
import org.apache.kafka.common.message.FetchGlobalSequenceRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.FetchGlobalSequenceResponse
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{PartitionKey, PhysicalBatch}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookup
import org.apache.kafka.server.util.MockTime
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, verifyNoInteractions, when}

import java.util.concurrent.CompletableFuture
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class GlobalSequenceFetchManagerTest {
  import GlobalSequenceFetch._
  private val topic = new Uuid(1, 2)
  private val route = IndexRoutingManager.CoordinatorLocation(new Uuid(3, 4), 0, new Node(1, "localhost", 9092), 10, 7)
  private def mapping(global: Long, count: Int = 1) = new GlobalSequenceLookup.Mapping(global,
    new PhysicalBatch(new PartitionKey(topic, (global % 2).toInt), global * 10, global * 10 + count - 1, count))
  private def data(batch: PhysicalBatch, bytes: Int = 1): Data = Data(MemoryRecords.withRecords(batch.baseOffset(), Compression.NONE,
    (0 until batch.recordCount()).map(_ => new SimpleRecord(new Array[Byte](bytes))): _*), 3, batch.lastOffset() + 1)
  private case class Work(batch: PhysicalBatch, deadline: Long, future: CompletableFuture[Data])
  private class Context(entries: Seq[GlobalSequenceLookup.Mapping] = (0L until 6L).map(mapping(_)),
                        maxBytes: Int = MaxPayloadBytes, startOffset: Long = 0, endOffset: Long = Long.MaxValue) extends AutoCloseable {
    val time = new MockTime(0, 0)
    val index: IndexRoutingManager = mock(classOf[IndexRoutingManager])
    val source: GlobalSequenceDataRouter = mock(classOf[GlobalSequenceDataRouter])
    val lookup = new CompletableFuture[IndexRoutingManager.RoutedResult[GlobalSequenceLookup.Result]]()
    val request = Request(new GlobalSequenceLookup.Request(topic, startOffset, endOffset, 1000, 1000), maxBytes)
    val work = ArrayBuffer.empty[Work]
    when(index.lookupIndex(any())).thenReturn(lookup)
    when(index.isCurrent(any(), any())).thenReturn(true)
    when(source.read(any(), anyLong())).thenAnswer { call =>
      val item = Work(call.getArgument[PhysicalBatch](0), call.getArgument[Long](1), new CompletableFuture[Data]())
      work += item
      item.future
    }
    val manager = new GlobalSequenceFetchManager(index, source, time.scheduler, time)
    val future = manager.fetch(request)
    def loaded(): Unit = {
      val end = entries.lastOption.map(_.globalEndOffset()).getOrElse(startOffset)
      lookup.complete(IndexRoutingManager.RoutedResult(new GlobalSequenceLookup.Result(
        new GlobalSequenceLookup.Snapshot(route.indexTopicId, 0, 10, 100, end), entries.asJava,
        math.min(endOffset, end)), route))
    }
    def complete(position: Int, size: Int = 1): Unit = work(position).future.complete(data(work(position).batch, size))
    def response: FetchGlobalSequenceResponse = future.join()
    override def close(): Unit = { manager.close(); time.scheduler.clear() }
  }

  @Test
  def testReverseCompletionsRespectGlobalOrderAndBoundTheReadWindow(): Unit = {
    val c = new Context
    try {
      verifyNoInteractions(c.source)
      c.loaded()
      assertEquals(ParallelReads, c.work.size)
      (1 until ParallelReads).reverse.foreach(c.complete(_))
      assertFalse(c.future.isDone)
      assertEquals(ParallelReads, c.work.size, "Completed later reads must not expand the window past the blocked prefix")
      c.complete(0)
      assertEquals(6, c.work.size)
      c.complete(5)
      assertFalse(c.future.isDone)
      c.complete(4)
      val response = c.response.data()
      assertEquals(Errors.NONE.code(), response.errorCode())
      assertEquals(6L, response.nextGlobalOffset())
      assertEquals(0L until 6L, response.batches().asScala.map(_.globalBaseOffset()).toSeq)
      response.batches().asScala.foreach { entry =>
        val original = entry.records().asInstanceOf[MemoryRecords].batches().iterator().next()
        assertEquals(entry.globalBaseOffset() * 10, original.baseOffset())
        original.ensureValid()
      }
    } finally c.close()
  }

  @Test
  def testFrontFailureReturnsOnlyTheCompletedPrefix(): Unit = {
    for (failed <- Seq(0, 1)) {
      val c = new Context
      try {
        c.loaded()
        c.complete(2)
        c.work(failed).future.completeExceptionally(new OffsetOutOfRangeException("retained index, deleted data"))
        if (failed == 1) { assertFalse(c.future.isDone); c.complete(0) }
        val response = c.response.data()
        assertEquals(Errors.OFFSET_OUT_OF_RANGE.code(), response.errorCode())
        assertEquals(failed.toLong, response.nextGlobalOffset())
        assertEquals(failed, response.batches().size())
        assertTrue(c.work(3).future.isCancelled)
      } finally c.close()
    }
  }

  @Test
  def testSoftByteLimitIncludesFullBatchesAndIgnoresErrorsBeyondPage(): Unit = {
    val c = new Context(maxBytes = 1)
    try {
      c.loaded()
      c.work(1).future.completeExceptionally(new OffsetOutOfRangeException("outside this page"))
      c.complete(0, 1000)
      assertEquals(Errors.NONE.code(), c.response.data().errorCode())
      assertEquals(1, c.response.data().batches().size())
      assertTrue(c.response.data().batches().get(0).records().sizeInBytes() > 1)
      assertEquals(1L, c.response.data().nextGlobalOffset())
      assertTrue(c.work(2).future.isCancelled)
    } finally c.close()
    val batchSize = data(mapping(0).batch()).records.sizeInBytes()
    val limited = new Context(maxBytes = batchSize + 1)
    try {
      limited.loaded()
      limited.complete(0)
      limited.complete(1)
      assertEquals(1L, limited.response.data().nextGlobalOffset())
      assertEquals(1, limited.response.data().batches().size())
    } finally limited.close()
  }

  @Test
  def testPartialBatchSelectionKeepsOriginalRecordsAndExactCursor(): Unit = {
    val c = new Context(Seq(mapping(0, 3)), startOffset = 1, endOffset = 2)
    try {
      c.loaded()
      c.complete(0)
      val response = c.response.data()
      val batch = response.batches().get(0)
      assertEquals(1L, batch.selectedGlobalStartOffset())
      assertEquals(2L, batch.selectedGlobalEndOffset())
      assertEquals(0L, batch.physicalBaseOffset())
      assertEquals(2L, batch.physicalLastOffset())
      assertEquals(3, batch.recordCount())
      assertEquals(2L, response.nextGlobalOffset())
    } finally c.close()
  }

  @Test
  def testOneDeadlineIncludesLookupAndPreservesOnlyCompletedPrefixOnTimeout(): Unit = {
    val c = new Context
    try {
      c.time.sleep(600)
      c.loaded()
      assertTrue(c.work.forall(_.deadline == 1000000000L))
      c.complete(0)
      c.complete(2)
      c.time.sleep(400)
      assertEquals(Errors.REQUEST_TIMED_OUT.code(), c.response.data().errorCode())
      assertEquals(1L, c.response.data().nextGlobalOffset())
      assertEquals(1, c.response.data().batches().size())
      assertTrue(c.work(1).future.isCancelled)
    } finally c.close()
  }

  @Test
  def testIndexRouteChangeDropsTheObsoletePage(): Unit = {
    val c = new Context(Seq(mapping(0)))
    try {
      c.loaded()
      when(c.index.isCurrent(any(), any())).thenReturn(false)
      c.complete(0)
      assertEquals(Errors.NOT_COORDINATOR.code(), c.response.data().errorCode())
      assertEquals(0L, c.response.data().nextGlobalOffset())
      assertTrue(c.response.data().batches().isEmpty)
    } finally c.close()
  }

  @Test
  def testCancellationAndOversizedBatchLeaveNoSkippedCursor(): Unit = {
    val c = new Context
    try {
      c.loaded()
      c.future.cancel(false)
      assertTrue(c.work.forall(_.future.isCancelled))
    } finally c.close()
    val large = new Context
    try {
      large.loaded()
      large.work.head.future.completeExceptionally(new RecordTooLargeException("hard cap"))
      assertEquals(Errors.MESSAGE_TOO_LARGE.code(), large.response.data().errorCode())
      assertEquals(0L, large.response.data().nextGlobalOffset())
    } finally large.close()
  }

  @Test
  def testManyImmediateCompletionsDoNotRecurseAndEmptyPagesNeedNoDataReads(): Unit = {
    val c = new Context((0L until 1000L).map(mapping(_)))
    try {
      when(c.source.read(any(), anyLong())).thenAnswer { call => CompletableFuture.completedFuture(data(call.getArgument[PhysicalBatch](0))) }
      c.loaded()
      assertEquals(1000, c.response.data().batches().size())
      assertEquals(1000L, c.response.data().nextGlobalOffset())
    } finally c.close()
    val empty = new Context(Seq.empty, startOffset = 5)
    try {
      empty.loaded()
      assertEquals(5L, empty.response.data().nextGlobalOffset())
      verifyNoInteractions(empty.source)
    } finally empty.close()
  }

  @Test
  def testRequestRejectsUnsupportedIsolationAndInvalidByteBudgets(): Unit = {
    val request = new FetchGlobalSequenceRequestData().setTopicId(topic).setGlobalEndOffsetExclusive(1)
    assertThrows(classOf[IllegalArgumentException], () => GlobalSequenceFetch.request(request.setMaxBytes(0)))
    assertThrows(classOf[IllegalArgumentException], () => GlobalSequenceFetch.request(request.setMaxBytes(MaxPayloadBytes + 1)))
    request.setMaxBytes(1).setIsolationLevel(1.toByte)
    assertThrows(classOf[org.apache.kafka.common.errors.InvalidRequestException], () => GlobalSequenceFetch.request(request))
  }
}
