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
import kafka.server.GlobalSequenceSourceReader.{AWAIT_HIGH_WATERMARK, BATCH, SourceLogGapException}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.{CorruptRecordException, FencedLeaderEpochException, InvalidRequestException, NotLeaderOrFollowerException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record.{DefaultRecordBatch, FileRecords, MemoryRecords, RecordBatch, Records, SimpleRecord, TimestampType}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.utils.{Crc32C, MockTime}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PartitionKey
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams}
import org.apache.kafka.storage.internals.log.{FetchDataInfo, LogConfig, LogOffsetMetadata, LogReadInfo, UnifiedLog}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, anyLong}
import org.mockito.Mockito.{doAnswer, mock, never, verify, when}

import java.nio.ByteBuffer
import java.util.{Map => JMap, Optional}

class GlobalSequenceSourceReaderValidationTest {
  private val key = new PartitionKey(new Uuid(1, 2), 0)
  private val tp = new TopicPartition("ordered", 0)
  private val topicIdPartition = new TopicIdPartition(key.topicId(), tp)
  private def records(base: Long = 0): MemoryRecords = MemoryRecords.withRecords(base, Compression.NONE,
    new SimpleRecord(Array[Byte](1)), new SimpleRecord(Array[Byte](2)), new SimpleRecord(Array[Byte](3)))

  private class Context(initial: Records = records(), initialHw: Long = 3) {
    val partition: Partition = mock(classOf[Partition])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val metadata: MetadataCache = mock(classOf[MetadataCache])
    var hw = initialHw
    var start = 0L
    var end = 3L
    var onRead: () => Unit = () => ()
    var fenced = false
    var reads = 0
    val config = new LogConfig(JMap.of(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "true", TopicConfig.CLEANUP_POLICY_CONFIG, "delete"))
    when(log.config()).thenReturn(config)
    when(log.topicId()).thenReturn(Optional.of(key.topicId()))
    when(log.logEndOffset()).thenAnswer(_ => Long.box(end))
    when(replicaManager.metadataCache).thenReturn(metadata)
    when(metadata.getTopicName(key.topicId())).thenReturn(Optional.of(tp.topic()))
    when(replicaManager.getPartitionOrException(topicIdPartition)).thenReturn(partition)
    when(partition.localLogWithEpochOrThrow(any(), anyBoolean())).thenReturn(log)
    when(partition.fetchRecords(any(), any(), anyLong(), anyInt(), anyBoolean(), anyBoolean())).thenAnswer { call =>
      val params = call.getArgument[FetchParams](0)
      val request = call.getArgument[FetchRequest.PartitionData](1)
      assertEquals(FetchIsolation.LOG_END, params.isolation)
      assertFalse(params.isFromFollower)
      assertTrue(params.fetchOnlyLeader())
      assertEquals(Optional.of(Int.box(3)), request.currentLeaderEpoch)
      assertEquals(params.maxBytes > 0, call.getArgument[Boolean](4))
      assertFalse(call.getArgument[Boolean](5))
      if (fenced) throw new FencedLeaderEpochException("source changed")
      val data = if (call.getArgument[Int](3) == 0) MemoryRecords.EMPTY else {
        reads += 1
        onRead()
        initial
      }
      new LogReadInfo(new FetchDataInfo(new LogOffsetMetadata(request.fetchOffset), data), Optional.empty(), hw, start, end, 0L)
    }
    val reader = new GlobalSequenceSourceReader(replicaManager, new MockTime())
    def read(): GlobalSequenceSourceReader.ReadResult = reader.read(key, 3, 0, 1024)
  }

  @Test
  def testHighWatermarkAdvanceDuringReadDoesNotExpandCapturedBoundary(): Unit = {
    val ctx = new Context(initialHw = 2)
    ctx.onRead = () => ctx.hw = 3
    val first = ctx.read()
    assertEquals(AWAIT_HIGH_WATERMARK, first.status)
    assertEquals(2L, first.dataHighWatermark)
    assertEquals(0L, first.nextPhysicalOffset)
    assertTrue(first.batch.isEmpty)
    val next = ctx.read()
    assertEquals(BATCH, next.status)
    assertEquals(3L, next.dataHighWatermark)
  }

  @Test
  def testNoPayloadReadAtHighWatermark(): Unit = {
    val ctx = new Context(initialHw = 0)
    assertEquals(AWAIT_HIGH_WATERMARK, ctx.read().status)
    assertEquals(0, ctx.reads)
  }

  @Test
  def testLocalHistoryMovedToRemoteStorageIsNotSkipped(): Unit = {
    val ctx = new Context
    when(ctx.log.localLogStartOffset()).thenReturn(2L)
    val error = assertThrows(classOf[SourceLogGapException], () => ctx.read())
    assertEquals(0L, error.logStartOffset)
    assertEquals(2L, error.localLogStartOffset)
    assertEquals(0, ctx.reads)
  }

  @Test
  def testMissingBatchAndEmptyReadBelowHighWatermarkAreGaps(): Unit = {
    for (data <- Seq(records(base = 1), MemoryRecords.EMPTY)) {
      val ctx = new Context(data)
      assertThrows(classOf[SourceLogGapException], () => ctx.read())
    }
  }

  @Test
  def testTruncatedFirstBatchAndBadCrcAreRejected(): Unit = {
    val partial = records().buffer().duplicate()
    partial.limit(partial.limit() - 1)
    val badCrc = records().buffer().duplicate()
    badCrc.put(badCrc.limit() - 1, (badCrc.get(badCrc.limit() - 1) ^ 1).toByte)
    for (buffer <- Seq(partial, badCrc)) {
      val ctx = new Context(MemoryRecords.readableRecords(buffer))
      assertThrows(classOf[CorruptRecordException], () => ctx.read())
    }
  }

  @Test
  def testMissingRecordsCannotHideBehindValidCrcAndBatchHeader(): Unit = {
    val builder = MemoryRecords.builder(ByteBuffer.allocate(1024), Compression.NONE, TimestampType.CREATE_TIME, 0L)
    builder.appendWithOffset(0L, new SimpleRecord(Array[Byte](1)))
    builder.appendWithOffset(2L, new SimpleRecord(Array[Byte](2)))
    val data = builder.build()
    // Make the count agree with the declared 0..2 span, but preserve the missing offset 1 in the record stream.
    val buffer = data.buffer()
    buffer.putInt(DefaultRecordBatch.RECORDS_COUNT_OFFSET, 3)
    val attributesOffset = DefaultRecordBatch.CRC_OFFSET + Integer.BYTES
    buffer.putInt(DefaultRecordBatch.CRC_OFFSET, Crc32C.compute(buffer, attributesOffset, buffer.limit() - attributesOffset).toInt)
    data.batches().iterator().next().ensureValid()
    val ctx = new Context(data)
    assertThrows(classOf[CorruptRecordException], () => ctx.read())
  }

  @Test
  def testLegacyRecordFormatIsNotAssignedAnAmbiguousBatchIdentity(): Unit = {
    val legacy = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 0L, Compression.NONE, TimestampType.CREATE_TIME,
      new SimpleRecord(Array[Byte](1)))
    val ctx = new Context(legacy, initialHw = 1)
    assertThrows(classOf[UnsupportedForMessageFormatException], () => ctx.read())
  }

  @Test
  def testLeaderOrPartitionReplacementDuringReadCannotReturnSuccess(): Unit = {
    val fenced = new Context
    fenced.onRead = () => fenced.fenced = true
    assertThrows(classOf[FencedLeaderEpochException], () => fenced.read())
    val replaced = new Context
    val otherPartition = mock(classOf[Partition])
    replaced.onRead = () => when(replaced.replicaManager.getPartitionOrException(topicIdPartition)).thenReturn(otherPartition)
    assertThrows(classOf[NotLeaderOrFollowerException], () => replaced.read())
    val moved = new Context
    val otherLog = mock(classOf[UnifiedLog])
    moved.onRead = () => when(moved.partition.localLogWithEpochOrThrow(any(), anyBoolean())).thenReturn(otherLog)
    assertThrows(classOf[NotLeaderOrFollowerException], () => moved.read())
  }

  @Test
  def testRetentionOrHighWatermarkRegressionDuringReadIsRejected(): Unit = {
    val deleted = new Context
    deleted.onRead = () => deleted.start = 1
    assertThrows(classOf[SourceLogGapException], () => deleted.read())
    val regressed = new Context
    regressed.onRead = () => regressed.hw = 2
    assertThrows(classOf[SourceLogGapException], () => regressed.read())
  }

  @Test
  def testShortFileReadIsCorruptionUnlessSourceWasFenced(): Unit = {
    for (fence <- Seq(false, true)) {
      val file = mock(classOf[FileRecords])
      when(file.sizeInBytes()).thenReturn(100)
      val ctx = new Context(file)
      doAnswer { call =>
        val buffer = call.getArgument[ByteBuffer](0)
        buffer.put(1.toByte).flip()
        ctx.fenced = fence
        null
      }.when(file).readInto(any[ByteBuffer](), anyInt())
      if (fence) assertThrows(classOf[FencedLeaderEpochException], () => ctx.read())
      else assertThrows(classOf[CorruptRecordException], () => ctx.read())
      verify(file, never()).close()
    }
  }

  @Test
  def testDisabledTopicCannotUseTheReader(): Unit = {
    val ctx = new Context
    val config = new LogConfig(JMap.of(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "false"))
    when(ctx.log.config()).thenReturn(config)
    assertThrows(classOf[InvalidRequestException], () => ctx.read())
    assertEquals(0, ctx.reads)
  }
}
