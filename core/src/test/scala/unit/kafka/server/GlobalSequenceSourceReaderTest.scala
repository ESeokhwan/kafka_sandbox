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

import kafka.cluster.AbstractPartitionTest
import kafka.server.GlobalSequenceSourceReader.{AWAIT_HIGH_WATERMARK, BATCH, CONTINUE, SourceLogGapException}
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.{FencedLeaderEpochException, InvalidRequestException, NotLeaderOrFollowerException, UnknownLeaderEpochException, UnknownTopicIdException}
import org.apache.kafka.common.record.{CompressionType, ControlRecordType, EndTransactionMarker, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.LeaderAndIsrRequest
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{PartitionKey, PhysicalBatch}
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogStartOffsetIncrementReason}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.Mockito.{mock, when}

import java.util.{List => JList, Optional, Properties}

@Timeout(30)
class GlobalSequenceSourceReaderTest extends AbstractPartitionTest {
  private val epoch = 3
  private var replicaManager: ReplicaManager = _
  private var reader: GlobalSequenceSourceReader = _
  private def key: PartitionKey = new PartitionKey(topicId.get, topicPartition.partition())

  override def createLogProperties(overrides: Map[String, String]): Properties = super.createLogProperties(overrides ++ Map(
    TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG -> "true", TopicConfig.CLEANUP_POLICY_CONFIG -> "delete"))

  private def state(leader: Int, leaderEpoch: Int): LeaderAndIsrRequest.PartitionState =
    new LeaderAndIsrRequest.PartitionState().setControllerEpoch(0).setLeader(leader).setLeaderEpoch(leaderEpoch)
      .setPartitionEpoch(leaderEpoch).setReplicas(JList.of(Int.box(brokerId), Int.box(remoteReplicaId)))
      .setIsr(JList.of(Int.box(brokerId), Int.box(remoteReplicaId))).setIsNew(true)

  @BeforeEach
  def initializeReader(): Unit = {
    partition.createLogIfNotExists(isNew = true, isFutureReplica = false, offsetCheckpoints, topicId)
    partition.makeLeader(state(brokerId, epoch), offsetCheckpoints, topicId)
    replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.metadataCache).thenReturn(metadataCache)
    when(metadataCache.getTopicName(topicId.get)).thenReturn(Optional.of(topicPartition.topic()))
    when(replicaManager.getPartitionOrException(new TopicIdPartition(topicId.get, topicPartition))).thenReturn(partition)
    reader = new GlobalSequenceSourceReader(replicaManager, time)
  }

  private def log = partition.localLogOrException
  private def commit(): Unit = { log.updateHighWatermark(log.logEndOffset) }
  private def read(offset: Long, maxBytes: Int = 1024): GlobalSequenceSourceReader.ReadResult = reader.read(key, epoch, offset, maxBytes)
  private def append(count: Int = 1, compression: Compression = Compression.NONE): Long = {
    val records = MemoryRecords.withRecords(compression, (0 until count).map(_ => new SimpleRecord(time.milliseconds(), Array[Byte](1, 2, 3))): _*)
    log.appendAsLeader(records, epoch).firstOffset()
  }
  private def marker(kind: ControlRecordType): Int = {
    val records = MemoryRecords.withEndTransactionMarker(log.logEndOffset + 100, 0.toShort, new EndTransactionMarker(kind, 0))
    log.appendAsLeader(records, epoch, AppendOrigin.COORDINATOR)
    records.sizeInBytes()
  }

  @ParameterizedTest
  @EnumSource(classOf[CompressionType])
  def testCompleteCompressedBatchBelowCapturedHighWatermark(compressionType: CompressionType): Unit = {
    append(3, Compression.of(compressionType).build())
    assertEquals(0L, log.highWatermark)
    assertEquals(AWAIT_HIGH_WATERMARK, read(0).status)
    log.updateHighWatermark(2L)
    val partial = read(0, maxBytes = 1)
    assertEquals(AWAIT_HIGH_WATERMARK, partial.status)
    assertEquals(2L, partial.dataHighWatermark)
    assertEquals(2L, log.highWatermark, "Reading must not normalize or mutate the source HW")
    assertEquals(0L, partial.nextPhysicalOffset)
    assertTrue(partial.batch.isEmpty)
    commit()
    val complete = read(0, maxBytes = 1)
    assertEquals(BATCH, complete.status)
    assertEquals(Some(new PhysicalBatch(key, 0, 2, 3)), complete.batch)
    assertEquals(3L, complete.nextPhysicalOffset)
    assertEquals(3L, complete.dataHighWatermark)
    assertEquals(epoch, complete.sourceLeaderEpoch)
    assertEquals(AWAIT_HIGH_WATERMARK, read(3).status)
    // The reader must not close the FileRecords slice borrowed from the log.
    assertEquals(3L, append())
  }

  @Test
  def testOneTargetBatchPerReadAndControlBatchesNeverGetIndexed(): Unit = {
    marker(ControlRecordType.COMMIT) // 0
    append(2) // 1..2
    marker(ControlRecordType.ABORT) // 3
    append(1) // 4
    marker(ControlRecordType.COMMIT) // 5
    commit()
    val first = read(0)
    assertEquals(Some(new PhysicalBatch(key, 1, 2, 2)), first.batch)
    assertEquals(3L, first.nextPhysicalOffset)
    val second = read(first.nextPhysicalOffset)
    assertEquals(Some(new PhysicalBatch(key, 4, 4, 1)), second.batch)
    val end = read(second.nextPhysicalOffset)
    assertEquals(AWAIT_HIGH_WATERMARK, end.status)
    assertEquals(6L, end.nextPhysicalOffset)
    assertTrue(end.batch.isEmpty)
  }

  @Test
  def testControlOnlyReadBudgetAndIncompleteTrailingBatch(): Unit = {
    val controlSize = marker(ControlRecordType.COMMIT)
    marker(ControlRecordType.ABORT)
    append(3)
    commit()
    // The second batch is cut by the byte limit. It must be reread from its base, not skipped.
    val first = read(0, controlSize + 10)
    assertEquals(CONTINUE, first.status)
    assertEquals(1L, first.nextPhysicalOffset)
    assertTrue(first.batch.isEmpty)
    val second = read(1, maxBytes = 1)
    assertEquals(CONTINUE, second.status)
    assertEquals(2L, second.nextPhysicalOffset)
    val third = read(2, maxBytes = 1)
    assertEquals(Some(new PhysicalBatch(key, 2, 4, 3)), third.batch)
  }

  @Test
  def testControlBatchAtHighWatermarkDoesNotAdvanceCursor(): Unit = {
    marker(ControlRecordType.COMMIT)
    marker(ControlRecordType.ABORT)
    log.updateHighWatermark(1L)
    val first = read(0)
    assertEquals(AWAIT_HIGH_WATERMARK, first.status)
    assertEquals(1L, first.nextPhysicalOffset)
    assertEquals(1L, read(1).nextPhysicalOffset)
    commit()
    assertEquals(2L, read(1).nextPhysicalOffset)
  }

  @Test
  def testOpenAndAbortedTransactionDataAreStillIndexTargets(): Unit = {
    val producerId = 17L
    val records = MemoryRecords.withTransactionalRecords(Compression.gzip().build(), producerId, 0.toShort, 0,
      new SimpleRecord(time.milliseconds(), Array[Byte](1)), new SimpleRecord(time.milliseconds(), Array[Byte](2)))
    val guard = log.maybeStartTransactionVerification(producerId, 0, 0.toShort, false)
    log.appendAsLeader(records, epoch, AppendOrigin.CLIENT, RequestLocal.noCaching(), guard)
    commit()
    assertEquals(0L, log.lastStableOffset)
    assertEquals(Some(new PhysicalBatch(key, 0, 1, 2)), read(0).batch)
    log.appendAsLeader(MemoryRecords.withEndTransactionMarker(producerId, 0.toShort, new EndTransactionMarker(ControlRecordType.ABORT, 0)),
      epoch, AppendOrigin.COORDINATOR)
    commit()
    assertEquals(Some(new PhysicalBatch(key, 0, 1, 2)), read(0).batch)
    assertEquals(3L, read(2).nextPhysicalOffset)
    assertEquals(AWAIT_HIGH_WATERMARK, read(2).status)
  }

  @Test
  def testSegmentBoundaryContinuesWithoutInventingProgress(): Unit = {
    marker(ControlRecordType.COMMIT)
    log.roll()
    append(2)
    commit()
    val first = read(0)
    assertEquals(CONTINUE, first.status)
    assertEquals(1L, first.nextPhysicalOffset)
    assertEquals(Some(new PhysicalBatch(key, 1, 2, 2)), read(first.nextPhysicalOffset).batch)
  }

  @Test
  def testMissingPrefixAndCursorBeyondLogEndAreExplicitGaps(): Unit = {
    append(2)
    append()
    commit()
    // A stale reader can encounter a prefix already indexed and legitimately expired.
    log.updateGlobalSequenceIndexedOffset(topicId.get, 2L)
    log.maybeIncrementLogStartOffset(2L, LogStartOffsetIncrementReason.ClientRecordDeletion)
    val gap = assertThrows(classOf[SourceLogGapException], () => read(0))
    assertEquals(key, gap.partition)
    assertEquals(0L, gap.requestedOffset)
    assertEquals(2L, gap.logStartOffset)
    assertEquals(3L, gap.logEndOffset)
    assertThrows(classOf[SourceLogGapException], () => read(4))
    assertEquals(Some(new PhysicalBatch(key, 2, 2, 1)), read(2).batch)
  }

  @Test
  def testCursorInMiddleOfBatchIsRejected(): Unit = {
    append(3)
    commit()
    assertThrows(classOf[InvalidRequestException], () => read(1))
  }

  @Test
  def testLeaderEpochAndLocalLeadershipAreRequired(): Unit = {
    append(2)
    commit()
    assertThrows(classOf[FencedLeaderEpochException], () => reader.read(key, epoch - 1, 0, 1024))
    assertThrows(classOf[UnknownLeaderEpochException], () => reader.read(key, epoch + 1, 0, 1024))
    partition.makeFollower(state(remoteReplicaId, epoch + 1), offsetCheckpoints, topicId)
    assertThrows(classOf[NotLeaderOrFollowerException], () => reader.read(key, epoch + 1, 0, 1024))
    partition.makeLeader(state(brokerId, epoch + 2), offsetCheckpoints, topicId)
    assertThrows(classOf[FencedLeaderEpochException], () => read(0))
    // Stored batch leader epochs do not form part of the physical batch identity.
    assertEquals(Some(new PhysicalBatch(key, 0, 1, 2)), reader.read(key, epoch + 2, 0, 1024).batch)
  }

  @Test
  def testTopicIdMustMatchTheActualLog(): Unit = {
    val otherId = Uuid.randomUuid()
    when(metadataCache.getTopicName(otherId)).thenReturn(Optional.of(topicPartition.topic()))
    when(replicaManager.getPartitionOrException(new TopicIdPartition(otherId, topicPartition))).thenReturn(partition)
    assertThrows(classOf[UnknownTopicIdException], () => reader.read(new PartitionKey(otherId, 0), epoch, 0, 1024))
    when(metadataCache.getTopicName(topicId.get)).thenReturn(Optional.empty())
    assertThrows(classOf[UnknownTopicIdException], () => read(0))
  }

  @Test
  def testInvalidReadLimitsAreRejected(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => read(-1))
    assertThrows(classOf[IllegalArgumentException], () => read(0, maxBytes = 0))
    assertThrows(classOf[IllegalArgumentException], () => reader.read(key, -1, 0, 1))
  }
}
