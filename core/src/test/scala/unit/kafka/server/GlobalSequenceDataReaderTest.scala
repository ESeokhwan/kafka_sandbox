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
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.{CorruptRecordException, NotLeaderOrFollowerException, OffsetNotAvailableException, OffsetOutOfRangeException, RecordTooLargeException, TimeoutException, UnknownTopicIdException}
import org.apache.kafka.test.TestUtils.assertFutureThrows
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
class GlobalSequenceDataReaderTest extends AbstractPartitionTest {
  private val epoch = 3
  private var replicaManager: ReplicaManager = _
  private var reader: GlobalSequenceDataReader = _
  private val workers = new GlobalSequenceTestExecutor
  private def key: PartitionKey = new PartitionKey(topicId.get, topicPartition.partition())

  override def createLogProperties(overrides: Map[String, String]): Properties = super.createLogProperties(overrides ++ Map(
    TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG -> "true", TopicConfig.CLEANUP_POLICY_CONFIG -> "delete",
    TopicConfig.MAX_MESSAGE_BYTES_CONFIG -> (GlobalSequenceFetch.MaxBatchBytes + 1024).toString,
    org.apache.kafka.storage.internals.log.LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG -> (GlobalSequenceFetch.MaxBatchBytes * 2).toString))

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
    reader = new GlobalSequenceDataReader(replicaManager, time.scheduler, time, workers)
  }

  private def log = partition.localLogOrException
  private def commit(): Unit = { log.updateHighWatermark(log.logEndOffset) }
  private def read(base: Long = 0, last: Long = 2, leaderEpoch: Int = epoch): java.util.concurrent.CompletableFuture[GlobalSequenceFetch.Data] =
    reader.read(new PhysicalBatch(key, base, last, (last - base + 1).toInt), leaderEpoch, time.nanoseconds() + 100000000L)
  private def finish(future: java.util.concurrent.CompletableFuture[GlobalSequenceFetch.Data]): GlobalSequenceFetch.Data = {
    workers.runAll()
    future.join()
  }
  @org.junit.jupiter.api.AfterEach
  def closeReader(): Unit = if (reader != null) reader.close()
  private def append(count: Int = 1, compression: Compression = Compression.NONE): Long = {
    val records = MemoryRecords.withRecords(compression, (0 until count).map(_ => new SimpleRecord(time.milliseconds(), Array[Byte](1, 2, 3))): _*)
    log.appendAsLeader(records, epoch).firstOffset()
  }
  @ParameterizedTest
  @EnumSource(classOf[CompressionType])
  def testReturnsOneOriginalBatchWithoutRewritingOffsetsOrCrc(compressionType: CompressionType): Unit = {
    append(1)
    append(3, Compression.of(compressionType).build())
    append(1)
    commit()
    val result = finish(read(1, 3))
    val batch = result.records.batches().iterator().next()
    batch.ensureValid()
    assertEquals(1L, batch.baseOffset())
    assertEquals(3L, batch.lastOffset())
    assertEquals(compressionType, batch.compressionType())
    assertEquals(5L, result.dataHighWatermark)
    assertEquals(epoch, result.sourceLeaderEpoch)
    // Compare directly with the persisted log bytes, including the leader epoch and CRC.
    val expected = java.nio.ByteBuffer.allocate(result.records.sizeInBytes())
    val stored = log.activeSegment().log().slice(0, log.activeSegment().size())
    val all = java.nio.ByteBuffer.allocate(stored.sizeInBytes())
    stored.readInto(all, 0)
    val second = MemoryRecords.readableRecords(all).batches().iterator()
    second.next()
    second.next().writeTo(expected)
    expected.flip()
    assertEquals(expected, result.records.buffer())
    assertEquals(5L, append(), "The borrowed file slice must stay open")
  }

  @Test
  def testEntireBatchMustBeBelowRawHighWatermark(): Unit = {
    append(3)
    log.updateHighWatermark(2L)
    val pending = read()
    workers.runAll()
    assertFutureThrows(classOf[OffsetNotAvailableException], pending)
    assertEquals(2L, log.highWatermark)
    commit()
    assertEquals(3L, finish(read()).dataHighWatermark)
  }

  @Test
  def testOpenAndAbortedTransactionsRemainReadableAtReadUncommitted(): Unit = {
    val producerId = 17L
    val records = MemoryRecords.withTransactionalRecords(Compression.gzip().build(), producerId, 0.toShort, 0,
      new SimpleRecord(time.milliseconds(), Array[Byte](1)), new SimpleRecord(time.milliseconds(), Array[Byte](2)))
    val guard = log.maybeStartTransactionVerification(producerId, 0, 0.toShort, false)
    log.appendAsLeader(records, epoch, AppendOrigin.CLIENT, RequestLocal.noCaching(), guard)
    commit()
    assertEquals(0L, log.lastStableOffset)
    val open = finish(read(0, 1)).records
    assertTrue(open.batches().iterator().next().isTransactional)
    log.appendAsLeader(MemoryRecords.withEndTransactionMarker(producerId, 0.toShort, new EndTransactionMarker(ControlRecordType.ABORT, 0)),
      epoch, AppendOrigin.COORDINATOR)
    commit()
    assertEquals(open.buffer(), finish(read(0, 1)).records.buffer())
    val control = read(2, 2)
    workers.runAll()
    assertFutureThrows(classOf[CorruptRecordException], control)
  }

  @Test
  def testMissingHistoryAndWrongMappingNeverSkipAhead(): Unit = {
    append(3)
    commit()
    val wrong = read(1, 2)
    workers.runAll()
    assertFutureThrows(classOf[CorruptRecordException], wrong)
    log.updateGlobalSequenceIndexedOffset(topicId.get, 3L)
    log.maybeIncrementLogStartOffset(3, LogStartOffsetIncrementReason.ClientRecordDeletion)
    val missing = read()
    workers.runAll()
    assertFutureThrows(classOf[OffsetOutOfRangeException], missing)
  }

  @Test
  def testFencingUuidTimeoutAndCancellation(): Unit = {
    append(3)
    commit()
    val stale = read(leaderEpoch = epoch - 1)
    workers.runAll()
    assertFutureThrows(classOf[NotLeaderOrFollowerException], stale)
    val timed = read()
    time.sleep(100)
    assertFutureThrows(classOf[TimeoutException], timed)
    val cancelled = read()
    cancelled.cancel(false)
    workers.runAll()
    assertTrue(cancelled.isCancelled)
    when(metadataCache.getTopicName(topicId.get)).thenReturn(Optional.empty())
    val deleted = read()
    workers.runAll()
    assertFutureThrows(classOf[UnknownTopicIdException], deleted)
  }

  @Test
  def testPartitionReplacementAfterCopyRejectsTheOldPayload(): Unit = {
    append(3)
    commit()
    val replacement = mock(classOf[kafka.cluster.Partition])
    when(replicaManager.getPartitionOrException(new TopicIdPartition(topicId.get, topicPartition)))
      .thenReturn(partition, partition, replacement)
    val stale = read()
    workers.runAll()
    assertFutureThrows(classOf[NotLeaderOrFollowerException], stale)
  }

  @Test
  def testClosingReaderReleasesQueuedReads(): Unit = {
    val queued = read()
    reader.close()
    assertFutureThrows(classOf[org.apache.kafka.common.errors.CoordinatorNotAvailableException], queued)
    assertFutureThrows(classOf[org.apache.kafka.common.errors.CoordinatorNotAvailableException], read())
  }

  @Test
  def testOversizedPhysicalBatchFailsBeforeResponseAllocation(): Unit = {
    log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord(time.milliseconds(), new Array[Byte](GlobalSequenceFetch.MaxBatchBytes))), epoch)
    commit()
    val future = read(0, 0)
    workers.runAll()
    assertFutureThrows(classOf[RecordTooLargeException], future)
  }
}
