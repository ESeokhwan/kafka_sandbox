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

import kafka.log.LogTestUtils
import kafka.utils.TestUtils
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.{OffsetNotAvailableException, OffsetOutOfRangeException, PolicyViolationException}
import org.apache.kafka.common.record.{ControlRecordType, EndTransactionMarker, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig, LogStartOffsetIncrementReason, UnifiedLog}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Optional
import scala.jdk.CollectionConverters._

class GlobalSequenceRetentionLogTest {
  private class Context(enabled: Boolean = true, overrides: Map[String, String] = Map.empty, remote: Boolean = false) extends AutoCloseable {
    val time = new MockTime()
    val id: Uuid = Uuid.randomUuid()
    val root = TestUtils.tempDir()
    val dir = TestUtils.randomPartitionLogDir(root)
    val stats = new BrokerTopicStats
    val config = new LogConfig((Map(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG -> enabled.toString,
      TopicConfig.CLEANUP_POLICY_CONFIG -> "delete", TopicConfig.RETENTION_MS_CONFIG -> "10",
      TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG -> remote.toString) ++ overrides).asJava)
    var log: UnifiedLog = open(0)
    def open(start: Long): UnifiedLog = LogTestUtils.createLog(dir, config, stats, time.scheduler, time,
      logStartOffset = start, topicId = Some(id), remoteStorageSystemEnable = remote)
    def append(count: Int = 2): Unit = {
      log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE,
        (0 until count).map(_ => new SimpleRecord(time.milliseconds(), Array[Byte](1))): _*), 3)
      log.updateHighWatermark(log.logEndOffset())
    }
    def twoSegments(): Unit = { append(); log.roll(); append(); log.roll() }
    override def close(): Unit = { log.close(); stats.close(); time.scheduler.clear(); Utils.delete(root) }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("retention.ms", "retention.bytes"))
  def testRetentionCannotRemoveUnindexedBatches(policy: String): Unit = {
    val c = new Context(overrides = Map(policy -> "1"))
    try {
      c.twoSegments()
      c.time.sleep(20)
      assertEquals(0L, c.log.globalSequenceDeletionLimit())
      assertEquals(0, c.log.deleteOldSegments())
      assertEquals(0L, c.log.logStartOffset())
      c.log.updateGlobalSequenceIndexedOffset(c.id, 2)
      assertEquals(1, c.log.deleteOldSegments())
      assertEquals(2L, c.log.logStartOffset())
      assertEquals(0, c.log.deleteOldSegments())
      c.log.updateGlobalSequenceIndexedOffset(c.id, 4)
      assertEquals(1, c.log.deleteOldSegments())
      assertEquals(4L, c.log.logStartOffset())
    } finally c.close()
  }

  @Test
  def testDeleteRecordsRejectsBeyondCommittedPrefixWithoutPartiallyDeleting(): Unit = {
    val c = new Context
    try {
      c.twoSegments()
      assertThrows(classOf[PolicyViolationException], () => c.log.maybeIncrementLogStartOffset(2, LogStartOffsetIncrementReason.ClientRecordDeletion))
      assertEquals(0L, c.log.logStartOffset())
      c.log.updateGlobalSequenceIndexedOffset(c.id, 2)
      assertTrue(c.log.maybeIncrementLogStartOffset(2, LogStartOffsetIncrementReason.ClientRecordDeletion))
      assertThrows(classOf[PolicyViolationException], () => c.log.maybeIncrementLogStartOffset(3, LogStartOffsetIncrementReason.ClientRecordDeletion))
      assertEquals(2L, c.log.logStartOffset())
      assertThrows(classOf[OffsetOutOfRangeException], () => c.log.maybeIncrementLogStartOffset(5, LogStartOffsetIncrementReason.ClientRecordDeletion))
    } finally c.close()
  }

  @Test
  def testFollowerStartOffsetPropagationWaitsForItsOwnProgressRefresh(): Unit = {
    val c = new Context
    try {
      c.twoSegments()
      assertFalse(c.log.maybeIncrementLogStartOffset(4, LogStartOffsetIncrementReason.LeaderOffsetIncremented))
      c.log.updateGlobalSequenceIndexedOffset(c.id, 2)
      assertTrue(c.log.maybeIncrementLogStartOffset(4, LogStartOffsetIncrementReason.LeaderOffsetIncremented))
      assertEquals(2L, c.log.logStartOffset())
      c.log.updateGlobalSequenceIndexedOffset(c.id, 4)
      assertTrue(c.log.maybeIncrementLogStartOffset(4, LogStartOffsetIncrementReason.LeaderOffsetIncremented))
      assertEquals(4L, c.log.logStartOffset())
    } finally c.close()
  }

  @Test
  def testRestartPinsDeletionUntilProgressIsReconfirmed(): Unit = {
    val c = new Context
    try {
      c.twoSegments()
      c.log.updateGlobalSequenceIndexedOffset(c.id, 2)
      c.log.maybeIncrementLogStartOffset(2, LogStartOffsetIncrementReason.ClientRecordDeletion)
      c.log.close()
      c.log = c.open(2)
      c.log.updateHighWatermark(4L)
      c.time.sleep(20)
      assertEquals(0L, c.log.globalSequenceDeletionLimit())
      assertEquals(0, c.log.deleteOldSegments())
      assertEquals(2L, c.log.logStartOffset())
      c.log.updateGlobalSequenceIndexedOffset(c.id, 4)
      assertTrue(c.log.deleteOldSegments() > 0)
      assertEquals(4L, c.log.logStartOffset())
    } finally c.close()
  }

  @Test
  def testPinDoesNotRegressOrAcceptAnotherTopicIdentity(): Unit = {
    val c = new Context
    try {
      assertFalse(c.log.updateGlobalSequenceIndexedOffset(Uuid.randomUuid(), 100))
      assertEquals(0L, c.log.globalSequenceDeletionLimit())
      assertTrue(c.log.updateGlobalSequenceIndexedOffset(c.id, 4))
      assertTrue(c.log.updateGlobalSequenceIndexedOffset(c.id, 2))
      assertEquals(4L, c.log.globalSequenceDeletionLimit())
      assertThrows(classOf[IllegalArgumentException], () => c.log.updateGlobalSequenceIndexedOffset(c.id, -1))
      assertThrows(classOf[IllegalArgumentException], () => c.log.updateGlobalSequenceIndexedOffset(Uuid.ZERO_UUID, 0))
    } finally c.close()
  }

  @Test
  def testControlOnlySuffixRemainsAvailableForRecovery(): Unit = {
    val c = new Context
    try {
      c.append()
      c.log.roll()
      c.log.appendAsLeader(MemoryRecords.withEndTransactionMarker(c.time.milliseconds(), 123L, 0.toShort, new EndTransactionMarker(ControlRecordType.COMMIT, 0)),
        3, AppendOrigin.COORDINATOR)
      c.log.updateHighWatermark(3L)
      c.log.roll()
      c.log.updateGlobalSequenceIndexedOffset(c.id, 2)
      c.time.sleep(20)
      assertEquals(1, c.log.deleteOldSegments())
      assertEquals(2L, c.log.logStartOffset())
      assertEquals(0, c.log.deleteOldSegments())
      c.append(1)
      c.log.updateGlobalSequenceIndexedOffset(c.id, 4)
      c.time.sleep(20)
      assertTrue(c.log.deleteOldSegments() > 0)
      assertEquals(4L, c.log.logStartOffset())
    } finally c.close()
  }

  @Test
  def testLaggingReplicaCannotJumpOverUnknownIndexProgress(): Unit = {
    val c = new Context
    try {
      c.append()
      assertThrows(classOf[OffsetNotAvailableException], () => c.log.truncateFullyAndStartAt(10, Optional.empty[java.lang.Long]()))
      assertThrows(classOf[OffsetNotAvailableException], () => c.log.truncateFullyAndStartAt(0, Optional.of(Long.box(10))))
      assertEquals(0L, c.log.logStartOffset())
      assertEquals(2L, c.log.logEndOffset())
      c.log.updateGlobalSequenceIndexedOffset(c.id, 10)
      c.log.truncateFullyAndStartAt(10, Optional.empty[java.lang.Long]())
      assertEquals(10L, c.log.logStartOffset())
      assertEquals(10L, c.log.logEndOffset())
    } finally c.close()
  }

  @Test
  def testTieredLocalDeletionAndLocalStartOffsetRespectThePin(): Unit = {
    val c = new Context(remote = true, overrides = Map(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG -> "1"))
    try {
      c.twoSegments()
      c.log.updateHighestOffsetInRemoteStorage(3)
      c.time.sleep(20)
      assertEquals(0, c.log.deleteOldSegments())
      c.log.maybeIncrementLocalLogStartOffset(4, LogStartOffsetIncrementReason.LeaderOffsetIncremented)
      assertEquals(0L, c.log.localLogStartOffset())
      c.log.updateGlobalSequenceIndexedOffset(c.id, 2)
      assertEquals(1, c.log.deleteOldSegments())
      assertEquals(2L, c.log.localLogStartOffset())
      c.log.updateLogStartOffsetFromRemoteTier(4)
      assertEquals(2L, c.log.logStartOffset())
    } finally c.close()
  }

  @Test
  def testOrdinaryTopicsRetainExistingDeletionBehavior(): Unit = {
    val c = new Context(enabled = false)
    try {
      c.twoSegments()
      assertEquals(Long.MaxValue, c.log.globalSequenceDeletionLimit())
      c.time.sleep(20)
      assertEquals(2, c.log.deleteOldSegments())
      assertEquals(4L, c.log.logStartOffset())
      c.append(2)
      assertTrue(c.log.maybeIncrementLogStartOffset(6, LogStartOffsetIncrementReason.ClientRecordDeletion))
    } finally c.close()
  }
}
