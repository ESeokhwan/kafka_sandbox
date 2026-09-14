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

import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, NewPartitionReassignment, NewTopic, RecordsToDelete}
import org.apache.kafka.common.{ElectionType, TopicPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.test.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorConfig
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PartitionKey
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.storage.internals.log.AppendOrigin
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}

import java.util
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._
import scala.util.Try

@Timeout(120)
class GlobalSequenceRetentionIntegrationTest {
  private class Context extends AutoCloseable {
    val cluster = new KafkaClusterTestKit.Builder(new TestKitNodes.Builder()
      .setNumBrokerNodes(3).setNumControllerNodes(1).build())
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_NUM_PARTITIONS_CONFIG, "2")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_REPLICATION_FACTOR_CONFIG, "3")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_MIN_ISR_CONFIG, "2")
      .setConfigProp(GlobalSequenceCoordinatorConfig.RETENTION_REFRESH_INTERVAL_MS_CONFIG, "100")
      .setConfigProp("log.retention.check.interval.ms", "100")
      .build()
    var admin: Admin = _
    def brokers: Seq[BrokerServer] = cluster.brokers().values().asScala.toSeq.sortBy(_.config.brokerId)
    def start(): Unit = {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      admin = Admin.create(cluster.clientProperties())
    }
    def create(tp: TopicPartition, replicas: Seq[BrokerServer]): PartitionKey = {
      admin.createTopics(util.List.of(new NewTopic(tp.topic(), util.Map.of(Int.box(0), replicas.map(b => Int.box(b.config.brokerId)).asJava))
        .configs(util.Map.of(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "true", TopicConfig.CLEANUP_POLICY_CONFIG, "delete",
          TopicConfig.RETENTION_MS_CONFIG, "0")))).all().get(30, TimeUnit.SECONDS)
      TestUtils.waitUntilTrue(() => brokers.forall(_.metadataCache.contains(tp)), "Source metadata did not arrive", 30000)
      val id = admin.describeTopics(util.List.of(tp.topic())).allTopicNames().get(30, TimeUnit.SECONDS).get(tp.topic()).topicId()
      new PartitionKey(id, 0)
    }
    def append(source: BrokerServer, tp: TopicPartition, count: Int): Long = {
      val records = MemoryRecords.withRecords(Compression.NONE,
        (0 until count).map(_ => new SimpleRecord(System.currentTimeMillis(), Array[Byte](1))): _*)
      source.replicaManager.onlinePartition(tp).get.appendRecordsToLeader(records, AppendOrigin.CLIENT, -1, RequestLocal.noCaching).lastOffset()
    }
    def progress(key: PartitionKey, last: Long): Unit = TestUtils.waitUntilTrue(() =>
      Try(brokers.head.indexRoutingManager.describePartition(key, 5000).get(6, TimeUnit.SECONDS)).toOption.exists(r =>
        r.value.committedProgress().isPresent && r.value.committedProgress().get().lastOffset() == last),
      s"Index progress did not reach $last for $key", 30000)
    def pins(tp: TopicPartition, expected: Long, replicas: Seq[BrokerServer]): Unit = TestUtils.waitUntilTrue(() =>
      replicas.forall(_.replicaManager.localLog(tp).exists(_.globalSequenceDeletionLimit() == expected)),
      s"Replica deletion pins did not reach $expected", 30000)
    def reassign(tp: TopicPartition, replicas: Seq[BrokerServer]): Unit = {
      admin.alterPartitionReassignments(util.Map.of(tp, util.Optional.of(new NewPartitionReassignment(
        replicas.map(b => Int.box(b.config.brokerId)).asJava)))).all().get(30, TimeUnit.SECONDS)
      TestUtils.waitUntilTrue(() => admin.listPartitionReassignments().reassignments().get(10, TimeUnit.SECONDS).isEmpty,
        "Source reassignment did not finish", 30000)
    }
    def elect(tp: TopicPartition, id: Uuid, target: BrokerServer): Unit = {
      // A recovered local HW does not mean the controller has already added the replica back to ISR.
      TestUtils.waitUntilTrue(() => brokers.forall(_.metadataCache.getImage().topics().getTopic(id)
        .partitions().get(0).isr.contains(target.config.brokerId)), "Restarted replica did not rejoin ISR", 30000)
      admin.electLeaders(ElectionType.PREFERRED, util.Set.of(tp)).all().get(30, TimeUnit.SECONDS)
      TestUtils.waitUntilTrue(() => brokers.forall(_.metadataCache.getImage().topics().getTopic(id)
        .partitions().get(0).leader == target.config.brokerId), "Source leader did not move", 30000)
    }
    override def close(): Unit = { if (admin != null) admin.close(); cluster.close() }
  }

  @Test
  def testRetentionAndDeleteRecordsPreserveUnindexedDataAcrossRestartAndFollowerPromotion(): Unit = {
    val c = new Context
    try {
      c.start()
      val tp = new TopicPartition("retained", 0)
      val key = c.create(tp, c.brokers)
      val source = c.brokers.head
      assertEquals(1L, c.append(source, tp, 2))
      c.progress(key, 1)
      c.pins(tp, 2, c.brokers)
      source.globalSequenceIndexerManager.indexer(key).get.close()
      // Expire the first batch before appending the unindexed batch so they occupy distinct segments.
      TestUtils.waitUntilTrue(() => {
        c.brokers.foreach(_.replicaManager.localLog(tp).get.deleteOldSegments())
        c.brokers.forall(_.replicaManager.localLog(tp).get.logStartOffset() == 2)
      }, "Indexed prefix did not expire", 30000)
      assertEquals(3L, c.append(source, tp, 2))
      TestUtils.waitUntilTrue(() => c.brokers.forall(_.replicaManager.localLog(tp).exists(_.highWatermark() >= 4)),
        "Source data did not replicate", 30000)
      c.brokers.foreach { broker =>
        val log = broker.replicaManager.localLog(tp).get
        assertEquals(2L, log.globalSequenceDeletionLimit())
        assertEquals(4L, log.logEndOffset())
        assertEquals(0, log.deleteOldSegments(), "An unindexed batch must remain even though retention.ms=0")
      }
      assertFutureThrows(classOf[PolicyViolationException],
        c.admin.deleteRecords(util.Map.of(tp, RecordsToDelete.beforeOffset(4))).all())

      val promoted = c.brokers(2)
      promoted.shutdown()
      promoted.startup()
      c.cluster.waitForReadyBrokers()
      c.pins(tp, 2, c.brokers)
      TestUtils.waitUntilTrue(() => promoted.replicaManager.localLog(tp).exists(_.highWatermark() >= 4),
        "Restarted follower did not recover its source HW", 30000)
      assertEquals(2L, promoted.replicaManager.localLog(tp).get.logStartOffset())
      assertEquals(4L, promoted.replicaManager.localLog(tp).get.logEndOffset())
      c.reassign(tp, Seq(promoted) ++ c.brokers.filterNot(_ == promoted))
      c.elect(tp, key.topicId(), promoted)
      c.progress(key, 3)
      c.pins(tp, 4, c.brokers)
      c.admin.deleteRecords(util.Map.of(tp, RecordsToDelete.beforeOffset(4))).all().get(30, TimeUnit.SECONDS)
      TestUtils.waitUntilTrue(() => c.brokers.forall(_.replicaManager.localLog(tp).get.logStartOffset() == 4),
        "Committed index prefix did not release the replica deletion pins", 30000)
    } finally c.close()
  }

  @Test
  def testNewFollowerCanCatchUpPastExpiredIndexedPrefix(): Unit = {
    val c = new Context
    try {
      c.start()
      val tp = new TopicPartition("catchup", 0)
      val source = c.brokers.head
      val follower = c.brokers(1)
      val key = c.create(tp, Seq(source))
      assertEquals(1L, c.append(source, tp, 2))
      c.progress(key, 1)
      c.pins(tp, 2, Seq(source))
      TestUtils.waitUntilTrue(() => {
        source.replicaManager.localLog(tp).get.deleteOldSegments()
        source.replicaManager.localLog(tp).get.logStartOffset() == 2
      }, "Indexed source prefix did not expire", 30000)
      c.reassign(tp, Seq(source, follower))
      c.pins(tp, 2, Seq(source, follower))
      TestUtils.waitUntilTrue(() => follower.replicaManager.localLog(tp).exists(log => log.logStartOffset() == 2 && log.logEndOffset() == 2),
        "New follower could not advance past the confirmed indexed prefix", 30000)
      assertEquals(2L, c.append(source, tp, 1))
      c.progress(key, 2)
      c.pins(tp, 3, Seq(source, follower))
      assertEquals(3L, follower.replicaManager.localLog(tp).get.logEndOffset())
    } finally c.close()
  }
}
