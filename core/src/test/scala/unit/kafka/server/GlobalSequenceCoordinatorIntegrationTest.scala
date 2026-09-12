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
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, ConfigEntry, NewPartitionReassignment, NewPartitions, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{ElectionType, TopicPartition, Uuid}
import org.apache.kafka.common.message.{AppendGlobalSequenceIndexRequestData, DescribeGlobalSequencePartitionRequestData, RegisterGlobalSequenceIndexerRequestData, RegisterGlobalSequenceIndexerResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{CoordinatorNotAvailableException, InvalidConfigurationException, InvalidPartitionsException}
import org.apache.kafka.common.internals.Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME
import org.apache.kafka.common.test.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorConfig
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{AppendRequest, AppendStatus, PartitionKey, PhysicalBatch, RegistrationRequest}
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotSame, assertTrue}
import org.junit.jupiter.api.{Test, Timeout}

import java.util
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._
import scala.util.Try

@Timeout(120)
class GlobalSequenceCoordinatorIntegrationTest {
  @Test
  def testInternalTopicCreationLoadingAndBrokerRestart(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(new TestKitNodes.Builder()
      .setNumBrokerNodes(1).setNumControllerNodes(1).build())
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_NUM_PARTITIONS_CONFIG, "2")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_MIN_ISR_CONFIG, "1")
      .setConfigProp("auto.create.topics.enable", "false")
      .build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      val broker = cluster.brokers().values().iterator().next()
      val admin = Admin.create(cluster.clientProperties())
      try {
        assertFalse(admin.listTopics().names().get(30, TimeUnit.SECONDS).contains(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME))
        admin.createTopics(util.List.of(new NewTopic("ordered", 1, 1.toShort).configs(util.Map.of(
          TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "true", TopicConfig.CLEANUP_POLICY_CONFIG, "delete"))))
          .all().get(30, TimeUnit.SECONDS)
        TestUtils.waitUntilTrue(() => broker.metadataCache.numPartitions(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME).isPresent,
          "Global sequence index topic was not created", 30000)
        TestUtils.retry(30000) {
          val index = admin.describeTopics(util.List.of(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)).allTopicNames().get(10, TimeUnit.SECONDS)
            .get(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)
          assertTrue(index.isInternal)
          assertEquals(2, index.partitions().size())
          assertTrue(index.partitions().asScala.forall(_.replicas().size() == 1))
        }
        val resource = new ConfigResource(ConfigResource.Type.TOPIC, GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)
        val configs = admin.describeConfigs(util.List.of(resource)).all().get(30, TimeUnit.SECONDS).get(resource)
        GlobalSequenceCoordinatorConfig.REQUIRED_INDEX_TOPIC_CONFIGS.forEach { (key, value) =>
          assertEquals(value, configs.get(key).value())
        }
        assertFutureThrows(classOf[InvalidPartitionsException], admin.createPartitions(util.Map.of(
          GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, NewPartitions.increaseTo(3))).all())
        assertFutureThrows(classOf[InvalidConfigurationException], admin.incrementalAlterConfigs(util.Map.of(resource,
          util.List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "1000"), AlterConfigOp.OpType.SET)))).all())
        val topicId = admin.describeTopics(util.List.of("ordered")).allTopicNames().get(30, TimeUnit.SECONDS).get("ordered").topicId()
        val partition = new PartitionKey(topicId, 0)
        val oldService = broker.globalSequenceCoordinator
        val mappedPartition = oldService.partitionFor(topicId)
        TestUtils.waitUntilTrue(() => Try(oldService.committedProgress(partition).get(10, TimeUnit.SECONDS)).toOption.exists(_.isEmpty),
          "Global sequence shard did not finish loading", 30000)
        val props = cluster.clientProperties()
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
        props.put(ProducerConfig.ACKS_CONFIG, "all")
        val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
        val physicalOffset = try {
          producer.send(new ProducerRecord[Array[Byte], Array[Byte]]("ordered", 0, null, Array[Byte](1, 2, 3)))
            .get(30, TimeUnit.SECONDS).offset()
        } finally producer.close()
        val dataPartition = new TopicPartition("ordered", 0)
        val dataHw = broker.replicaManager.localLog(dataPartition).get.highWatermark
        assertTrue(dataHw > physicalOffset)
        val sourceEpoch = broker.metadataCache.getImage().topics().getTopic(topicId).partitions().get(0).leaderEpoch
        val indexEpoch = broker.metadataCache.getImage().topics().getTopic(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)
          .partitions().get(mappedPartition).leaderEpoch
        val registrationId = Uuid.randomUuid()
        val registration = new RegisterGlobalSequenceIndexerRequestData().setTopicId(topicId).setPartition(0)
          .setSourceBrokerId(broker.config.brokerId).setSourceLeaderEpoch(sourceEpoch).setExpectedGeneration(-1)
          .setRegistrationId(registrationId).setCoordinatorLeaderEpoch(indexEpoch)
        def register(data: RegisterGlobalSequenceIndexerRequestData): RegisterGlobalSequenceIndexerResponseData =
          IntegrationTestUtils.connectAndReceive[RegisterGlobalSequenceIndexerResponse](
            new RegisterGlobalSequenceIndexerRequest.Builder(data).build(), broker.socketServer, broker.config.interBrokerListenerName).data()
        val stale = register(registration.duplicate().setCoordinatorLeaderEpoch(indexEpoch + 1))
        assertEquals(Errors.NOT_COORDINATOR.code(), stale.errorCode())
        val registered = register(registration)
        assertEquals(Errors.NONE.code(), registered.errorCode())
        assertTrue(registered.registered())
        assertEquals(0L, registered.indexerGeneration())
        assertTrue(register(registration).registered())
        val append = new AppendGlobalSequenceIndexRequestData().setTopicId(topicId).setPartition(0)
          .setPhysicalBaseOffset(physicalOffset).setPhysicalLastOffset(physicalOffset).setRecordCount(1)
          .setPredecessorBaseOffset(-1).setDataHighWatermark(dataHw).setSourceBrokerId(broker.config.brokerId)
          .setSourceLeaderEpoch(sourceEpoch).setIndexerGeneration(0).setRegistrationId(registrationId).setCoordinatorLeaderEpoch(indexEpoch)
        def appendIndex(): AppendGlobalSequenceIndexResponse = IntegrationTestUtils.connectAndReceive[AppendGlobalSequenceIndexResponse](
          new AppendGlobalSequenceIndexRequest.Builder(append).build(), broker.socketServer, broker.config.interBrokerListenerName)
        val indexed = appendIndex().data()
        assertEquals(Errors.NONE.code(), indexed.errorCode())
        assertEquals(AppendStatus.INDEXED.code(), indexed.status())
        assertEquals(0L, indexed.globalBaseOffset())
        val duplicate = appendIndex().data()
        assertEquals(Errors.NONE.code(), duplicate.errorCode())
        assertEquals(AppendStatus.ALREADY_INDEXED.code(), duplicate.status())
        assertEquals(-1L, duplicate.globalBaseOffset())
        assertEquals(physicalOffset, duplicate.indexedThroughLastOffset())
        def describe(): DescribeGlobalSequencePartitionResponse = IntegrationTestUtils.connectAndReceive[DescribeGlobalSequencePartitionResponse](
          new DescribeGlobalSequencePartitionRequest.Builder(new DescribeGlobalSequencePartitionRequestData().setTopicId(topicId).setPartition(0)).build(),
          broker.socketServer, broker.config.interBrokerListenerName)
        assertEquals(physicalOffset, describe().data().physicalLastOffset())
        broker.shutdown()
        assertFutureThrows(classOf[CoordinatorNotAvailableException], oldService.committedProgress(partition))
        broker.startup()
        cluster.waitForReadyBrokers()
        assertNotSame(oldService, broker.globalSequenceCoordinator)
        assertEquals(mappedPartition, broker.globalSequenceCoordinator.partitionFor(topicId))
        TestUtils.waitUntilTrue(() => Try(broker.globalSequenceCoordinator.committedProgress(partition).get(10, TimeUnit.SECONDS)).toOption.exists(p => p.isPresent && p.get().lastOffset() == physicalOffset),
          "Global sequence shard did not reload after broker restart", 30000)
        val recovered = describe().data()
        assertEquals(Errors.NONE.code(), recovered.errorCode())
        assertEquals(physicalOffset, recovered.physicalLastOffset())
        assertEquals(registrationId, recovered.registrationId())
        assertEquals(0L, recovered.indexerGeneration())
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }

  @Test
  def testRoutingOverNetworkAndCoordinatorLeaderChange(): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(new TestKitNodes.Builder()
      .setNumBrokerNodes(3).setNumControllerNodes(1).build())
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_NUM_PARTITIONS_CONFIG, "2")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_REPLICATION_FACTOR_CONFIG, "3")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_MIN_ISR_CONFIG, "2")
      .build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      val brokers = cluster.brokers().values().asScala.toSeq.sortBy(_.config.brokerId)
      val ids = brokers.map(b => Int.box(b.config.brokerId))
      val source = brokers.head
      val admin = Admin.create(cluster.clientProperties())
      try {
        admin.createTopics(util.List.of(new NewTopic("routed", util.Map.of(Int.box(0), ids.asJava)).configs(util.Map.of(
          TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "true", TopicConfig.CLEANUP_POLICY_CONFIG, "delete"))))
          .all().get(30, TimeUnit.SECONDS)
        TestUtils.waitUntilTrue(() => brokers.forall(b => b.metadataCache.contains(new TopicPartition("routed", 0)) &&
          b.metadataCache.numPartitions(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME).isPresent),
          "Data and index topic metadata did not reach all brokers", 30000)
        val topicId = admin.describeTopics(util.List.of("routed")).allTopicNames().get(30, TimeUnit.SECONDS).get("routed").topicId()
        val partition = new PartitionKey(topicId, 0)
        val mapped = source.globalSequenceCoordinator.partitionFor(topicId)
        val indexPartition = new TopicPartition(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, mapped)

        def moveIndexLeader(target: Int): Unit = {
          val replicas = (Seq(Int.box(target)) ++ ids.filter(_.intValue() != target)).asJava
          admin.alterPartitionReassignments(util.Map.of(indexPartition, util.Optional.of(new NewPartitionReassignment(replicas))))
            .all().get(30, TimeUnit.SECONDS)
          TestUtils.waitUntilTrue(() => admin.listPartitionReassignments().reassignments().get(10, TimeUnit.SECONDS).isEmpty,
            "Index reassignment did not complete", 30000)
          val current = admin.describeTopics(util.List.of(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)).allTopicNames().get(10, TimeUnit.SECONDS)
            .get(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME).partitions().get(mapped).leader().id()
          if (current != target) admin.electLeaders(ElectionType.PREFERRED, util.Set.of(indexPartition)).all().get(30, TimeUnit.SECONDS)
          TestUtils.waitUntilTrue(() => brokers.forall(b =>
            b.metadataCache.getImage().topics().getTopic(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME).partitions().get(mapped).leader == target),
            "Index leader metadata did not reach all brokers", 30000)
        }

        moveIndexLeader(brokers(1).config.brokerId)
        // The same API goes directly to the local service on the index leader and over the inter-broker network elsewhere.
        brokers.foreach { broker =>
          val description = broker.indexRoutingManager.describePartition(partition, 30000).get(35, TimeUnit.SECONDS)
          assertTrue(description.value.committedProgress().isEmpty)
          assertEquals(brokers(1).config.brokerId, description.coordinator.node.id())
        }
        val sourceEpoch = source.metadataCache.getImage().topics().getTopic(topicId).partitions().get(0).leaderEpoch
        val registration = new RegistrationRequest(partition, source.config.brokerId, sourceEpoch, -1, Uuid.randomUuid())
        val registered = source.indexRoutingManager.registerIndexer(registration, 30000).get(35, TimeUnit.SECONDS)
        assertTrue(registered.value.registered())
        val owner = registered.value.indexer().get()
        val props = cluster.clientProperties()
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
        props.put(ProducerConfig.ACKS_CONFIG, "all")
        val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
        try {
          def produce(): PhysicalBatch = {
            val offset = producer.send(new ProducerRecord[Array[Byte], Array[Byte]]("routed", 0, null, Array[Byte](1)))
              .get(30, TimeUnit.SECONDS).offset()
            new PhysicalBatch(partition, offset, offset, 1)
          }
          def request(batch: PhysicalBatch, predecessor: Long): AppendRequest = new AppendRequest(batch, predecessor,
            source.replicaManager.localLog(new TopicPartition("routed", 0)).get.highWatermark, owner)
          val first = produce()
          val append = request(first, -1)
          val indexed = source.indexRoutingManager.appendIndex(append, 30000).get(35, TimeUnit.SECONDS)
          assertEquals(AppendStatus.INDEXED, indexed.value.status())
          assertEquals(0L, indexed.value.globalBaseOffset().getAsLong)
          moveIndexLeader(brokers(2).config.brokerId)
          assertFalse(source.indexRoutingManager.isCurrent(partition, indexed.coordinator))
          // A newly elected leader may have the replicated tail before its HW catches up.
          // Reattach to the same registration barrier before using progress for recovery.
          val confirmed = source.indexRoutingManager.registerIndexer(registration, 30000).get(35, TimeUnit.SECONDS)
          assertTrue(confirmed.value.registered())
          assertEquals(owner, confirmed.value.indexer().get())
          val recovered = source.indexRoutingManager.describePartition(partition, 30000).get(35, TimeUnit.SECONDS)
          assertEquals(first, recovered.value.committedProgress().get())
          assertEquals(owner, recovered.value.currentIndexer().get())
          assertEquals(brokers(2).config.brokerId, recovered.coordinator.node.id())
          val retry = source.indexRoutingManager.appendIndex(append, 30000).get(35, TimeUnit.SECONDS)
          assertEquals(AppendStatus.ALREADY_INDEXED, retry.value.status())
          assertTrue(retry.value.globalBaseOffset().isEmpty)
          val second = produce()
          val next = source.indexRoutingManager.appendIndex(request(second, first.baseOffset()), 30000).get(35, TimeUnit.SECONDS)
          assertEquals(AppendStatus.INDEXED, next.value.status())
          assertEquals(1L, next.value.globalBaseOffset().getAsLong)
        } finally producer.close()
      } finally admin.close()
    } finally cluster.close()
  }

}
