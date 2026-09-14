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
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{ElectionType, TopicPartition, Uuid}
import org.apache.kafka.common.message.{LookupGlobalSequenceRequestData, AppendGlobalSequenceIndexRequestData, DescribeGlobalSequencePartitionRequestData, RegisterGlobalSequenceIndexerRequestData, RegisterGlobalSequenceIndexerResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.storage.internals.log.AppendOrigin
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{CoordinatorNotAvailableException, InvalidConfigurationException, InvalidPartitionsException}
import org.apache.kafka.common.internals.Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME
import org.apache.kafka.common.test.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.coordinator.globalsequence.{GlobalSequenceCoordinatorConfig, GlobalSequenceCoordinatorRecordSerde}
import org.apache.kafka.coordinator.globalsequence.generated.{BatchIndexKey, BatchIndexValue}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{AppendRequest, AppendStatus, PartitionKey, RegistrationRequest}
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotSame, assertTrue}
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.nio.ByteBuffer
import java.time.Duration
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
      // This test exercises the low-level RPCs with an explicit owner. Automatic indexing is tested below.
      broker.globalSequenceIndexerManager.close()
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
        // Inject data directly: this test deliberately disabled the automatic indexer above.
        val dataPartition = new TopicPartition("ordered", 0)
        val physicalOffset = broker.replicaManager.onlinePartition(dataPartition).get.appendRecordsToLeader(
          MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(Array[Byte](1, 2, 3))),
          AppendOrigin.CLIENT, -1, RequestLocal.noCaching).firstOffset
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
        TestUtils.waitUntilTrue(() => Try(describe().data()).toOption.exists(_.indexerGeneration() > 0),
          "The restarted broker did not register its automatic indexer", 30000)
        val recovered = describe().data()
        assertEquals(Errors.NONE.code(), recovered.errorCode())
        assertEquals(physicalOffset, recovered.physicalLastOffset())
        assertTrue(recovered.registrationId() != registrationId)
        assertTrue(recovered.indexerGeneration() > 0)
        val resumedProducer = new KafkaProducer[Array[Byte], Array[Byte]](props)
        val nextOffset = try {
          resumedProducer.send(new ProducerRecord[Array[Byte], Array[Byte]]("ordered", 0, null, Array[Byte](4)))
            .get(30, TimeUnit.SECONDS).offset()
        } finally resumedProducer.close()
        TestUtils.waitUntilTrue(() => Try(describe().data()).toOption.exists(_.physicalLastOffset() == nextOffset),
          "Automatic indexing did not resume from the committed batch after restart", 30000)
        val lookup = new LookupGlobalSequenceRequest.Builder(new LookupGlobalSequenceRequestData().setTopicId(topicId)
          .setGlobalStartOffset(0).setGlobalEndOffsetExclusive(100).setMaxBatches(10).setTimeoutMs(10000)).build()
        val recoveredPage = IntegrationTestUtils.connectAndReceive[LookupGlobalSequenceResponse](lookup, broker.socketServer,
          broker.config.interBrokerListenerName).data()
        assertEquals(Errors.NONE.code(), recoveredPage.errorCode(), recoveredPage.errorMessage())
        assertEquals(2L, recoveredPage.committedGlobalEndOffset())
        assertEquals(2, recoveredPage.batches().size())
        assertEquals(physicalOffset, recoveredPage.batches().get(0).physicalBaseOffset())
        assertEquals(nextOffset, recoveredPage.batches().get(1).physicalBaseOffset())
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
      // Keep manual routing/registration coverage independent of the automatic owner.
      brokers.foreach(_.globalSequenceIndexerManager.close())
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
        locally {
          val reader = new GlobalSequenceSourceReader(source.replicaManager)
          def produce(): GlobalSequenceSourceReader.ReadResult = {
            val sourcePartition = source.replicaManager.onlinePartition(new TopicPartition("routed", 0)).get
            val info = sourcePartition.appendRecordsToLeader(
              MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(Array[Byte](1))),
              AppendOrigin.CLIENT, -1, RequestLocal.noCaching)
            val offset = info.firstOffset
            TestUtils.waitUntilTrue(() => sourcePartition.log.get.highWatermark > info.lastOffset,
              "Manually appended data did not reach its HW", 30000)
            val read = reader.read(partition, sourceEpoch, offset, maxBytes = 1)
            assertEquals(GlobalSequenceSourceReader.BATCH, read.status)
            assertEquals(offset, read.batch.get.baseOffset())
            read
          }
          def request(read: GlobalSequenceSourceReader.ReadResult, predecessor: Long): AppendRequest =
            new AppendRequest(read.batch.get, predecessor, read.dataHighWatermark, owner)
          val firstRead = produce()
          val first = firstRead.batch.get
          val append = request(firstRead, -1)
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
        }
      } finally admin.close()
    } finally cluster.close()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("1", "all"))
  def testAutomaticIndexingAcrossSourcePartitionsAndIndexLeaderChange(acks: String): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(new TestKitNodes.Builder()
      .setNumBrokerNodes(3).setNumControllerNodes(1).build())
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_NUM_PARTITIONS_CONFIG, "2")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_REPLICATION_FACTOR_CONFIG, "3")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_MIN_ISR_CONFIG, "2")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEXER_NUM_THREADS_CONFIG, "1")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEXER_READ_MAX_BYTES_CONFIG, "1")
      .build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      val brokers = cluster.brokers().values().asScala.toSeq.sortBy(_.config.brokerId)
      val ids = brokers.map(b => Int.box(b.config.brokerId))
      val admin = Admin.create(cluster.clientProperties())
      try {
        val assignments = util.Map.of(Int.box(0), ids.asJava, Int.box(1), (ids.tail :+ ids.head).asJava)
        admin.createTopics(util.List.of(
          new NewTopic("automatic", assignments).configs(util.Map.of(
            TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "true", TopicConfig.CLEANUP_POLICY_CONFIG, "delete")),
          new NewTopic("ordinary", 1, 1.toShort))).all().get(30, TimeUnit.SECONDS)
        TestUtils.waitUntilTrue(() => brokers.forall(b => b.metadataCache.contains(new TopicPartition("automatic", 1)) &&
          b.metadataCache.numPartitions(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME).isPresent),
          "Automatic source/index metadata did not reach the brokers", 30000)
        val topics = admin.describeTopics(util.List.of("automatic", "ordinary")).allTopicNames().get(30, TimeUnit.SECONDS)
        val topicId = topics.get("automatic").topicId()
        val ordinary = new PartitionKey(topics.get("ordinary").topicId(), 0)
        assertTrue(brokers.forall(_.globalSequenceIndexerManager.indexer(ordinary).isEmpty))
        val keys = (0 to 1).map(p => new PartitionKey(topicId, p))
        TestUtils.waitUntilTrue(() => keys.indices.forall(p => brokers(p).globalSequenceIndexerManager.indexer(keys(p)).isDefined),
          "Source leaders did not start automatic indexers", 30000)
        assertTrue(keys.indices.forall(p => brokers.filterNot(_ == brokers(p)).forall(_.globalSequenceIndexerManager.indexer(keys(p)).isEmpty)))
        val indexPartition = new TopicPartition(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, brokers.head.globalSequenceCoordinator.partitionFor(topicId))
        val props = cluster.clientProperties()
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
        props.put(ProducerConfig.ACKS_CONFIG, acks)
        if (acks == "1") props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
        val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
        def produceAndAwait(start: Int): Unit = {
          for (offset <- start until start + 3; partition <- 0 to 1) {
            val result = producer.send(new ProducerRecord[Array[Byte], Array[Byte]]("automatic", partition, null, Array[Byte](1)))
              .get(30, TimeUnit.SECONDS)
            assertEquals(offset.toLong, result.offset(), "Produce still returns the physical offset")
            val committed = brokers(partition).indexRoutingManager.describePartition(keys(partition), 5000)
              .get(6, TimeUnit.SECONDS).value.committedProgress()
            assertTrue(committed.isPresent && committed.get().lastOffset() >= result.offset(),
              "A successful Produce response must already be covered by committed index progress")
          }
          // Each broker accepts client lookup, routing to the local or remote index leader.
          val globalEnd = (start + 3) * 2L
          brokers.foreach { broker =>
            var next = 0L
            while (next < globalEnd) {
              val request = new LookupGlobalSequenceRequest.Builder(new LookupGlobalSequenceRequestData().setTopicId(topicId)
                .setGlobalStartOffset(next).setGlobalEndOffsetExclusive(Long.MaxValue).setMaxBatches(2).setTimeoutMs(10000)).build()
              val page = IntegrationTestUtils.connectAndReceive[LookupGlobalSequenceResponse](request, broker.socketServer,
                broker.config.interBrokerListenerName).data()
              assertEquals(Errors.NONE.code(), page.errorCode(), page.errorMessage())
              assertEquals(globalEnd, page.committedGlobalEndOffset())
              assertEquals(2, page.batches().size())
              page.batches().asScala.foreach { mapping =>
                assertEquals(next, mapping.globalBaseOffset())
                assertEquals((next % 2).toInt, mapping.physicalPartition())
                assertEquals(next / 2, mapping.physicalBaseOffset())
                assertEquals(next + 1, mapping.selectedGlobalEndOffset())
                next += 1
              }
              assertEquals(next, page.nextGlobalOffset())
            }
          }
          keys.foreach { key =>
            TestUtils.waitUntilTrue(() => Try(brokers.head.indexRoutingManager.describePartition(key, 5000)
              .get(6, TimeUnit.SECONDS)).toOption.exists(r => r.value.committedProgress().isPresent &&
              r.value.committedProgress().get().lastOffset() == start + 2),
              s"Automatic indexing did not commit $key through ${start + 2}", 30000)
          }
        }
        try {
          produceAndAwait(0)
          val currentLeader = admin.describeTopics(util.List.of(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)).allTopicNames()
            .get(30, TimeUnit.SECONDS).get(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME).partitions().get(indexPartition.partition()).leader().id()
          val target = ids.find(_.intValue() != currentLeader).get
          val replicas = (Seq(target) ++ ids.filterNot(_ == target)).asJava
          admin.alterPartitionReassignments(util.Map.of(indexPartition, util.Optional.of(new NewPartitionReassignment(replicas))))
            .all().get(30, TimeUnit.SECONDS)
          TestUtils.waitUntilTrue(() => admin.listPartitionReassignments().reassignments().get(10, TimeUnit.SECONDS).isEmpty,
            "Index reassignment did not finish", 30000)
          admin.electLeaders(ElectionType.PREFERRED, util.Set.of(indexPartition)).all().get(30, TimeUnit.SECONDS)
          TestUtils.waitUntilTrue(() => brokers.forall(_.metadataCache.getImage().topics().getTopic(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)
            .partitions().get(indexPartition.partition()).leader == target.intValue()), "Index leader did not change", 30000)
          produceAndAwait(3)
        } finally producer.close()

        // Inspect the durable index records independently of the in-memory committed-progress view.
        val consumerProps = cluster.clientProperties()
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
        try {
          consumer.assign(util.List.of(indexPartition))
          consumer.seekToBeginning(util.List.of(indexPartition))
          val end = consumer.endOffsets(util.List.of(indexPartition)).get(indexPartition)
          val indexes = scala.collection.mutable.ArrayBuffer.empty[(Long, BatchIndexValue)]
          val serde = new GlobalSequenceCoordinatorRecordSerde
          TestUtils.waitUntilTrue(() => {
            consumer.poll(Duration.ofMillis(100)).asScala.foreach { record =>
              val decoded = serde.deserialize(ByteBuffer.wrap(record.key()), ByteBuffer.wrap(record.value()))
              decoded.key() match {
                case key: BatchIndexKey if key.topicId() == topicId =>
                  indexes += ((key.globalBaseOffset(), decoded.value().message().asInstanceOf[BatchIndexValue]))
                case _ =>
              }
            }
            consumer.position(indexPartition) >= end
          }, "Could not read the committed index log", 30000)
          assertEquals((0L until 12L).toSeq, indexes.map(_._1).toSeq)
          for (partition <- 0 to 1) {
            val batches = indexes.map(_._2).filter(_.physicalPartition() == partition)
            assertEquals((0L until 6L).toSeq, batches.map(_.physicalBaseOffset()).toSeq)
            assertTrue(batches.forall(batch => batch.physicalLastOffset() == batch.physicalBaseOffset() && batch.recordCount() == 1))
          }
        } finally consumer.close()
      } finally admin.close()
    } finally cluster.close()
  }

  @Test
  def testSourceLeaderRoundTripRecoversCommittedButUnindexedBatches(): Unit = {
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
      val tp = new TopicPartition("recovery", 0)
      val admin = Admin.create(cluster.clientProperties())
      try {
        admin.createTopics(util.List.of(new NewTopic(tp.topic(), util.Map.of(Int.box(0), ids.asJava))
          .configs(util.Map.of(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "true", TopicConfig.CLEANUP_POLICY_CONFIG, "delete"))))
          .all().get(30, TimeUnit.SECONDS)
        TestUtils.waitUntilTrue(() => brokers.forall(_.metadataCache.contains(tp)), "Source metadata was not published", 30000)
        val id = admin.describeTopics(util.List.of(tp.topic())).allTopicNames().get(30, TimeUnit.SECONDS).get(tp.topic()).topicId()
        val key = new PartitionKey(id, 0)
        val source = brokers.head
        def awaitProgress(last: Long): Unit = TestUtils.waitUntilTrue(() => Try(source.indexRoutingManager.describePartition(key, 5000)
          .get(6, TimeUnit.SECONDS)).toOption.exists(r => r.value.committedProgress().isPresent &&
          r.value.committedProgress().get().lastOffset() == last), s"Recovery did not index through $last", 30000)
        def appendOn(broker: BrokerServer, count: Int): Long = {
          val records = MemoryRecords.withRecords(Compression.NONE, (0 until count).map(_ => new SimpleRecord(Array[Byte](1))): _*)
          val info = broker.replicaManager.onlinePartition(tp).get.appendRecordsToLeader(records, AppendOrigin.CLIENT, -1, RequestLocal.noCaching)
          info.lastOffset()
        }
        assertEquals(0, appendOn(source, 1))
        awaitProgress(0)
        val firstIndexer = source.globalSequenceIndexerManager.indexer(key).get
        firstIndexer.close()
        // Commit source data with indexing stopped, independently of the Produce waiter added in step 13.
        assertEquals(2, appendOn(source, 2))
        TestUtils.waitUntilTrue(() => source.replicaManager.localLog(tp).get.highWatermark >= 3,
          "Source data did not replicate before leader movement", 30000)
        assertEquals(0L, source.indexRoutingManager.describePartition(key, 5000).get(6, TimeUnit.SECONDS)
          .value.committedProgress().get().lastOffset())
        def move(target: BrokerServer): Unit = {
          val targetId = Int.box(target.config.brokerId)
          val replicas = (Seq(targetId) ++ ids.filterNot(_ == targetId)).asJava
          admin.alterPartitionReassignments(util.Map.of(tp, util.Optional.of(new NewPartitionReassignment(replicas))))
            .all().get(30, TimeUnit.SECONDS)
          TestUtils.waitUntilTrue(() => admin.listPartitionReassignments().reassignments().get(10, TimeUnit.SECONDS).isEmpty,
            "Source reassignment did not finish", 30000)
          admin.electLeaders(ElectionType.PREFERRED, util.Set.of(tp)).all().get(30, TimeUnit.SECONDS)
          TestUtils.waitUntilTrue(() => brokers.forall(_.metadataCache.getImage().topics().getTopic(id)
            .partitions().get(0).leader == target.config.brokerId), "Source leader did not move", 30000)
        }
        move(brokers(1))
        awaitProgress(2)
        val secondIndexer = brokers(1).globalSequenceIndexerManager.indexer(key).get
        assertTrue(secondIndexer.sourceLeaderEpoch > firstIndexer.sourceLeaderEpoch)
        move(source)
        TestUtils.waitUntilTrue(() => source.globalSequenceIndexerManager.indexer(key)
          .exists(_.sourceLeaderEpoch > secondIndexer.sourceLeaderEpoch), "Returning source did not create a new lifetime", 30000)
        assertTrue(secondIndexer.isStopped)
        assertNotSame(firstIndexer, source.globalSequenceIndexerManager.indexer(key).get)
        assertEquals(3, appendOn(source, 1))
        awaitProgress(3)
      } finally admin.close()
    } finally cluster.close()
  }

}
