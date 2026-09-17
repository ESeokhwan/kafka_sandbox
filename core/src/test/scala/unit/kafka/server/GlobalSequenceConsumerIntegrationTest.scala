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

import kafka.examples.GlobalSequenceConsumerExample
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, NewPartitionReassignment, NewTopic, RecordsToDelete}
import org.apache.kafka.clients.consumer.{ConsumerConfig, GlobalSequenceConsumerConfig, GlobalSequenceConsumerRecord, GlobalSequenceTopicIdMismatchException, KafkaConsumer, KafkaGlobalSequenceConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.{ElectionType, TopicPartition, Uuid}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.{ElectionNotNeededException, OffsetOutOfRangeException}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.internals.Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.common.test.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.coordinator.globalsequence.{GlobalSequenceCoordinatorConfig, GlobalSequenceCoordinatorRecordSerde}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PartitionKey
import org.apache.kafka.coordinator.globalsequence.generated.{BatchIndexKey, BatchIndexValue}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.{ExecutionException, Future, TimeUnit}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

@Timeout(240)
class GlobalSequenceConsumerIntegrationTest {
  private case class TopicRef(name: String, id: Uuid, partitions: Int) {
    def tp(partition: Int): TopicPartition = new TopicPartition(name, partition)
    def key(partition: Int): PartitionKey = new PartitionKey(id, partition)
  }
  private case class Input(timestamp: Long, key: Array[Byte], value: Array[Byte], headers: Seq[(String, Array[Byte])])
  private case class Produced(partition: Int, offset: Long, input: Input)
  private case class Mapping(globalBase: Long, partition: Int, physicalBase: Long, count: Int)

  private class Context extends AutoCloseable {
    val cluster = new KafkaClusterTestKit.Builder(new TestKitNodes.Builder()
      .setNumBrokerNodes(3).setNumControllerNodes(1).build())
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_NUM_PARTITIONS_CONFIG, "2")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_REPLICATION_FACTOR_CONFIG, "3")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_MIN_ISR_CONFIG, "2")
      .setConfigProp(GlobalSequenceCoordinatorConfig.RETENTION_REFRESH_INTERVAL_MS_CONFIG, "100")
      .setConfigProp("transaction.state.log.num.partitions", "1")
      .setConfigProp("transaction.state.log.replication.factor", "3")
      .setConfigProp("transaction.state.log.min.isr", "2")
      .build()
    var admin: Admin = _
    private val stopped = mutable.Set.empty[Int]

    def brokers: Vector[BrokerServer] = cluster.brokers().values().asScala.toVector.sortBy(_.config.brokerId)
    def liveBrokers: Vector[BrokerServer] = brokers.filterNot(broker => stopped(broker.config.brokerId))

    def start(): Unit = {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      admin = Admin.create(cluster.clientProperties())
    }

    def create(name: String, partitions: Int): TopicRef = {
      val brokerIds = brokers.map(broker => Int.box(broker.config.brokerId))
      val assignments = (0 until partitions).map { partition =>
        val rotated = brokerIds.drop(partition % brokerIds.size) ++ brokerIds.take(partition % brokerIds.size)
        Int.box(partition) -> rotated.asJava
      }.toMap.asJava
      val result = admin.createTopics(util.List.of(new NewTopic(name, assignments).configs(util.Map.of(
        TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "true",
        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE))))
      result.all().get(60, TimeUnit.SECONDS)
      val ref = TopicRef(name, result.topicId(name).get(60, TimeUnit.SECONDS), partitions)
      await("Source metadata and committed index owners") {
        liveBrokers.forall(_.metadataCache.getTopicId(name) == ref.id) && (0 until partitions).forall { partition =>
          Try(liveBrokers.head.indexRoutingManager.describePartition(ref.key(partition), 1000)
            .get(2, TimeUnit.SECONDS)).toOption.exists(_.value.currentIndexer().isPresent)
        }
      }
      ref
    }

    def producer(transactionalId: Option[String] = None): KafkaProducer[Array[Byte], Array[Byte]] = {
      val properties = cluster.clientProperties()
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      properties.put(ProducerConfig.ACKS_CONFIG, "all")
      properties.put(ProducerConfig.LINGER_MS_CONFIG, "1000")
      properties.put(ProducerConfig.BATCH_SIZE_CONFIG, (1024 * 1024).toString)
      properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")
      transactionalId.foreach(properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, _))
      new KafkaProducer[Array[Byte], Array[Byte]](properties)
    }

    def consumerProperties(isolation: String, maxBatches: Int): Properties = {
      val properties = cluster.clientProperties()
      properties.put(GlobalSequenceConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      properties.put(GlobalSequenceConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      properties.put(GlobalSequenceConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation)
      properties.put(GlobalSequenceConsumerConfig.FETCH_MAX_BATCHES_CONFIG, maxBatches.toString)
      properties
    }

    def produceBatch(producer: KafkaProducer[Array[Byte], Array[Byte]], ref: TopicRef,
                     partition: Int, inputs: Seq[Input]): Vector[Produced] = {
      val futures: Seq[(Input, Future[RecordMetadata])] = inputs.map { input =>
        val headers: java.lang.Iterable[Header] = input.headers.map {
          case (key, value) => new RecordHeader(key, value).asInstanceOf[Header]
        }.asJava
        val record = new ProducerRecord[Array[Byte], Array[Byte]](ref.name, Int.box(partition),
          Long.box(input.timestamp), input.key, input.value, headers)
        input -> producer.send(record)
      }
      producer.flush()
      val produced = futures.map { case (input, future) =>
        val metadata = future.get(30, TimeUnit.SECONDS)
        Produced(partition, metadata.offset(), input)
      }.toVector
      awaitIndexed(ref, partition, produced.last.offset)
      produced
    }

    def awaitIndexed(ref: TopicRef, partition: Int, lastOffset: Long): Unit =
      await(s"Committed progress for ${ref.name}-$partition through $lastOffset") {
        Try(liveBrokers.head.indexRoutingManager.describePartition(ref.key(partition), 1000)
          .get(2, TimeUnit.SECONDS)).toOption.exists { response =>
          response.value.committedProgress().isPresent &&
            response.value.committedProgress().get().lastOffset() == lastOffset
        }
      }

    def mappings(ref: TopicRef, expectedCount: Int): Vector[Mapping] = {
      val properties = cluster.clientProperties()
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](properties)
      try {
        val topicPartition = indexTopicPartition(ref)
        consumer.assign(util.List.of(topicPartition))
        consumer.seekToBeginning(util.List.of(topicPartition))
        val serde = new GlobalSequenceCoordinatorRecordSerde
        val result = mutable.ArrayBuffer.empty[Mapping]
        await("Committed batch mappings in the independent index scan") {
          consumer.poll(Duration.ofMillis(100)).asScala.foreach { record =>
            val decoded = serde.deserialize(ByteBuffer.wrap(record.key()), ByteBuffer.wrap(record.value()))
            if (decoded.key().isInstanceOf[BatchIndexKey]) {
              val key = decoded.key().asInstanceOf[BatchIndexKey]
              if (key.topicId() == ref.id) {
                val value = decoded.value().message().asInstanceOf[BatchIndexValue]
                result += Mapping(key.globalBaseOffset(), value.physicalPartition(),
                  value.physicalBaseOffset(), value.recordCount())
              }
            }
          }
          result.size >= expectedCount
        }
        result.toVector.sortBy(_.globalBase)
      } finally consumer.close()
    }

    def indexTopicPartition(ref: TopicRef): TopicPartition =
      new TopicPartition(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, liveBrokers.head.globalSequenceCoordinator.partitionFor(ref.id))

    def leader(topicPartition: TopicPartition): BrokerServer = {
      val leaderId = liveBrokers.head.metadataCache.getImage().topics().getTopic(topicPartition.topic())
        .partitions().get(topicPartition.partition()).leader
      liveBrokers.find(_.config.brokerId == leaderId).get
    }

    def move(topicPartition: TopicPartition, topicId: Uuid, targetId: Int): Unit = {
      val replicas = Int.box(targetId) +: brokers.map(broker => Int.box(broker.config.brokerId))
        .filterNot(_.intValue() == targetId)
      admin.alterPartitionReassignments(util.Map.of(topicPartition,
        util.Optional.of(new NewPartitionReassignment(replicas.asJava)))).all().get(30, TimeUnit.SECONDS)
      await("Partition reassignment") {
        admin.listPartitionReassignments().reassignments().get(10, TimeUnit.SECONDS).isEmpty
      }
      try admin.electLeaders(ElectionType.PREFERRED, util.Set.of(topicPartition)).all().get(30, TimeUnit.SECONDS)
      catch {
        case exception: ExecutionException if exception.getCause.isInstanceOf[ElectionNotNeededException] =>
      }
      await("Leader move") {
        liveBrokers.forall(_.metadataCache.getImage().topics().getTopic(topicId)
          .partitions().get(topicPartition.partition()).leader == targetId)
      }
    }

    def stop(broker: BrokerServer): Unit = {
      stopped += broker.config.brokerId
      broker.shutdown()
    }

    def restart(broker: BrokerServer): Unit = {
      broker.startup()
      stopped -= broker.config.brokerId
      cluster.waitForReadyBrokers()
    }

    def requestCount(apiKey: ApiKeys): Long = brokers.map { broker =>
      broker.socketServer.dataPlaneRequestChannel.metrics(apiKey.name).totalTimeHist.count
    }.sum

    def await(reason: String)(condition: => Boolean): Unit =
      TestUtils.waitUntilTrue(() => condition, reason, 30000)

    override def close(): Unit = {
      stopped.toVector.foreach { id => Try(brokers.find(_.config.brokerId == id).get.startup()) }
      if (admin != null) admin.close()
      cluster.close()
    }
  }

  @Test
  def testPublicConsumerPaginatesAcrossLeaderAndBootstrapFailoverAndRunsExample(): Unit = {
    val context = new Context
    try {
      context.start()
      val ref = context.create("public-consumer", 2)
      val producer = context.producer()
      val produced = try {
        context.produceBatch(producer, ref, 0, Seq(
          Input(1000, bytes("k0"), bytes("v0"), Seq("header" -> bytes("h0"))),
          Input(1001, null, null, Seq.empty),
          Input(1002, bytes("k2"), bytes("v2"), Seq("header" -> bytes("h2"))))) ++
          context.produceBatch(producer, ref, 1, Seq(
            Input(2000, bytes("k3"), bytes("v3"), Seq("header" -> bytes("h3"))),
            Input(2001, bytes("k4"), bytes("v4"), Seq.empty),
            Input(2002, bytes("k5"), bytes("v5"), Seq("header" -> bytes("h5")))))
      } finally producer.close()
      val mappings = context.mappings(ref, 2)
      assertEquals(Vector(3, 3), mappings.map(_.count))
      val byPhysical = produced.map(record => (record.partition, record.offset) -> record).toMap
      val expected = mappings.flatMap { mapping =>
        (0 until mapping.count).map { delta =>
          val physicalOffset = mapping.physicalBase + delta
          mapping.globalBase + delta -> byPhysical((mapping.partition, physicalOffset))
        }
      }

      val groupApis = Seq(ApiKeys.JOIN_GROUP, ApiKeys.HEARTBEAT, ApiKeys.OFFSET_COMMIT)
      val groupCountsBefore = groupApis.map(api => api -> context.requestCount(api)).toMap
      val globalFetchesBefore = context.requestCount(ApiKeys.FETCH_GLOBAL_SEQUENCE)
      val properties = context.consumerProperties("read_uncommitted", 1)
      val consumer = new KafkaGlobalSequenceConsumer[Array[Byte], Array[Byte]](properties)
      try {
        val first = consumer.fetch(ref.name, 1, 5, Duration.ofSeconds(10))
        assertEquals(ref.id, first.topicId())
        assertEquals(6L, first.committedGlobalEndOffset())
        assertEquals(3L, first.nextGlobalOffset())
        assertFalse(first.transactionPending())
        assertTrue(first.error().isEmpty)

        val targetId = context.brokers.find(_.config.brokerId != context.leader(ref.tp(1)).config.brokerId).get.config.brokerId
        context.move(ref.tp(1), ref.id, targetId)
        val indexTopicPartition = context.indexTopicPartition(ref)
        val indexTopicId = context.liveBrokers.head.metadataCache.getImage().topics()
          .getTopic(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME).id()
        val indexTarget = context.brokers.find(_.config.brokerId != context.leader(indexTopicPartition).config.brokerId).get.config.brokerId
        context.move(indexTopicPartition, indexTopicId, indexTarget)

        val second = consumer.fetch(ref.name, first.topicId(), first.nextGlobalOffset(), 5, Duration.ofSeconds(10))
        assertEquals(5L, second.nextGlobalOffset())
        val actual = (first.records().asScala ++ second.records().asScala).toVector
        assertEquals(expected.filter { case (global, _) => global >= 1 && global < 5 }.map(_._1),
          actual.map(_.globalOffset()))
        actual.foreach { record =>
          val source = expected.toMap.apply(record.globalOffset())
          assertRecord(source, record)
        }
      } finally consumer.close()
      groupApis.foreach(api => assertEquals(groupCountsBefore(api), context.requestCount(api), api.name))
      assertTrue(context.requestCount(ApiKeys.FETCH_GLOBAL_SEQUENCE) > globalFetchesBefore)

      val stopped = context.brokers.head
      val survivor = context.brokers.find(_.config.brokerId != stopped.config.brokerId).get.config.brokerId
      (0 until ref.partitions).foreach(partition => context.move(ref.tp(partition), ref.id, survivor))
      val indexTopicPartition = context.indexTopicPartition(ref)
      val indexTopicId = context.liveBrokers.head.metadataCache.getImage().topics()
        .getTopic(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME).id()
      context.move(indexTopicPartition, indexTopicId, survivor)
      context.stop(stopped)
      val failoverConsumer = new KafkaGlobalSequenceConsumer[Array[Byte], Array[Byte]](
        context.consumerProperties("read_uncommitted", 10))
      try {
        val page = failoverConsumer.fetch(ref.name, 0, 6, Duration.ofSeconds(20))
        assertEquals((0L until 6L).toVector, page.records().asScala.map(_.globalOffset()).toVector)
      } finally failoverConsumer.close()
      context.restart(stopped)

      val outputBytes = new ByteArrayOutputStream
      val output = new PrintStream(outputBytes, true, UTF_8)
      try GlobalSequenceConsumerExample.read(context.consumerProperties("read_uncommitted", 1),
        ref.name, 0, 6, output)
      finally output.close()
      val lines = outputBytes.toString(UTF_8).linesIterator.toVector
      assertEquals(6, lines.count(_.startsWith("record\t")))
      assertEquals(2, lines.count(_.startsWith("page\t")))
      assertTrue(lines.last.contains(s"topicId=${ref.id}"))
      assertTrue(lines.last.contains("next=6"))
      assertTrue(lines.last.contains("pending=false"))
      assertTrue(lines.last.contains("error=NONE"))
    } finally context.close()
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testReadCommittedReturnsPendingAndAdvancesAcrossCommittedOrAbortedBatch(commit: Boolean): Unit = {
    val context = new Context
    try {
      context.start()
      val ref = context.create(s"consumer-transaction-$commit", 1)
      val transaction = context.producer(Some(s"consumer-transactional-id-$commit"))
      val regular = context.producer()
      val committed = new KafkaGlobalSequenceConsumer[Array[Byte], Array[Byte]](
        context.consumerProperties("read_committed", 1))
      val uncommitted = new KafkaGlobalSequenceConsumer[Array[Byte], Array[Byte]](
        context.consumerProperties("read_uncommitted", 10))
      try {
        transaction.initTransactions()
        transaction.beginTransaction()
        val transactionalMetadata = transaction.send(new ProducerRecord[Array[Byte], Array[Byte]](
          ref.name, 0, null, bytes("transactional"))).get(30, TimeUnit.SECONDS)
        val regularMetadata = regular.send(new ProducerRecord[Array[Byte], Array[Byte]](
          ref.name, 0, null, bytes("regular"))).get(30, TimeUnit.SECONDS)
        assertEquals(0L, transactionalMetadata.offset())
        assertEquals(1L, regularMetadata.offset())
        context.awaitIndexed(ref, 0, 1)

        val pending = committed.fetch(ref.name, 0, 2, Duration.ofSeconds(10))
        assertTrue(pending.transactionPending())
        assertTrue(pending.records().isEmpty)
        assertEquals(0L, pending.nextGlobalOffset())
        assertEquals(2L, pending.committedGlobalEndOffset())
        val visible = uncommitted.fetch(ref.name, 0, 2, Duration.ofSeconds(10))
        assertEquals(Vector(0L, 1L), visible.records().asScala.map(_.globalOffset()).toVector)

        if (commit) transaction.commitTransaction() else transaction.abortTransaction()
        context.await("Source LSO after transaction completion") {
          context.leader(ref.tp(0)).replicaManager.localLog(ref.tp(0)).exists(_.lastStableOffset() >= 2)
        }
        var first = committed.fetch(ref.name, pending.topicId(), 0, 2, Duration.ofSeconds(10))
        context.await("Resolved read_committed page") {
          first = committed.fetch(ref.name, pending.topicId(), 0, 2, Duration.ofSeconds(10))
          !first.transactionPending()
        }
        assertEquals(1L, first.nextGlobalOffset())
        assertEquals(if (commit) Vector(0L) else Vector.empty,
          first.records().asScala.map(_.globalOffset()).toVector)
        val second = committed.fetch(ref.name, pending.topicId(), first.nextGlobalOffset(), 2,
          Duration.ofSeconds(10))
        assertEquals(Vector(1L), second.records().asScala.map(_.globalOffset()).toVector)
        assertEquals(bytes("regular").toVector, second.records().get(0).value().toVector)
        assertEquals(2L, second.nextGlobalOffset())
      } finally {
        committed.close()
        uncommitted.close()
        transaction.close()
        regular.close()
      }
    } finally context.close()
  }

  @Test
  def testPartialSourceDeletionErrorAndRecreatedTopicUuidFence(): Unit = {
    val context = new Context
    try {
      context.start()
      val ref = context.create("consumer-deletion", 2)
      val producer = context.producer()
      try {
        context.produceBatch(producer, ref, 0, Seq(Input(1000, null, bytes("kept"), Seq.empty)))
        context.produceBatch(producer, ref, 1, Seq(Input(1001, null, bytes("deleted"), Seq.empty)))
      } finally producer.close()
      val mappings = context.mappings(ref, 2)
      assertEquals(Vector(0L, 1L), mappings.map(_.globalBase))
      val missing = mappings(1)
      val missingTopicPartition = ref.tp(missing.partition)
      val deleteBefore = missing.physicalBase + missing.count
      context.await("Deletion limit includes the indexed source range") {
        context.liveBrokers.forall(_.replicaManager.localLog(missingTopicPartition)
          .exists(_.globalSequenceDeletionLimit() >= deleteBefore))
      }
      context.admin.deleteRecords(util.Map.of(missingTopicPartition,
        RecordsToDelete.beforeOffset(deleteBefore))).all().get(30, TimeUnit.SECONDS)
      context.await("Deleted source range") {
        context.leader(missingTopicPartition).replicaManager.localLog(missingTopicPartition)
          .exists(_.logStartOffset() >= deleteBefore)
      }

      val consumer = new KafkaGlobalSequenceConsumer[Array[Byte], Array[Byte]](
        context.consumerProperties("read_uncommitted", 10))
      try {
        val partial = consumer.fetch(ref.name, 0, 2, Duration.ofSeconds(10))
        assertEquals(Vector(0L), partial.records().asScala.map(_.globalOffset()).toVector)
        assertEquals(1L, partial.nextGlobalOffset())
        assertTrue(partial.error().orElseThrow().isInstanceOf[OffsetOutOfRangeException])

        context.admin.deleteTopics(util.List.of(ref.name)).all().get(30, TimeUnit.SECONDS)
        context.await("Old topic metadata removal") {
          context.liveBrokers.forall(_.metadataCache.getTopicId(ref.name) == Uuid.ZERO_UUID)
        }
        val replacement = context.create(ref.name, 1)
        assertNotEquals(ref.id, replacement.id)
        assertThrows(classOf[GlobalSequenceTopicIdMismatchException], () =>
          consumer.fetch(ref.name, ref.id, partial.nextGlobalOffset(), 2, Duration.ofSeconds(10)))
      } finally consumer.close()
    } finally context.close()
  }

  private def assertRecord(expected: Produced, actual: GlobalSequenceConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
    assertEquals(expected.partition, actual.physicalPartition())
    assertEquals(expected.offset, actual.physicalOffset())
    assertEquals(expected.input.timestamp, actual.timestamp())
    assertArrayEquals(expected.input.key, actual.key())
    assertArrayEquals(expected.input.value, actual.value())
    assertEquals(expected.input.headers.map(_._1), actual.headers().asScala.map(_.key()).toSeq)
    expected.input.headers.zip(actual.headers().asScala).foreach {
      case ((_, value), header) => assertArrayEquals(value, header.value())
    }
    assertTrue(actual.leaderEpoch().isPresent)
  }

  private def bytes(value: String): Array[Byte] = value.getBytes(UTF_8)
}
