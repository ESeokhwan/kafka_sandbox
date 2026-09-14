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
import org.apache.kafka.clients.admin.{Admin, NewPartitionReassignment, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{ElectionType, TopicPartition, Uuid}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.internals.Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME
import org.apache.kafka.common.message.{FetchGlobalSequenceRequestData, FetchGlobalSequenceResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{FetchGlobalSequenceRequest, FetchGlobalSequenceResponse}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.test.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorConfig
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

@Timeout(180)
class GlobalSequenceTransactionFetchIntegrationTest {
  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testCommittedFetchAcrossTransactionsAndDataAndIndexLeaderChanges(commitFirst: Boolean): Unit = {
    val cluster = new KafkaClusterTestKit.Builder(new TestKitNodes.Builder().setNumBrokerNodes(3).setNumControllerNodes(1).build())
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_NUM_PARTITIONS_CONFIG, "2")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_REPLICATION_FACTOR_CONFIG, "3")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_MIN_ISR_CONFIG, "2")
      .setConfigProp("transaction.state.log.num.partitions", "1")
      .setConfigProp("transaction.state.log.replication.factor", "3")
      .setConfigProp("transaction.state.log.min.isr", "2")
      .build()
    try {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      val brokers = cluster.brokers().values().asScala.toSeq.sortBy(_.config.brokerId)
      val ids = brokers.map(b => Int.box(b.config.brokerId))
      val topic = "transactional-order"
      val admin = Admin.create(cluster.clientProperties())
      try {
        admin.createTopics(util.List.of(new NewTopic(topic, util.Map.of(Int.box(0), ids.asJava,
          Int.box(1), (Seq(ids(1), ids(0), ids(2))).asJava)).configs(util.Map.of(
          TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "true", TopicConfig.CLEANUP_POLICY_CONFIG, "delete"))))
          .all().get(30, TimeUnit.SECONDS)
        val topicId = admin.describeTopics(util.List.of(topic)).allTopicNames().get(30, TimeUnit.SECONDS).get(topic).topicId()
        def producer(transactional: Boolean): KafkaProducer[Array[Byte], Array[Byte]] = {
          val props = cluster.clientProperties()
          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
          props.put(ProducerConfig.ACKS_CONFIG, "all")
          if (transactional) props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "global-reader-transaction")
          new KafkaProducer[Array[Byte], Array[Byte]](props)
        }
        def fetch(broker: BrokerServer, start: Long = 0, end: Long = Long.MaxValue, committed: Boolean = true,
                  maxBatches: Int = 100, version: Short = 1): FetchGlobalSequenceResponseData = {
          val request = new FetchGlobalSequenceRequest.Builder(new FetchGlobalSequenceRequestData().setTopicId(topicId)
            .setGlobalStartOffset(start).setGlobalEndOffsetExclusive(end).setMaxBatches(maxBatches)
            .setIsolationLevel(if (committed) 1.toByte else 0.toByte).setTimeoutMs(10000)).build(version)
          val response = IntegrationTestUtils.connectAndReceive[FetchGlobalSequenceResponse](request, broker.socketServer,
            broker.config.interBrokerListenerName).data()
          assertEquals(Errors.NONE.code(), response.errorCode(), response.errorMessage())
          response
        }
        def move(tp: TopicPartition, target: Integer, id: Uuid): Unit = {
          val replicas = (Seq(target) ++ ids.filterNot(_ == target)).asJava
          admin.alterPartitionReassignments(util.Map.of(tp, util.Optional.of(new NewPartitionReassignment(replicas))))
            .all().get(30, TimeUnit.SECONDS)
          TestUtils.waitUntilTrue(() => admin.listPartitionReassignments().reassignments().get(10, TimeUnit.SECONDS).isEmpty,
            "Partition reassignment did not finish", 30000)
          admin.electLeaders(ElectionType.PREFERRED, util.Set.of(tp)).all().get(30, TimeUnit.SECONDS)
          TestUtils.waitUntilTrue(() => brokers.forall(_.metadataCache.getImage().topics().getTopic(id)
            .partitions().get(tp.partition()).leader == target.intValue()), "Partition leader did not move", 30000)
        }
        def awaitLso(offset: Long): Unit = TestUtils.waitUntilTrue(() => brokers.exists { broker =>
          broker.replicaManager.localLog(new TopicPartition(topic, 0)).exists(_.lastStableOffset() >= offset)
        }, s"Source LSO did not advance to $offset", 30000)
        val transaction = producer(transactional = true)
        val regular = producer(transactional = false)
        try {
          transaction.initTransactions()
          transaction.beginTransaction()
          assertEquals(0L, transaction.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, null, Array[Byte](10)))
            .get(30, TimeUnit.SECONDS).offset())
          assertEquals(0L, regular.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, 1, null, Array[Byte](20)))
            .get(30, TimeUnit.SECONDS).offset())
          def verifyPending(): Unit = brokers.foreach { broker =>
            val waiting = fetch(broker)
            assertEquals(2L, waiting.committedGlobalEndOffset())
            assertTrue(waiting.transactionPending())
            assertTrue(waiting.batches().isEmpty)
            assertEquals(0L, waiting.nextGlobalOffset())
            for (version <- Seq(0.toShort, 1.toShort)) {
              val uncommitted = fetch(broker, committed = false, version = version)
              assertEquals(2, uncommitted.batches().size())
              assertEquals(2L, uncommitted.nextGlobalOffset())
            }
          }
          verifyPending()
          move(new TopicPartition(topic, 0), ids(1), topicId)
          verifyPending()
          if (commitFirst) transaction.commitTransaction() else transaction.abortTransaction()
          awaitLso(2)
          brokers.foreach { broker =>
            TestUtils.retry(30000) {
              val firstPage = fetch(broker, maxBatches = 1)
              assertFalse(firstPage.transactionPending())
              assertEquals(1L, firstPage.nextGlobalOffset())
              assertEquals(if (commitFirst) 1 else 0, firstPage.batches().size())
              val nextPage = fetch(broker, start = firstPage.nextGlobalOffset())
              assertEquals(1, nextPage.batches().size())
              assertEquals(1L, nextPage.batches().get(0).globalBaseOffset())
              assertEquals(2L, nextPage.nextGlobalOffset())
            }
          }
          // Reuse the producer after abort/commit. Markers have physical offsets but no global offsets.
          transaction.beginTransaction()
          assertEquals(2L, transaction.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, null, Array[Byte](11)))
            .get(30, TimeUnit.SECONDS).offset())
          assertEquals(1L, regular.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, 1, null, Array[Byte](21)))
            .get(30, TimeUnit.SECONDS).offset())
          transaction.commitTransaction()
          awaitLso(4)
          move(new TopicPartition(topic, 0), ids.head, topicId)
          val indexImage = brokers.head.metadataCache.getImage().topics().getTopic(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)
          val indexPartition = brokers.head.globalSequenceCoordinator.partitionFor(topicId)
          val target = ids.find(_.intValue() != indexImage.partitions().get(indexPartition).leader).get
          move(new TopicPartition(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, indexPartition), target, indexImage.id())
          brokers.foreach { broker =>
            TestUtils.retry(30000) {
              val page = fetch(broker)
              val expected = if (commitFirst) Seq(0L, 1L, 2L, 3L) else Seq(1L, 2L, 3L)
              assertFalse(page.transactionPending())
              assertEquals(4L, page.committedGlobalEndOffset())
              assertEquals(4L, page.nextGlobalOffset())
              assertEquals(expected, page.batches().asScala.map(_.globalBaseOffset()).toSeq)
              page.batches().asScala.foreach { entry =>
                val global = entry.globalBaseOffset().toInt
                val batch = entry.records().asInstanceOf[MemoryRecords].batches().iterator().next()
                batch.ensureValid()
                assertEquals(Seq(0L, 0L, 2L, 1L)(global), batch.baseOffset())
                assertEquals(ByteBuffer.wrap(Array[Byte](Seq(10, 20, 11, 21)(global).toByte)), batch.iterator().next().value())
              }
              val fromLaterTransaction = fetch(broker, start = 2, end = 3)
              assertEquals(1, fromLaterTransaction.batches().size())
              assertEquals(2L, fromLaterTransaction.batches().get(0).globalBaseOffset())
              val ru = fetch(broker, committed = false)
              assertEquals(4, ru.batches().size())
              val firstProducer = ru.batches().get(0).records().asInstanceOf[MemoryRecords].batches().iterator().next().producerId()
              val laterProducer = ru.batches().get(2).records().asInstanceOf[MemoryRecords].batches().iterator().next().producerId()
              assertEquals(firstProducer, laterProducer)
            }
          }
        } finally {
          transaction.close()
          regular.close()
        }
      } finally admin.close()
    } finally cluster.close()
  }
}
