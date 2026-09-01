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

import kafka.server.IntegrationTestUtils.connectAndReceive
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.LookupGlobalSequenceIndexRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{LookupGlobalSequenceIndexRequest, LookupGlobalSequenceIndexResponse}
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, Type}
import org.apache.kafka.common.test.{ClusterInstance, TestUtils}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorConfig
import org.junit.jupiter.api.Assertions.assertEquals

import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

class GlobalSequenceRoutingIntegrationTest {

  @ClusterTest(
    types = Array(Type.KRAFT),
    brokers = 2,
    controllers = 1,
    serverProperties = Array(
      new ClusterConfigProperty(
        key = GlobalSequenceCoordinatorConfig.NUM_INDEX_PARTITIONS_CONFIG,
        value = "1"
      ),
      new ClusterConfigProperty(
        key = GlobalSequenceCoordinatorConfig.INDEX_TOPIC_REPLICATION_FACTOR_CONFIG,
        value = "2"
      )
    )
  )
  def testProduceAutoCreatesGlobalSequenceIndexTopic(cluster: ClusterInstance): Unit = {
    val dataTopic = "global-sequence-auto-create"
    val dataTopicDefinition = new NewTopic(dataTopic, 1, 2.toShort)
      .configs(util.Map.of(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "true"))

    val admin = cluster.admin()
    try {
      admin.createTopics(util.List.of(dataTopicDefinition)).all().get(30, TimeUnit.SECONDS)
      cluster.waitForTopic(dataTopic, 1)
    } finally {
      admin.close()
    }

    val producer = cluster.producer[Array[Byte], Array[Byte]]()
    try {
      producer.send(new ProducerRecord(
        dataTopic,
        0,
        null,
        "first".getBytes(StandardCharsets.UTF_8)
      )).get(30, TimeUnit.SECONDS)
    } finally {
      producer.close()
    }

    cluster.waitForTopic(Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, 1)

    val verificationAdmin = cluster.admin()
    try {
      val description = verificationAdmin.describeTopics(
        util.List.of(Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)
      ).allTopicNames().get(30, TimeUnit.SECONDS).get(Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)
      assertEquals(1, description.partitions().size())
      assertEquals(2, description.partitions().get(0).replicas().size())

      val resource = new ConfigResource(ConfigResource.Type.TOPIC, Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)
      val topicConfig = verificationAdmin.describeConfigs(util.List.of(resource))
        .all()
        .get(30, TimeUnit.SECONDS)
        .get(resource)
      assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, topicConfig.get(TopicConfig.CLEANUP_POLICY_CONFIG).value())
    } finally {
      verificationAdmin.close()
    }
  }

  @ClusterTest(
    types = Array(Type.KRAFT),
    brokers = 2,
    controllers = 1,
    serverProperties = Array(
      new ClusterConfigProperty(
        key = GlobalSequenceCoordinatorConfig.NUM_INDEX_PARTITIONS_CONFIG,
        value = "1"
      )
    )
  )
  def testProduceRoutesIndexAppendToRemoteLeader(cluster: ClusterInstance): Unit = {
    val brokerIds = cluster.brokerIds.asScala.toSeq.sorted
    val dataLeaderId = brokerIds.head
    val indexLeaderId = brokerIds.last
    val dataTopic = "global-sequence-routing"
    val dataPartition = 0
    val indexPartition = 0

    val dataAssignments = util.Map.of(
      Integer.valueOf(dataPartition),
      util.List.of(Integer.valueOf(dataLeaderId))
    )
    val indexAssignments = util.Map.of(
      Integer.valueOf(indexPartition),
      util.List.of(Integer.valueOf(indexLeaderId))
    )
    val dataTopicDefinition = new NewTopic(dataTopic, dataAssignments)
      .configs(util.Map.of(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "true"))
    val indexTopicDefinition = new NewTopic(Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, indexAssignments)
      .configs(util.Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT))

    val admin = cluster.admin()
    val dataTopicId = try {
      admin.createTopics(util.List.of(dataTopicDefinition, indexTopicDefinition))
        .all()
        .get(30, TimeUnit.SECONDS)
      cluster.waitForTopic(dataTopic, 1)
      cluster.waitForTopic(Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, 1)
      admin.describeTopics(util.List.of(dataTopic))
        .allTopicNames()
        .get(30, TimeUnit.SECONDS)
        .get(dataTopic)
        .topicId()
    } finally {
      admin.close()
    }

    val producer = cluster.producer[Array[Byte], Array[Byte]]()
    try {
      val first = producer.send(new ProducerRecord(
        dataTopic,
        dataPartition,
        null,
        "first".getBytes(StandardCharsets.UTF_8)
      )).get(30, TimeUnit.SECONDS)
      val second = producer.send(new ProducerRecord(
        dataTopic,
        dataPartition,
        null,
        "second".getBytes(StandardCharsets.UTF_8)
      )).get(30, TimeUnit.SECONDS)

      assertEquals(0L, first.offset)
      assertEquals(1L, second.offset)
    } finally {
      producer.close()
    }

    val indexTopicPartition = new TopicPartition(Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, indexPartition)
    val indexLeader = cluster.brokers().get(indexLeaderId)
    TestUtils.waitForCondition(
      () => indexLeader.replicaManager.localLog(indexTopicPartition).exists(_.logEndOffset >= 2L),
      "The remote index leader did not append both global sequence allocations"
    )

    val lookupRequest = new LookupGlobalSequenceIndexRequest.Builder(
      new LookupGlobalSequenceIndexRequestData()
        .setTopicId(dataTopicId)
        .setGlobalStartOffset(0L)
        .setGlobalEndOffsetExclusive(2L)
    ).build()
    val lookupResponse = connectAndReceive[LookupGlobalSequenceIndexResponse](
      lookupRequest,
      cluster.brokers().get(dataLeaderId).socketServer,
      cluster.clientListener()
    )

    assertEquals(Errors.NONE.code, lookupResponse.data.errorCode)
    assertEquals(2, lookupResponse.data.indexEntries.size)
    assertEquals(0L, lookupResponse.data.indexEntries.get(0).globalBaseOffset)
    assertEquals(1, lookupResponse.data.indexEntries.get(0).recordCount)
    assertEquals(dataPartition, lookupResponse.data.indexEntries.get(0).physicalPartition)
    assertEquals(0L, lookupResponse.data.indexEntries.get(0).physicalBaseOffset)
    assertEquals(1L, lookupResponse.data.indexEntries.get(1).globalBaseOffset)
    assertEquals(1L, lookupResponse.data.indexEntries.get(1).physicalBaseOffset)
  }
}
