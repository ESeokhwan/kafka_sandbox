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

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.internals.Topic
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
    try {
      admin.createTopics(util.List.of(dataTopicDefinition, indexTopicDefinition))
        .all()
        .get(30, TimeUnit.SECONDS)
      cluster.waitForTopic(dataTopic, 1)
      cluster.waitForTopic(Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, 1)
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
  }
}
