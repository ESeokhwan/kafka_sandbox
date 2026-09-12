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
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, ConfigEntry, NewPartitions, NewTopic}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{CoordinatorNotAvailableException, InvalidConfigurationException, InvalidPartitionsException}
import org.apache.kafka.common.internals.Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME
import org.apache.kafka.common.test.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorConfig
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PartitionKey
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
        broker.shutdown()
        assertFutureThrows(classOf[CoordinatorNotAvailableException], oldService.committedProgress(partition))
        broker.startup()
        cluster.waitForReadyBrokers()
        assertNotSame(oldService, broker.globalSequenceCoordinator)
        assertEquals(mappedPartition, broker.globalSequenceCoordinator.partitionFor(topicId))
        TestUtils.waitUntilTrue(() => Try(broker.globalSequenceCoordinator.committedProgress(partition).get(10, TimeUnit.SECONDS)).toOption.exists(_.isEmpty),
          "Global sequence shard did not reload after broker restart", 30000)
      } finally {
        admin.close()
      }
    } finally {
      cluster.close()
    }
  }
}
