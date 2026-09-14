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

package org.apache.kafka.coordinator.globalsequence;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GlobalSequenceCoordinatorConfigTest {
    private GlobalSequenceCoordinatorConfig config(Map<String, ?> overrides) {
        return new GlobalSequenceCoordinatorConfig(
            new AbstractConfig(GlobalSequenceCoordinatorConfig.CONFIG_DEF, overrides, false));
    }

    @Test
    void testDefaultsAndIndexRetention() {
        GlobalSequenceCoordinatorConfig config = config(Map.of());
        assertEquals(50, config.indexTopicNumPartitions());
        assertEquals(3, config.indexTopicReplicationFactor());
        assertEquals(2, config.indexTopicMinIsr());
        assertEquals(100 * 1024 * 1024, config.indexTopicSegmentBytes());
        assertEquals(1, config.numThreads());
        assertEquals(2, config.indexerNumThreads());
        assertEquals(1024 * 1024, config.indexerReadMaxBytes());
        assertEquals(5 * 1024 * 1024, config.loadBufferSize());
        assertEquals(5000, config.writeTimeoutMs());
        assertEquals(5, config.appendLingerMs());

        Properties props = config.indexTopicConfigs();
        assertEquals("delete", props.getProperty(TopicConfig.CLEANUP_POLICY_CONFIG));
        assertEquals("-1", props.getProperty(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals("-1", props.getProperty(TopicConfig.RETENTION_BYTES_CONFIG));
        assertEquals("false", props.getProperty(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG));
        assertEquals("false", props.getProperty(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG));
        assertEquals("2", props.getProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG));
    }

    @Test
    void testSingleBrokerOverrides() {
        GlobalSequenceCoordinatorConfig config = config(Map.of(
            GlobalSequenceCoordinatorConfig.INDEX_TOPIC_NUM_PARTITIONS_CONFIG, 1,
            GlobalSequenceCoordinatorConfig.INDEX_TOPIC_REPLICATION_FACTOR_CONFIG, (short) 1,
            GlobalSequenceCoordinatorConfig.INDEX_TOPIC_MIN_ISR_CONFIG, 1,
            GlobalSequenceCoordinatorConfig.INDEX_TOPIC_SEGMENT_BYTES_CONFIG, 1024 * 1024,
            GlobalSequenceCoordinatorConfig.NUM_THREADS_CONFIG, 2,
            GlobalSequenceCoordinatorConfig.LOAD_BUFFER_SIZE_CONFIG, 4096,
            GlobalSequenceCoordinatorConfig.WRITE_TIMEOUT_MS_CONFIG, 100,
            GlobalSequenceCoordinatorConfig.APPEND_LINGER_MS_CONFIG, 0,
            GlobalSequenceCoordinatorConfig.INDEXER_NUM_THREADS_CONFIG, 3,
            GlobalSequenceCoordinatorConfig.INDEXER_READ_MAX_BYTES_CONFIG, 1024
        ));
        assertEquals(1, config.indexTopicNumPartitions());
        assertEquals(1, config.indexTopicReplicationFactor());
        assertEquals(1, config.indexTopicMinIsr());
        assertEquals(2, config.numThreads());
        assertEquals(3, config.indexerNumThreads());
        assertEquals(1024, config.indexerReadMaxBytes());
        assertEquals(4096, config.loadBufferSize());
        assertEquals(100, config.writeTimeoutMs());
        assertEquals(0, config.appendLingerMs());
        assertEquals("1048576", config.indexTopicConfigs().getProperty(TopicConfig.SEGMENT_BYTES_CONFIG));
        assertEquals("1", config.indexTopicConfigs().getProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG));
    }

    @Test
    void testRejectInvalidBounds() {
        for (String name : GlobalSequenceCoordinatorConfig.CONFIG_DEF.names()) {
            assertThrows(ConfigException.class, () -> config(Map.of(name, -1)), name);
            if (!name.equals(GlobalSequenceCoordinatorConfig.APPEND_LINGER_MS_CONFIG)) {
                assertThrows(ConfigException.class, () -> config(Map.of(name, 0)), name);
            }
        }
        assertThrows(ConfigException.class, () -> config(Map.of(
            GlobalSequenceCoordinatorConfig.INDEX_TOPIC_MIN_ISR_CONFIG, 4)));
        assertThrows(ConfigException.class, () -> config(Map.of(
            GlobalSequenceCoordinatorConfig.INDEX_TOPIC_SEGMENT_BYTES_CONFIG, 1024)));
    }
}
