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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.SHORT;

/**
 * Static broker settings for the global sequence coordinator.
 * Topic activation is separately controlled by {@link TopicConfig#GLOBAL_SEQUENCE_ENABLED_CONFIG}.
 */
public class GlobalSequenceCoordinatorConfig {
    /** Topic-level overrides required to retain the complete index history. */
    public static final Map<String, String> REQUIRED_INDEX_TOPIC_CONFIGS = Map.of(
        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
        TopicConfig.RETENTION_MS_CONFIG, "-1",
        TopicConfig.RETENTION_BYTES_CONFIG, "-1",
        TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false"
    );
    public static final String INDEX_TOPIC_NUM_PARTITIONS_CONFIG = "global.sequence.coordinator.index.topic.num.partitions";
    public static final int INDEX_TOPIC_NUM_PARTITIONS_DEFAULT = 50;
    public static final String INDEX_TOPIC_NUM_PARTITIONS_DOC = "Number of index topic partitions. Must not change after the index topic is created.";

    public static final String INDEX_TOPIC_REPLICATION_FACTOR_CONFIG = "global.sequence.coordinator.index.topic.replication.factor";
    public static final short INDEX_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;
    public static final String INDEX_TOPIC_REPLICATION_FACTOR_DOC = "Replication factor of the index topic. Creation waits until enough brokers are available.";

    public static final String INDEX_TOPIC_MIN_ISR_CONFIG = "global.sequence.coordinator.index.topic.min.isr";
    public static final int INDEX_TOPIC_MIN_ISR_DEFAULT = 2;
    public static final String INDEX_TOPIC_MIN_ISR_DOC = "Minimum in-sync replicas for the index topic. Must not exceed its replication factor.";

    public static final String INDEX_TOPIC_SEGMENT_BYTES_CONFIG = "global.sequence.coordinator.index.topic.segment.bytes";
    public static final int INDEX_TOPIC_SEGMENT_BYTES_DEFAULT = 100 * 1024 * 1024;
    public static final String INDEX_TOPIC_SEGMENT_BYTES_DOC = "Index log segment size in bytes.";

    public static final String NUM_THREADS_CONFIG = "global.sequence.coordinator.threads";
    public static final int NUM_THREADS_DEFAULT = 1;
    public static final String NUM_THREADS_DOC = "Number of coordinator event processing threads.";

    public static final String INDEXER_NUM_THREADS_CONFIG = "global.sequence.indexer.num.threads";
    public static final int INDEXER_NUM_THREADS_DEFAULT = 2;
    public static final String INDEXER_NUM_THREADS_DOC = "Number of shared workers for source partition indexing and log reads.";

    public static final String INDEXER_READ_MAX_BYTES_CONFIG = "global.sequence.indexer.read.max.bytes";
    public static final int INDEXER_READ_MAX_BYTES_DEFAULT = 1024 * 1024;
    public static final String INDEXER_READ_MAX_BYTES_DOC = "Soft byte limit for each source log read. The first complete batch may exceed this limit.";

    public static final String LOAD_BUFFER_SIZE_CONFIG = "global.sequence.coordinator.load.buffer.size";
    public static final int LOAD_BUFFER_SIZE_DEFAULT = 5 * 1024 * 1024;
    public static final String LOAD_BUFFER_SIZE_DOC = "Soft limit in bytes for index log reads during coordinator loading.";

    public static final String WRITE_TIMEOUT_MS_CONFIG = "global.sequence.coordinator.write.timeout.ms";
    public static final int WRITE_TIMEOUT_MS_DEFAULT = 5000;
    public static final String WRITE_TIMEOUT_MS_DOC = "Time in milliseconds to wait for an index write to commit. Timeout does not cancel the write.";

    public static final String APPEND_LINGER_MS_CONFIG = "global.sequence.coordinator.append.linger.ms";
    public static final int APPEND_LINGER_MS_DEFAULT = 5;
    public static final String APPEND_LINGER_MS_DOC = "Time in milliseconds to accumulate coordinator writes before appending.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(INDEX_TOPIC_NUM_PARTITIONS_CONFIG, INT, INDEX_TOPIC_NUM_PARTITIONS_DEFAULT, atLeast(1), MEDIUM, INDEX_TOPIC_NUM_PARTITIONS_DOC)
        .define(INDEX_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, INDEX_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), MEDIUM, INDEX_TOPIC_REPLICATION_FACTOR_DOC)
        .define(INDEX_TOPIC_MIN_ISR_CONFIG, INT, INDEX_TOPIC_MIN_ISR_DEFAULT, atLeast(1), MEDIUM, INDEX_TOPIC_MIN_ISR_DOC)
        .define(INDEX_TOPIC_SEGMENT_BYTES_CONFIG, INT, INDEX_TOPIC_SEGMENT_BYTES_DEFAULT, atLeast(1024 * 1024), MEDIUM, INDEX_TOPIC_SEGMENT_BYTES_DOC)
        .define(NUM_THREADS_CONFIG, INT, NUM_THREADS_DEFAULT, atLeast(1), MEDIUM, NUM_THREADS_DOC)
        .define(INDEXER_NUM_THREADS_CONFIG, INT, INDEXER_NUM_THREADS_DEFAULT, atLeast(1), MEDIUM, INDEXER_NUM_THREADS_DOC)
        .define(INDEXER_READ_MAX_BYTES_CONFIG, INT, INDEXER_READ_MAX_BYTES_DEFAULT, atLeast(1), MEDIUM, INDEXER_READ_MAX_BYTES_DOC)
        .define(LOAD_BUFFER_SIZE_CONFIG, INT, LOAD_BUFFER_SIZE_DEFAULT, atLeast(1), MEDIUM, LOAD_BUFFER_SIZE_DOC)
        .define(WRITE_TIMEOUT_MS_CONFIG, INT, WRITE_TIMEOUT_MS_DEFAULT, atLeast(1), MEDIUM, WRITE_TIMEOUT_MS_DOC)
        .define(APPEND_LINGER_MS_CONFIG, INT, APPEND_LINGER_MS_DEFAULT, atLeast(0), MEDIUM, APPEND_LINGER_MS_DOC);

    private final AbstractConfig config;

    public GlobalSequenceCoordinatorConfig(AbstractConfig config) {
        this.config = Objects.requireNonNull(config);
        if (indexTopicMinIsr() > indexTopicReplicationFactor()) {
            throw new ConfigException(INDEX_TOPIC_MIN_ISR_CONFIG, indexTopicMinIsr(),
                "Must not exceed " + INDEX_TOPIC_REPLICATION_FACTOR_CONFIG);
        }
    }

    public int indexTopicNumPartitions() {
        return config.getInt(INDEX_TOPIC_NUM_PARTITIONS_CONFIG);
    }

    public short indexTopicReplicationFactor() {
        return config.getShort(INDEX_TOPIC_REPLICATION_FACTOR_CONFIG);
    }

    public int indexTopicMinIsr() {
        return config.getInt(INDEX_TOPIC_MIN_ISR_CONFIG);
    }

    public int indexTopicSegmentBytes() {
        return config.getInt(INDEX_TOPIC_SEGMENT_BYTES_CONFIG);
    }

    public int numThreads() {
        return config.getInt(NUM_THREADS_CONFIG);
    }

    public int loadBufferSize() {
        return config.getInt(LOAD_BUFFER_SIZE_CONFIG);
    }

    public int indexerNumThreads() {
        return config.getInt(INDEXER_NUM_THREADS_CONFIG);
    }

    public int indexerReadMaxBytes() {
        return config.getInt(INDEXER_READ_MAX_BYTES_CONFIG);
    }

    public int writeTimeoutMs() {
        return config.getInt(WRITE_TIMEOUT_MS_CONFIG);
    }

    public int appendLingerMs() {
        return config.getInt(APPEND_LINGER_MS_CONFIG);
    }

    /**
     * Preserve all index history until a checkpoint and index GC protocol exists.
     */
    public Properties indexTopicConfigs() {
        Properties props = new Properties();
        props.putAll(REQUIRED_INDEX_TOPIC_CONFIGS);
        props.setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(indexTopicMinIsr()));
        props.setProperty(TopicConfig.SEGMENT_BYTES_CONFIG, Integer.toString(indexTopicSegmentBytes()));
        props.setProperty(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "false");
        return props;
    }
}
