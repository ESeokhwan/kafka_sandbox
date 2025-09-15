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
package org.apache.kafka.coordinator.transienttopic;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.record.CompressionType;

import java.util.Optional;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.SHORT;
import static org.apache.kafka.common.utils.Utils.require;

public class TransientTopicCoordinatorConfig {

    public static final String COMMIT_TIMEOUT_MS_CONFIG = "transienttopic.commit.timeout.ms";
    public static final int COMMIT_TIMEOUT_MS_DEFAULT = 5000;
    public static final String COMMIT_TIMEOUT_MS_DOC = "Index of transient topic commit will be delayed until all replicas " +
        "for the index topic receive the commit or this timeout is reached. This is similar to producer request timeout.";

    public static final String INDEX_TOPIC_SEGMENT_BYTES_CONFIG = "transienttopic.index.log.segment.bytes";
    public static final int INDEX_TOPIC_SEGMENT_BYTES_DEFAULT = 100 * 1024 * 1024;
    public static final String INDEX_TOPIC_SEGMENT_BYTES_DOC = "The segment bytes of transient topic's index topic. The index " +
        "topic segment bytes should be kept relatively small in order to facilitate faster log compaction and cache loads.";

    public static final String INDEX_TOPIC_REPLICATION_FACTOR_CONFIG = "transienttopic.index.log.replication.factor";
    public static final short INDEX_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;
    public static final String INDEX_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor for the transient topic's index topic " +
        "(set higher to ensure availability). " + "Internal topic creation will fail until the cluster size meets this replication factor requirement.";

    public static final String INDEX_TOPIC_MIN_ISR_CONFIG = "transienttopic.index.log.min.isr";
    public static final int INDEX_TOPIC_MIN_ISR_DEFAULT = 2;
    public static final String INDEX_TOPIC_MIN_ISR_DOC = "The minimum number of replicas that must acknowledge a write to transaction topic " +
        "in order to be considered successful.";

    public static final String INDEX_TOPIC_COMPRESSION_CODEC_CONFIG = "transienttopic.index.compression.codec";
    public static final CompressionType INDEX_TOPIC_COMPRESSION_CODEC_DEFAULT = CompressionType.NONE;
    public static final String INDEX_TOPIC_COMPRESSION_CODEC_DOC = "Compression codec for the transient topic index.";

    public static final String TRANSIENT_TOPIC_INIT_PARTITIONS_CONFIG = "transienttopic.log.num.partitions";
    public static final int TRANSIENT_TOPIC_INIT_PARTITIONS_DEFAULT = 50;
    public static final String TRANSIENT_TOPIC_INIT_PARTITIONS_DOC = "The initial number of partitions for the transient topic.";

    public static final String TRANSIENT_TOPIC_SEGMENT_BYTES_CONFIG = "transienttopic.log.segment.bytes";
    public static final int TRANSIENT_TOPIC_SEGMENT_BYTES_DEFAULT = 1024 * 1024 * 1024;
    public static final String TRANSIENT_TOPIC_SEGMENT_BYTES_DOC = "The segment bytes of transient topic.";

    public static final String TRANSIENT_TOPIC_REPLICATION_FACTOR_CONFIG = "transienttopic.log.replication.factor";
    public static final short TRANSIENT_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;
    public static final String TRANSIENT_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor for the transient topic " +
        "(set higher to ensure availability). Internal topic creation will fail until the cluster size meets this replication factor requirement.";

    public static final String TRANSIENT_TOPIC_MIN_ISR_CONFIG = "transienttopic.log.min.isr";
    public static final int TRANSIENT_TOPIC_MIN_ISR_DEFAULT = 2;
    public static final String TRANSIENT_TOPIC_MIN_ISR_DOC = "The minimum number of replicas that must acknowledge a write " +
        "to transaction topic in order to be considered successful.";

    public static final ConfigDef TRANSIENT_TOPIC_COORDINATOR_CONFIG_DEF = new ConfigDef()
        .define(COMMIT_TIMEOUT_MS_CONFIG, INT, COMMIT_TIMEOUT_MS_DEFAULT, atLeast(1), HIGH, COMMIT_TIMEOUT_MS_DOC)
        .define(INDEX_TOPIC_SEGMENT_BYTES_CONFIG, INT, INDEX_TOPIC_SEGMENT_BYTES_DEFAULT, atLeast(1), HIGH, INDEX_TOPIC_SEGMENT_BYTES_DOC)
        .define(INDEX_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, INDEX_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), HIGH, INDEX_TOPIC_REPLICATION_FACTOR_DOC)
        .define(INDEX_TOPIC_MIN_ISR_CONFIG, INT, INDEX_TOPIC_MIN_ISR_DEFAULT, atLeast(1), HIGH, INDEX_TOPIC_MIN_ISR_DOC)
        .define(INDEX_TOPIC_COMPRESSION_CODEC_CONFIG, INT, (int) INDEX_TOPIC_COMPRESSION_CODEC_DEFAULT.id, HIGH, INDEX_TOPIC_COMPRESSION_CODEC_DOC)
        .define(TRANSIENT_TOPIC_INIT_PARTITIONS_CONFIG, INT, TRANSIENT_TOPIC_INIT_PARTITIONS_DEFAULT, atLeast(1), HIGH, TRANSIENT_TOPIC_INIT_PARTITIONS_DOC)
        .define(TRANSIENT_TOPIC_SEGMENT_BYTES_CONFIG, INT, TRANSIENT_TOPIC_SEGMENT_BYTES_DEFAULT, atLeast(1), HIGH, TRANSIENT_TOPIC_SEGMENT_BYTES_DOC)
        .define(TRANSIENT_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, TRANSIENT_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), HIGH, TRANSIENT_TOPIC_REPLICATION_FACTOR_DOC)
        .define(TRANSIENT_TOPIC_MIN_ISR_CONFIG, INT, TRANSIENT_TOPIC_MIN_ISR_DEFAULT, atLeast(1), HIGH, TRANSIENT_TOPIC_MIN_ISR_DOC);

    private final int commitTimeoutMs;
    private final CompressionType indexTopicCompressionType;
    private final int indexTopicSegmentBytes;
    private final short indexTopicReplicationFactor;
    private final int indexTopicMinIsr;
    private final int transientTopicInitPartitions;
    private final int transientTopicSegmentBytes;
    private final short transientTopicReplicationFactor;
    private final int transientTopicMinIsr;

    public TransientTopicCoordinatorConfig(AbstractConfig config) {
        this.commitTimeoutMs = config.getInt(COMMIT_TIMEOUT_MS_CONFIG);
        this.indexTopicCompressionType = Optional.ofNullable(config.getInt(INDEX_TOPIC_COMPRESSION_CODEC_CONFIG))
            .map(CompressionType::forId)
            .orElse(null);
        this.indexTopicSegmentBytes = config.getInt(INDEX_TOPIC_SEGMENT_BYTES_CONFIG);
        this.indexTopicReplicationFactor = config.getShort(INDEX_TOPIC_REPLICATION_FACTOR_CONFIG);
        this.indexTopicMinIsr = config.getInt(INDEX_TOPIC_MIN_ISR_CONFIG);
        this.transientTopicInitPartitions = config.getInt(TRANSIENT_TOPIC_INIT_PARTITIONS_CONFIG);
        this.transientTopicSegmentBytes = config.getInt(TRANSIENT_TOPIC_SEGMENT_BYTES_CONFIG);
        this.transientTopicReplicationFactor = config.getShort(TRANSIENT_TOPIC_REPLICATION_FACTOR_CONFIG);
        this.transientTopicMinIsr = config.getInt(TRANSIENT_TOPIC_MIN_ISR_CONFIG);

        require(commitTimeoutMs >= 1,
                String.format("%s must be greater or equal to 1", COMMIT_TIMEOUT_MS_CONFIG));
    }

    public int commitTimeoutMs() {
        return commitTimeoutMs;
    }

    public CompressionType indexTopicCompressionType() {
        return indexTopicCompressionType;
    }

    public int indexTopicSegmentBytes() {
        return indexTopicSegmentBytes;
    }

    public short indexTopicReplicationFactor() {
        return indexTopicReplicationFactor;
    }

    public int indexTopicMinIsr() {
        return indexTopicMinIsr;
    }

    public int transientTopicInitPartitions() {
        return transientTopicInitPartitions;
    }

    public int transientTopicSegmentBytes() {
        return transientTopicSegmentBytes;
    }

    public short transientTopicReplicationFactor() {
        return transientTopicReplicationFactor;
    }

    public int transientTopicMinIsr() {
        return transientTopicMinIsr;
    }
}
