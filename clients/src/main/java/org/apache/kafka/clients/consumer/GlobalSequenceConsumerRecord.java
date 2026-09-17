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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import java.util.Objects;
import java.util.Optional;

/**
 * A deserialized record together with its logical global offset and original
 * physical location.
 *
 * <p>The global offset belongs to the data topic UUID of the enclosing
 * {@link GlobalSequenceConsumerRecords} page. It is not a partition offset.
 * Filtering aborted transactions can leave gaps in global offsets. The original
 * physical location, timestamp and source leader epoch are preserved.</p>
 *
 * <p>Keys, values and headers are held by reference, as in {@link ConsumerRecord}.</p>
 */
public final class GlobalSequenceConsumerRecord<K, V> {
    private final String topic;
    private final long globalOffset;
    private final int physicalPartition;
    private final long physicalOffset;
    private final long timestamp;
    private final TimestampType timestampType;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final K key;
    private final V value;
    private final Headers headers;
    private final Optional<Integer> leaderEpoch;

    public GlobalSequenceConsumerRecord(
        String topic,
        long globalOffset,
        int physicalPartition,
        long physicalOffset,
        long timestamp,
        TimestampType timestampType,
        int serializedKeySize,
        int serializedValueSize,
        K key,
        V value,
        Headers headers,
        Optional<Integer> leaderEpoch
    ) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.globalOffset = globalOffset;
        this.physicalPartition = physicalPartition;
        this.physicalOffset = physicalOffset;
        this.timestamp = timestamp;
        this.timestampType = Objects.requireNonNull(timestampType, "timestampType");
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.key = key;
        this.value = value;
        this.headers = Objects.requireNonNull(headers, "headers");
        this.leaderEpoch = Objects.requireNonNull(leaderEpoch, "leaderEpoch");
    }

    public String topic() {
        return topic;
    }

    public long globalOffset() {
        return globalOffset;
    }

    /** The original data partition, not the index partition. */
    public int physicalPartition() {
        return physicalPartition;
    }

    /** The original offset within the physical data partition. */
    public long physicalOffset() {
        return physicalOffset;
    }

    public long timestamp() {
        return timestamp;
    }

    public TimestampType timestampType() {
        return timestampType;
    }

    public int serializedKeySize() {
        return serializedKeySize;
    }

    public int serializedValueSize() {
        return serializedValueSize;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }

    public Headers headers() {
        return headers;
    }

    /** The original data batch's partition leader epoch, not the coordinator leader epoch. */
    public Optional<Integer> leaderEpoch() {
        return leaderEpoch;
    }

    @Override
    public String toString() {
        return "GlobalSequenceConsumerRecord(" +
            "topic=" + topic +
            ", globalOffset=" + globalOffset +
            ", physicalPartition=" + physicalPartition +
            ", physicalOffset=" + physicalOffset +
            ", timestamp=" + timestamp +
            ", key=" + key +
            ", value=" + value +
            ')';
    }
}
