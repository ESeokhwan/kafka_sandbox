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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * The requested topic name resolved to a different UUID than the one associated
 * with a global cursor. That cursor must not be retried against the new UUID.
 *
 * <p>This is a non-retriable client identity error, not a broker protocol error.
 * An unresolved topic is a metadata failure rather than a UUID mismatch.</p>
 */
public class GlobalSequenceTopicIdMismatchException extends KafkaException {
    private static final long serialVersionUID = 1L;

    private final String topic;
    // Kafka's Uuid is not Serializable. Keep its bits so exception serialization retains the identity.
    private final long expectedMostSignificantBits;
    private final long expectedLeastSignificantBits;
    private final long actualMostSignificantBits;
    private final long actualLeastSignificantBits;

    /**
     * @param topic the topic name
     * @param expectedTopicId the nonzero UUID associated with the requested cursor
     * @param actualTopicId the different, nonzero UUID resolved from metadata
     * @throws NullPointerException if any argument is null
     * @throws IllegalArgumentException if either UUID is zero or both UUIDs are equal
     */
    public GlobalSequenceTopicIdMismatchException(String topic, Uuid expectedTopicId, Uuid actualTopicId) {
        super("Global sequence topic " + topic + " has UUID " + actualTopicId + "; expected " + expectedTopicId);
        this.topic = Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(expectedTopicId, "expectedTopicId");
        Objects.requireNonNull(actualTopicId, "actualTopicId");
        if (Uuid.ZERO_UUID.equals(expectedTopicId) || Uuid.ZERO_UUID.equals(actualTopicId) || expectedTopicId.equals(actualTopicId))
            throw new IllegalArgumentException("Expected and actual topic UUIDs must be nonzero and different");
        this.expectedMostSignificantBits = expectedTopicId.getMostSignificantBits();
        this.expectedLeastSignificantBits = expectedTopicId.getLeastSignificantBits();
        this.actualMostSignificantBits = actualTopicId.getMostSignificantBits();
        this.actualLeastSignificantBits = actualTopicId.getLeastSignificantBits();
    }

    public String topic() {
        return topic;
    }

    public Uuid expectedTopicId() {
        return new Uuid(expectedMostSignificantBits, expectedLeastSignificantBits);
    }

    public Uuid actualTopicId() {
        return new Uuid(actualMostSignificantBits, actualLeastSignificantBits);
    }
}
