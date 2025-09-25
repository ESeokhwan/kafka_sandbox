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
package org.apache.kafka.common;

import java.util.Objects;

/**
 * This is used for transient topics that are not persisted in the Kafka cluster.
 * It contains the topic name, partition id, and the start position (offset) of the
 * transient topic.
 */
public class TransientTopicPartition {

    private final TopicPartition topicPartition;
    private final long offset;

    /**
     * Create an instance with the provided parameters.
     *
     * @param topicPartition the topic partition
     * @param offset the start position of the transient topic
     */
    public TransientTopicPartition(TopicPartition topicPartition, long offset) {
        this.topicPartition = Objects.requireNonNull(topicPartition, "topicPartition can not be null");
        this.offset = offset;
    }

    /**
     * Create an instance with the provided parameters.
     *
     * @param topic the topic name or null
     * @param partition the partition id
     * @param offset the start position of the transient topic
     */
    public TransientTopicPartition(String topic, int partition, long offset) {
        this.topicPartition = new TopicPartition(topic, partition);
        this.offset = offset;
    }

    /**
     * @return the topic name or null if it is unknown.
     */
    public String topic() {
        return topicPartition.topic();
    }

    /**
     * @return the partition id.
     */
    public int partition() {
        return topicPartition.partition();
    }

    /**
     * @return the start position of the transient topic.
     */
    public long offset() {
        return offset;
    }

    public TransientTopicPartition withOffset(long offset) {
        return new TransientTopicPartition(topicPartition, offset);
    }

    /**
     * @return Topic partition representing this instance.
     */
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransientTopicPartition that = (TransientTopicPartition) o;
        return topicPartition.equals(that.topicPartition) && offset == that.offset;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + (int) offset;
        result = prime * result + topicPartition.hashCode();
        return result;
    }
}
