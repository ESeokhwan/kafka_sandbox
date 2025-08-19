package org.apache.kafka.common;

import java.util.Objects;

/**
 * This is used for transient topics that are not persisted in the Kafka cluster.
 * It contains the topic name, partition id, and the start position (offset) of the
 * transient topic.
 */
public class TransientTopicPartition {

    private final TopicPartition topicPartition;
    private final int offset;

    /**
     * Create an instance with the provided parameters.
     *
     * @param topicPartition the topic partition
     * @param offset the start position of the transient topic
     */
    public TransientTopicPartition(TopicPartition topicPartition, int offset) {
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
    public TransientTopicPartition(String topic, int partition, int offset) {
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
    public int offset() {
        return offset;
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
        int result = prime + offset;
        result = prime * result + topicPartition.hashCode();
        return result;
    }
}
