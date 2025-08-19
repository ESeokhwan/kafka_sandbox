package org.apache.kafka.common;

import java.io.Serializable;

/**
 * This class represents a transient topic in Kafka, which is not persisted in the cluster.
 * It contains the topic's unique identifier, name, partition information, and timestamps
 * for creation and last usage.
 */
public class TransientTopic implements Serializable {
    private int hash = 0;

    private final Uuid id;
    private final String name;
    private final TransientTopicPartition partition;
    private final long createdAt;
    private final long lastUsedAt;

    TransientTopic(Uuid id, String name, TransientTopicPartition partition, long createdAt, long lastUsedAt) {
        this.id = id;
        this.name = name;
        this.partition = partition;
        this.createdAt = createdAt;
        this.lastUsedAt = lastUsedAt;
    }

    public TransientTopic(String name, TransientTopicPartition partition) {
        this(Uuid.randomUuid(), name, partition, System.currentTimeMillis(), System.currentTimeMillis());
    }

    /**
     * @return the UUID of the transient topic
     */
    public Uuid id() {
        return id;
    }

    /**
     * @return the name of the transient topic
     */
    public String name() {
        return name;
    }

    /**
     * @return the partition information of the transient topic
     */
    public TransientTopicPartition partition() {
        return partition;
    }

    /**
     * @return the timestamp when the transient topic was created
     */
    public long createdAt() {
        return createdAt;
    }

    /**
     * @return the timestamp when the transient topic was last used
     */
    public long lastUsedAt() {
        return lastUsedAt;
    }

    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        final int prime = 31;
        int result = prime + id.hashCode();
        result = prime * result + name.hashCode();
        result = prime * result + partition.hashCode();
        result = prime * result + Long.hashCode(createdAt);
        result = prime * result + Long.hashCode(lastUsedAt);
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TransientTopic that = (TransientTopic) obj;
        return id.equals(that.id) &&
               name.equals(that.name) &&
               partition.equals(that.partition) &&
               createdAt == that.createdAt &&
               lastUsedAt == that.lastUsedAt;
    }
}
