package org.apache.kafka.coordinator.globalsequence;

import org.apache.kafka.common.Uuid;

import java.util.Objects;

public class GlobalSequenceIndexRecord {
    private final Uuid topicId;
    private final long globalOffset;
    private final int numRecords;
    private final int partition;
    private final long partitionOffset;

    private final long producerId;
    private final short producerEpoch;
    private final int baseSequence;

    public GlobalSequenceIndexRecord(Uuid topicId, long globalOffset, int numRecords, int partition, long partitionOffset, long producerId, short producerEpoch, int baseSequence) {
        this.topicId = Objects.requireNonNull(topicId);
        this.globalOffset = globalOffset;
        this.numRecords = numRecords;
        this.partition = partition;
        this.partitionOffset = partitionOffset;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
    }

    public Uuid topicId() {
        return topicId;
    }

    public long globalOffset() {
        return globalOffset;
    }

    public int numRecords() {
        return numRecords;
    }

    public int partition() {
        return partition;
    }

    public long partitionOffset() {
        return partitionOffset;
    }

    public long producerId() {
        return producerId;
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public int baseSequence() {
        return baseSequence;
    }

    public GlobalSequenceIndexRecord withGlobalOffset(long globalOffset) {
        return new GlobalSequenceIndexRecord(this.topicId, globalOffset, this.numRecords, this.partition, this.partitionOffset, this.producerId, this.producerEpoch, this.baseSequence);
    }
}
