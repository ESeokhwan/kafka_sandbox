package org.apache.kafka.coordinator.globalsequence;

public interface GlobalOffsetSequencer {

    void startup();

    long nextOffset();
}
