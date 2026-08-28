package org.apache.kafka.coordinator.globalsequence;

public class BasicGlobalOffsetSequencer implements GlobalOffsetSequencer {
    @Override
    public void startup() {

    }

    @Override
    public long nextOffset() {
        return 0;
    }
}
