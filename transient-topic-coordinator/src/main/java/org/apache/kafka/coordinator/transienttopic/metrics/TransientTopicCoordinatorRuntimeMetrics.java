package org.apache.kafka.coordinator.transienttopic.metrics;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.coordinator.group.metrics.CoordinatorRuntimeMetrics;
import org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime;

import java.util.Objects;
import java.util.function.Supplier;

public class TransientTopicCoordinatorRuntimeMetrics implements CoordinatorRuntimeMetrics {

    private final Metrics metrics;

    public TransientTopicCoordinatorRuntimeMetrics(Metrics metrics) {
        this.metrics = Objects.requireNonNull(metrics);
    }

    @Override
    public void recordPartitionStateChange(CoordinatorRuntime.CoordinatorState oldState, CoordinatorRuntime.CoordinatorState newState) {
        // TODO: Implement here, referring to GroupCoordinatorRuntimeMetrics.java
    }

    @Override
    public void recordPartitionLoadSensor(long startTimeMs, long endTimeMs) {
        // TODO: Implement here, referring to GroupCoordinatorRuntimeMetrics.java
    }

    @Override
    public void recordEventQueueTime(long durationMs) {
        // TODO: Implement here, referring to GroupCoordinatorRuntimeMetrics.java
    }

    @Override
    public void recordEventQueueProcessingTime(long durationMs) {
        // TODO: Implement here, referring to GroupCoordinatorRuntimeMetrics.java
    }

    @Override
    public void recordThreadIdleTime(long idleTimeMs) {
        // TODO: Implement here, referring to GroupCoordinatorRuntimeMetrics.java
    }

    @Override
    public void registerEventQueueSizeGauge(Supplier<Integer> sizeSupplier) {
        // TODO: Implement here, referring to GroupCoordinatorRuntimeMetrics.java
    }

    @Override
    public void close() throws Exception {
        // TODO: Implement here, referring to GroupCoordinatorRuntimeMetrics.java
    }
}
