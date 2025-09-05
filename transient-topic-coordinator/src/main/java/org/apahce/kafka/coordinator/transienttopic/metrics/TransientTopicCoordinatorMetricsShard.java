package org.apahce.kafka.coordinator.transienttopic.metrics;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetricsShard;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.util.Map;
import java.util.Objects;

public class TransientTopicCoordinatorMetricsShard implements CoordinatorMetricsShard {

    private final Map<String, Sensor> globalSensors;

    private final TopicPartition topicPartition;

    public TransientTopicCoordinatorMetricsShard(
        SnapshotRegistry snapshotRegistry,
        Map<String, Sensor> globalSensors,
        TopicPartition topicPartition
    ) {
        Objects.requireNonNull(snapshotRegistry);

        // TODO: Implement here, referring to GroupCoordinatorMetricsShard.java

        this.globalSensors = Objects.requireNonNull(globalSensors);
        this.topicPartition = Objects.requireNonNull(topicPartition);
    }

    @Override
    public void record(String sensorName) {
        // TODO: Implement here, referring to GroupCoordinatorMetricsShard.java
    }

    @Override
    public void record(String sensorName, double val) {
        // TODO: Implement here, referring to GroupCoordinatorMetricsShard.java
    }

    @Override
    public TopicPartition topicPartition() {
        // TODO: Implement here, referring to GroupCoordinatorMetricsShard.java
        return null;
    }

    @Override
    public void commitUpTo(long offset) {
        // TODO: Implement here, referring to GroupCoordinatorMetricsShard.java
    }
}
