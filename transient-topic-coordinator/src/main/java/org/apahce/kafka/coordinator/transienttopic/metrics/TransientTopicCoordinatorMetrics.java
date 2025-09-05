package org.apahce.kafka.coordinator.transienttopic.metrics;

import com.yammer.metrics.core.MetricsRegistry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetricsShard;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apahce.kafka.coordinator.transienttopic.TransientTopicCoordinatorShard;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class TransientTopicCoordinatorMetrics extends CoordinatorMetrics implements AutoCloseable {

    public static final String METRICS_GROUP = "transient-topic-coordinator-metrics";

    private final MetricsRegistry registry;
    private final Metrics metrics;
    private final Map<TopicPartition, TransientTopicCoordinatorShard> shards = new ConcurrentHashMap<>();

    public final Map<String, Sensor> globalSensors;

    public TransientTopicCoordinatorMetrics(MetricsRegistry registry, Metrics metrics) {
        this.registry = Objects.requireNonNull(registry);
        this.metrics = Objects.requireNonNull(metrics);

        registerGauges();

        // TODO: Add sensors
        globalSensors = Collections.unmodifiableMap(Utils.mkMap());
    }

    private void registerGauges() {
        // TODO: Add Gauges to registry
        // ex) registry.newGauge()
    }

    @Override
    public void close() throws Exception {
        // TODO: Implement here, referring to GroupCoordinatorMetrics.java
    }

    @Override
    public TransientTopicCoordinatorMetricsShard newMetricsShard(SnapshotRegistry snapshotRegistry, TopicPartition tp) {
        // TODO: Implement here, referring to GroupCoordinatorMetrics.java
        return null;
    }

    @Override
    public void activateMetricsShard(CoordinatorMetricsShard shard) {
        // TODO: Implement here, referring to GroupCoordinatorMetrics.java
    }

    @Override
    public void deactivateMetricsShard(CoordinatorMetricsShard shard) {
        // TODO: Implement here, referring to GroupCoordinatorMetrics.java
    }

    @Override
    public MetricsRegistry registry() {
        // TODO: Implement here, referring to GroupCoordinatorMetrics.java
        return null;
    }

    @Override
    public void onUpdateLastCommittedOffset(TopicPartition tp, long offset) {
        // TODO: Implement here, referring to GroupCoordinatorMetrics.java
    }
}
