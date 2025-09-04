package org.apahce.kafka.coordinator.transienttopic;

import org.apache.kafka.common.TransientTopic;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.CoordinatorRecord;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.group.runtime.CoordinatorShard;
import org.apache.kafka.coordinator.group.runtime.CoordinatorTimer;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.slf4j.Logger;

public class TransientTopicCoordinatorShard implements CoordinatorShard<CoordinatorRecord> {

    private final Logger log;

    private final TransientTopicIndexCache transientTopicIndexCache;

    private final Time time;

    private final CoordinatorTimer<Void, CoordinatorRecord> timer;

    private final CoordinatorMetrics coordinatorMetrics;

    private final CoordinatorMetricsShard metricsShard;

    TransientTopicCoordinatorShard(
        LogContext logContext,
        TransientTopicIndexCache transientTopicIndexCache,
        Time time,
        CoordinatorTimer<Void, CoordinatorRecord> timer,
        CoordinatorMetrics coordinatorMetrics,
        CoordinatorMetricsShard metricsShard
    ) {
        this.log = logContext.logger(TransientTopicCoordinatorShard.class);
        this.transientTopicIndexCache = transientTopicIndexCache;
        this.time = time;
        this.timer = timer;
        this.coordinatorMetrics = coordinatorMetrics;
        this.metricsShard = metricsShard;
    }

    public CoordinatorResult<TransientTopic, CoordinatorRecord> createNewTransientTopic(
        RequestContext context,
        Uuid topicId,
        String topicName
    ) {
        // TODO: Implement here
        return null;
    }

    @Override
    public void onLoaded(MetadataImage newImage) {
        // TODO: Implement here
        CoordinatorShard.super.onLoaded(newImage);
    }

    @Override
    public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        // TODO: Implement here
        CoordinatorShard.super.onNewMetadataImage(newImage, delta);
    }

    @Override
    public void onUnloaded() {
        // TODO: Implement here
        CoordinatorShard.super.onUnloaded();
    }

    @Override
    public void replay(long offset, long producerId, short producerEpoch, CoordinatorRecord record) throws RuntimeException {
        // TODO: Implement here
    }

    @Override
    public void replayEndTransactionMarker(long producerId, short producerEpoch, TransactionResult result) throws RuntimeException {
        // TODO: Implement here
        CoordinatorShard.super.replayEndTransactionMarker(producerId, producerEpoch, result);
    }
}
