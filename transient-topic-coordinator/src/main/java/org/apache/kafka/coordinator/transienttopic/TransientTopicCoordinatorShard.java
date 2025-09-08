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
package org.apache.kafka.coordinator.transienttopic;

import org.apache.kafka.common.TopicPartition;
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
import org.apache.kafka.coordinator.group.runtime.CoordinatorShardBuilder;
import org.apache.kafka.coordinator.group.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.transienttopic.metrics.TransientTopicCoordinatorMetrics;
import org.apache.kafka.coordinator.transienttopic.metrics.TransientTopicCoordinatorMetricsShard;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.slf4j.Logger;

public class TransientTopicCoordinatorShard implements CoordinatorShard<CoordinatorRecord> {

    public static class Builder implements CoordinatorShardBuilder<TransientTopicCoordinatorShard, CoordinatorRecord> {

        private final TransientTopicCoordinatorConfig config;
        private LogContext logContext;
        private SnapshotRegistry snapshotRegistry;
        private Time time;
        private CoordinatorTimer<Void, CoordinatorRecord> timer;
        private CoordinatorMetrics coordinatorMetrics;
        private TopicPartition topicPartition;

        public Builder(
            TransientTopicCoordinatorConfig config
        ) {
            this.config = config;
        }

        @Override
        public CoordinatorShardBuilder<TransientTopicCoordinatorShard, CoordinatorRecord> withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<TransientTopicCoordinatorShard, CoordinatorRecord> withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<TransientTopicCoordinatorShard, CoordinatorRecord> withTime(Time time) {
            this.time = time;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<TransientTopicCoordinatorShard, CoordinatorRecord> withTimer(CoordinatorTimer<Void, CoordinatorRecord> timer) {
            this.timer = timer;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<TransientTopicCoordinatorShard, CoordinatorRecord> withCoordinatorMetrics(CoordinatorMetrics coordinatorMetrics) {
            this.coordinatorMetrics = coordinatorMetrics;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<TransientTopicCoordinatorShard, CoordinatorRecord> withTopicPartition(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
            return this;
        }

        @Override
        public TransientTopicCoordinatorShard build() {
            if (logContext == null) logContext = new LogContext();
            if (config == null)
                throw new IllegalArgumentException("Config must be set.");
            if (snapshotRegistry == null)
                throw new IllegalArgumentException("SnapshotRegistry must be set.");
            if (time == null)
                throw new IllegalArgumentException("Time must be set.");
            if (timer == null)
                throw new IllegalArgumentException("Timer must be set.");
            if (coordinatorMetrics == null || !(coordinatorMetrics instanceof TransientTopicCoordinatorMetrics))
                throw new IllegalArgumentException("CoordinatorMetrics must be set and be of type TransientTopicCoordinatorMetrics.");
            if (topicPartition == null)
                throw new IllegalArgumentException("TopicPartition must be set.");

            TransientTopicCoordinatorMetricsShard metricsShard = ((TransientTopicCoordinatorMetrics) coordinatorMetrics)
                .newMetricsShard(snapshotRegistry, topicPartition);

            TransientTopicIndexCache indexCache = new TransientTopicIndexCache.Builder()
                .build();
            TransientTopicPartitionPool partitionPool = new TransientTopicPartitionPool.Builder()
                .build();

            return new TransientTopicCoordinatorShard(
                logContext,
                indexCache,
                partitionPool,
                time,
                timer,
                config,
                coordinatorMetrics,
                metricsShard
            );
        }
    }

    static final String TRANSIENT_TOPIC_EXPIRATION_KEY = "expire-transient-topic-metadata";

    private final Logger log;

    private final TransientTopicIndexCache indexCache;

    private final TransientTopicPartitionPool partitionPool;

    private final Time time;

    private final CoordinatorTimer<Void, CoordinatorRecord> timer;

    private final TransientTopicCoordinatorConfig config;

    private final CoordinatorMetrics coordinatorMetrics;

    private final CoordinatorMetricsShard metricsShard;

    TransientTopicCoordinatorShard(
        LogContext logContext,
        TransientTopicIndexCache indexCache,
        TransientTopicPartitionPool partitionPool,
        Time time,
        CoordinatorTimer<Void, CoordinatorRecord> timer,
        TransientTopicCoordinatorConfig config,
        CoordinatorMetrics coordinatorMetrics,
        CoordinatorMetricsShard metricsShard
    ) {
        this.log = logContext.logger(TransientTopicCoordinatorShard.class);
        this.indexCache = indexCache;
        this.partitionPool = partitionPool;
        this.time = time;
        this.timer = timer;
        this.config = config;
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
        coordinatorMetrics.activateMetricsShard(metricsShard);
    }

    @Override
    public void onUnloaded() {
        timer.cancel(TRANSIENT_TOPIC_EXPIRATION_KEY);
        coordinatorMetrics.deactivateMetricsShard(metricsShard);
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
