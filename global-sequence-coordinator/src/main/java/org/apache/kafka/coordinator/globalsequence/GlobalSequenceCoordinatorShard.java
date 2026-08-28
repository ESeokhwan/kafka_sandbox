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
package org.apache.kafka.coordinator.globalsequence;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShardBuilder;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.globalsequence.generated.GlobalSequenceIndexLogKey;
import org.apache.kafka.coordinator.globalsequence.generated.GlobalSequenceIndexLogValue;
import org.apache.kafka.coordinator.globalsequence.metrics.GlobalSequenceCoordinatorMetrics;
import org.apache.kafka.coordinator.globalsequence.metrics.GlobalSequenceCoordinatorMetricsShard;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class GlobalSequenceCoordinatorShard implements CoordinatorShard<CoordinatorRecord> {

    public static class Builder implements CoordinatorShardBuilder<GlobalSequenceCoordinatorShard, CoordinatorRecord> {

        private final GlobalSequenceCoordinatorConfig config;
        private LogContext logContext;
        private SnapshotRegistry snapshotRegistry;
        private Time time;
        private CoordinatorTimer<Void, CoordinatorRecord> timer;
        private CoordinatorExecutor<CoordinatorRecord> executor;
        private CoordinatorMetrics coordinatorMetrics;
        private TopicPartition topicPartition;
        private GlobalSequenceStateRegistry stateRegistry;

        public Builder(GlobalSequenceCoordinatorConfig config) {
            this.config = config;
        }

        @Override
        public Builder withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        @Override
        public Builder withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        @Override
        public Builder withTime(Time time) {
            this.time = time;
            return this;
        }

        @Override
        public Builder withTimer(CoordinatorTimer<Void, CoordinatorRecord> timer) {
            this.timer = timer;
            return this;
        }

        @Override
        public Builder withExecutor(CoordinatorExecutor<CoordinatorRecord> executor) {
            this.executor = executor;
            return this;
        }

        @Override
        public Builder withCoordinatorMetrics(CoordinatorMetrics coordinatorMetrics) {
            this.coordinatorMetrics = coordinatorMetrics;
            return this;
        }

        @Override
        public Builder withTopicPartition(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
            return this;
        }

        public Builder withStateRegistry(GlobalSequenceStateRegistry stateRegistry) {
            this.stateRegistry = stateRegistry;
            return this;
        }

        @SuppressWarnings("NPathComplexity")
        @Override
        public GlobalSequenceCoordinatorShard build() {
            if (logContext == null) logContext = new LogContext();
            if (config == null)
                throw new IllegalArgumentException("Config must be set.");
            if (snapshotRegistry == null)
                throw new IllegalArgumentException("SnapshotRegistry must be set.");
            if (time == null)
                throw new IllegalArgumentException("Time must be set.");
            if (timer == null)
                throw new IllegalArgumentException("Timer must be set.");
            if (executor == null)
                throw new IllegalArgumentException("Executor must be set.");
            if (coordinatorMetrics == null || !(coordinatorMetrics instanceof GlobalSequenceCoordinatorMetrics))
                throw new IllegalArgumentException("CoordinatorMetrics must be set and be of type GlobalSequenceCoordinatorMetrics.");
            if (topicPartition == null)
                throw new IllegalArgumentException("TopicPartition must be set.");
            if (stateRegistry == null)
                throw new IllegalArgumentException("StateRegistry must be set.");

            GlobalSequenceCoordinatorMetricsShard metricsShard = ((GlobalSequenceCoordinatorMetrics) coordinatorMetrics)
                    .newMetricsShard(snapshotRegistry, topicPartition);

            return new GlobalSequenceCoordinatorShard(
                    logContext,
                    stateRegistry,
                    time,
                    timer,
                    config,
                    coordinatorMetrics,
                    metricsShard
            );
        }
    }

    static final String GLOBAL_SEQUENCE_EXPIRATION_KEY = "expire-global-sequence-metadata";

    private final Logger log;

    private final GlobalSequenceStateRegistry stateRegistry;

    private final Time time;

    private final CoordinatorTimer<Void, CoordinatorRecord> timer;

    private final GlobalSequenceCoordinatorConfig config;

    private final CoordinatorMetrics coordinatorMetrics;

    private final CoordinatorMetricsShard metricsShard;

    GlobalSequenceCoordinatorShard(
            LogContext logContext,
            GlobalSequenceStateRegistry stateRegistry,
            Time time,
            CoordinatorTimer<Void, CoordinatorRecord> timer,
            GlobalSequenceCoordinatorConfig config,
            CoordinatorMetrics coordinatorMetrics,
            CoordinatorMetricsShard metricsShard
    ) {
        this.log = logContext.logger(GlobalSequenceCoordinatorShard.class);
        this.stateRegistry = stateRegistry;
        this.time = time;
        this.timer = timer;
        this.config = config;
        this.coordinatorMetrics = coordinatorMetrics;
        this.metricsShard = metricsShard;
    }

    public CoordinatorResult<GlobalSequenceIndexRecord, CoordinatorRecord> addNewIndexRecord(
        RequestContext context,
        Uuid topicId,
        int numRecords,
        int partitionIndex,
        long partitionOffset,
        long producerId,
        short producerEpoch,
        int baseSequence
    ) {
        GlobalSequenceIndexRecord newIndexRecord = stateRegistry.addSequenceIndex(topicId, numRecords, partitionIndex, partitionOffset, producerId, producerEpoch, baseSequence);

        List<CoordinatorRecord> records = new ArrayList<>();
        records.add(toCoordinatorRecord(newIndexRecord));
        return new CoordinatorResult<>(records, newIndexRecord);
    }

    private CoordinatorRecord toCoordinatorRecord(GlobalSequenceIndexRecord indexRecord) {
        GlobalSequenceIndexLogKey key = new GlobalSequenceIndexLogKey();
        key.setTopicId(indexRecord.topicId());
        key.setGlobalOffset(indexRecord.globalOffset());

        GlobalSequenceIndexLogValue value = new GlobalSequenceIndexLogValue();
        value.setRecordsCount(indexRecord.numRecords());
        value.setPartitionIndex(indexRecord.partition());
        value.setPartitionOffset(indexRecord.partitionOffset());
        value.setProducerId(indexRecord.producerId());
        value.setProducerEpoch(indexRecord.producerEpoch());
        value.setBaseSequence(indexRecord.baseSequence());

        return CoordinatorRecord.record(key, new ApiMessageAndVersion(value, (short) 0));
    }

    @Override
    public void onLoaded(MetadataImage newImage) {
        coordinatorMetrics.activateMetricsShard(metricsShard);
    }

    @Override
    public void onUnloaded() {
        timer.cancel(GLOBAL_SEQUENCE_EXPIRATION_KEY);
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
