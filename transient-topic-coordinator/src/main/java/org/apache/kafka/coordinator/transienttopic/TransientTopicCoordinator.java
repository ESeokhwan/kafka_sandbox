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
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.CoordinatorRecord;
import org.apache.kafka.coordinator.group.CoordinatorRecordSerde;
import org.apache.kafka.coordinator.group.metrics.CoordinatorRuntimeMetrics;
import org.apache.kafka.coordinator.group.runtime.CoordinatorEventProcessor;
import org.apache.kafka.coordinator.group.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.group.runtime.CoordinatorShardBuilderSupplier;
import org.apache.kafka.coordinator.group.runtime.MultiThreadedEventProcessor;
import org.apache.kafka.coordinator.group.runtime.PartitionWriter;
import org.apache.kafka.coordinator.transienttopic.metrics.TransientTopicCoordinatorMetrics;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.util.timer.Timer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;

public class TransientTopicCoordinator {

    public static class Builder {
        private int nodeId;
        private TransientTopicCoordinatorConfig config;
        private PartitionWriter writer;
        private CoordinatorLoader<CoordinatorRecord> loader;
        private Time time;
        private Timer timer;
        private CoordinatorRuntimeMetrics coordinatorRuntimeMetrics;
        private TransientTopicCoordinatorMetrics transientTopicCoordinatorMetrics;

        public Builder(
            int nodeId,
            TransientTopicCoordinatorConfig config
        ) {
            this.nodeId = nodeId;
            this.config = config;
        }

        public Builder withWriter(PartitionWriter writer) {
            this.writer = writer;
            return this;
        }

        public Builder withLoader(CoordinatorLoader<CoordinatorRecord> loader) {
            this.loader = loader;
            return this;
        }

        public Builder withTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder withTimer(Timer timer) {
            this.timer = timer;
            return this;
        }

        public Builder withCoordinatorRuntimeMetrics(CoordinatorRuntimeMetrics coordinatorRuntimeMetrics) {
            this.coordinatorRuntimeMetrics = coordinatorRuntimeMetrics;
            return this;
        }

        public Builder withTransientTopicCoordinatorMetrics(TransientTopicCoordinatorMetrics transientTopicCoordinatorMetrics) {
            this.transientTopicCoordinatorMetrics = transientTopicCoordinatorMetrics;
            return this;
        }

        public TransientTopicCoordinator build() {
            if (config == null)
                throw new IllegalArgumentException("Config must be set.");
            if (writer == null)
                throw new IllegalArgumentException("Writer must be set.");
            if (loader == null)
                throw new IllegalArgumentException("Loader must be set.");
            if (time == null)
                throw new IllegalArgumentException("Time must be set.");
            if (timer == null)
                throw new IllegalArgumentException("Timer must be set.");
            if (coordinatorRuntimeMetrics == null)
                throw new IllegalArgumentException("CoordinatorRuntimeMetrics must be set.");
            if (transientTopicCoordinatorMetrics == null)
                throw new IllegalArgumentException("TransientTopicCoordinatorMetrics must be set.");

            String logPrefix = String.format("TransientTopicCoordinator id=%d", nodeId);
            LogContext logContext = new LogContext(String.format("[%s] ", logPrefix));

            TransientTopicIndexCache indexCache = new TransientTopicIndexCache.Builder()
                .build();
            CoordinatorShardBuilderSupplier<TransientTopicCoordinatorShard, CoordinatorRecord> supplier = () ->
                new TransientTopicCoordinatorShard.Builder(config)
                    .withIndexCache(indexCache);

            CoordinatorEventProcessor processor = new MultiThreadedEventProcessor(
                logContext,
                "transient-topic-coordinator-event-processor-",
                1,
                time,
                coordinatorRuntimeMetrics
            );

            CoordinatorRuntime<TransientTopicCoordinatorShard, CoordinatorRecord> runtime =
                new CoordinatorRuntime.Builder<TransientTopicCoordinatorShard, CoordinatorRecord>()
                    .withTime(time)
                    .withTimer(timer)
                    .withLogPrefix(logPrefix)
                    .withLogContext(logContext)
                    .withEventProcessor(processor)
                    .withPartitionWriter(writer)
                    .withLoader(loader)
                    .withCoordinatorShardBuilderSupplier(supplier)
                    .withDefaultWriteTimeOut(Duration.ofMillis(1000)) // TODO: make configurable
                    .withCoordinatorRuntimeMetrics(coordinatorRuntimeMetrics)
                    .withCoordinatorMetrics(transientTopicCoordinatorMetrics)
                    .withSerializer(new CoordinatorRecordSerde())
                    .withCompression(Compression.of(CompressionType.NONE).build()) // TODO: make configurable
                    .withAppendLingerMs(1000) // TODO: make configurable
                    .build();

            return new TransientTopicCoordinator(
                logContext,
                config,
                runtime,
                indexCache,
                transientTopicCoordinatorMetrics
            );
        }
    }

    private final Logger log;

    private final TransientTopicCoordinatorConfig config;

    private final CoordinatorRuntime<TransientTopicCoordinatorShard, CoordinatorRecord> runtime;

    private final TransientTopicIndexCache indexCache;

    private final TransientTopicCoordinatorMetrics transientTopicCoordinatorMetrics;

    /**
     * Boolean indicating whether the coordinator is active or not.
     */
    private final AtomicBoolean isActive = new AtomicBoolean(false);

    /**
     * The number of partitions of the __transient_topic_index topics. This is provided
     * when the component is started.
     */
    private volatile int numPartitions = -1;

    TransientTopicCoordinator(
        LogContext logContext,
        TransientTopicCoordinatorConfig config,
        CoordinatorRuntime<TransientTopicCoordinatorShard, CoordinatorRecord> runtime,
        TransientTopicIndexCache indexCache,
        TransientTopicCoordinatorMetrics transientTopicCoordinatorMetrics
    ) {
        this.log = logContext.logger(TransientTopicCoordinator.class);
        this.config = config;
        this.runtime = runtime;
        this.indexCache = indexCache;
        this.transientTopicCoordinatorMetrics = transientTopicCoordinatorMetrics;
    }

    public TransientTopicIndexCache indexCache() {
        return indexCache;
    }

    /**
     * Throws CoordinatorNotAvailableException if the not active.
     */
    private void throwIfNotActive() {
        if (!isActive.get()) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }
    }

    private TopicPartition topicPartitionFor(
        Uuid topicId
    ) {
        return new TopicPartition(Topic.TRANSIENT_TOPIC_INDEX_TOPIC_NAME, partitionFor(topicId));
    }

    private int partitionFor(
        Uuid topicId
    ) {
        return Utils.abs(topicId.hashCode()) % numPartitions;
    }

    public TransientTopic getCachedIndex(
        String topicName
    ) {
        return indexCache.getIndex(topicName);
    }

    public boolean existsInIndexCache(String topicName) {
        return indexCache.contains(topicName);
    }

    public CompletableFuture<TransientTopic> createNewTransientTopic(
        RequestContext context,
        String topicName
    ) {
        // TODO: Implement here
        return null;
    }

    public CompletableFuture<Void> freeTransientTopic() {
        // TODO: Implement here
        return null;
    }

    public void onElection(
        int indexMetadataPartitionIndex,
        int indexMetadataPartitionLeaderEpoch
    ) {
        throwIfNotActive();
        runtime.scheduleLoadOperation(
            new TopicPartition(Topic.TRANSIENT_TOPIC_INDEX_TOPIC_NAME, indexMetadataPartitionIndex),
            indexMetadataPartitionLeaderEpoch
        );
    }

    public void onResignation(
        int groupMetadataPartitionIndex,
        OptionalInt groupMetadataPartitionLeaderEpoch
    ) {
        // TODO: Implement here
    }

    public void onNewMetadataImage(
        MetadataImage newImage,
        MetadataDelta delta
    ) {
        throwIfNotActive();
        runtime.onNewMetadataImage(newImage, delta);
    }

    public void startup(IntSupplier transientTopicIndexTopicPartitionCount) {
        if (!isActive.compareAndSet(false, true)) {
            log.warn("Transient topic coordinator is already running.");
            return;
        }

        log.info("Starting up.");
        numPartitions = transientTopicIndexTopicPartitionCount.getAsInt();
        isActive.set(true);
        log.info("Startup complete.");
    }

    public void shutdown() {
        if (!isActive.compareAndSet(true, false)) {
            log.warn("Transient topic coordinator is already shutting down.");
            return;
        }

        log.info("Shutting down.");
        isActive.set(false);
        Utils.closeQuietly(runtime, "coordinator runtime");
        Utils.closeQuietly(transientTopicCoordinatorMetrics, "transient topic coordinator metrics");
        log.info("Shutdown complete.");
    }
}
