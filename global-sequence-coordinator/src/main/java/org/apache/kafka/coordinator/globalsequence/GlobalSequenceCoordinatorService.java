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
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetricsImpl;
import org.apache.kafka.coordinator.common.runtime.MultiThreadedEventProcessor;
import org.apache.kafka.coordinator.common.runtime.PartitionWriter;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendResponse;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PartitionDescription;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PartitionKey;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PhysicalBatch;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.RegistrationRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.RegistrationResponse;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.IntSupplier;

import static org.apache.kafka.common.internals.Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME;
import static org.apache.kafka.common.internals.Topic.isInternal;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.requireNonZeroId;

/** Owns the local index runtime and initiates internal topic creation when indexing is enabled. */
public class GlobalSequenceCoordinatorService implements GlobalSequenceCoordinator {
    public static final String METRICS_GROUP = "global-sequence-coordinator-metrics";
    static final long TOPIC_CREATION_RETRY_MS = 1000;

    public static class Builder {
        private final int nodeId;
        private final GlobalSequenceCoordinatorConfig config;
        private PartitionWriter writer;
        private CoordinatorLoader<CoordinatorRecord> loader;
        private Time time = Time.SYSTEM;
        private Timer timer;
        private Metrics metrics;
        private Runnable createIndexTopic;

        public Builder(int nodeId, GlobalSequenceCoordinatorConfig config) {
            this.nodeId = nodeId;
            this.config = Objects.requireNonNull(config, "config");
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

        public Builder withMetrics(Metrics metrics) {
            this.metrics = metrics;
            return this;
        }

        /** Must enqueue a nonblocking controller request; repeated requests must be safe. */
        public Builder withTopicCreation(Runnable createIndexTopic) {
            this.createIndexTopic = createIndexTopic;
            return this;
        }

        public GlobalSequenceCoordinatorService build() {
            Objects.requireNonNull(writer, "writer");
            Objects.requireNonNull(loader, "loader");
            Objects.requireNonNull(time, "time");
            Objects.requireNonNull(timer, "timer");
            Objects.requireNonNull(metrics, "metrics");
            Objects.requireNonNull(createIndexTopic, "createIndexTopic");
            LogContext logContext = new LogContext("[GlobalSequenceCoordinator id=" + nodeId + "] ");
            CoordinatorRuntimeMetricsImpl runtimeMetrics = new CoordinatorRuntimeMetricsImpl(metrics, METRICS_GROUP);
            MultiThreadedEventProcessor processor = new MultiThreadedEventProcessor(logContext,
                "global-sequence-coordinator-event-processor-", config.numThreads(), time, runtimeMetrics);
            ExecutorService executor = Executors.newSingleThreadExecutor(runnable ->
                new KafkaThread("global-sequence-coordinator-executor-" + nodeId, runnable, true));
            try {
                CoordinatorRuntime<GlobalSequenceCoordinatorShard, CoordinatorRecord> runtime =
                    new CoordinatorRuntime.Builder<GlobalSequenceCoordinatorShard, CoordinatorRecord>()
                        .withLogContext(logContext)
                        .withTime(time)
                        .withTimer(timer)
                        .withEventProcessor(processor)
                        .withPartitionWriter(writer)
                        .withLoader(loader)
                        .withCoordinatorShardBuilderSupplier(GlobalSequenceCoordinatorShardBuilder::new)
                        .withDefaultWriteTimeOut(Duration.ofMillis(config.writeTimeoutMs()))
                        .withCoordinatorRuntimeMetrics(runtimeMetrics)
                        .withCoordinatorMetrics(new GlobalSequenceCoordinatorMetrics())
                        .withSerializer(new GlobalSequenceCoordinatorRecordSerde())
                        .withAppendLingerMs(config.appendLingerMs())
                        .withExecutorService(executor)
                        .build();
                return new GlobalSequenceCoordinatorService(logContext, config, runtime, timer, createIndexTopic);
            } catch (RuntimeException e) {
                Utils.closeQuietly(processor, "global sequence event processor");
                executor.shutdown();
                Utils.closeQuietly(runtimeMetrics, "global sequence runtime metrics");
                Utils.closeQuietly(loader, "global sequence loader");
                Utils.closeQuietly(timer, "global sequence timer");
                throw e;
            }
        }
    }

    private final Logger log;
    private final GlobalSequenceCoordinatorConfig config;
    private final CoordinatorRuntime<GlobalSequenceCoordinatorShard, CoordinatorRecord> runtime;
    private final Timer timer;
    private final Runnable createIndexTopic;
    private volatile boolean active;
    private boolean closed;
    private volatile MetadataImage metadataImage = MetadataImage.EMPTY;
    private volatile IllegalStateException metadataFailure;
    private Uuid indexTopicId;
    private boolean needsTopicCreation;
    private TimerTask topicCreationTask;

    GlobalSequenceCoordinatorService(
        LogContext logContext,
        GlobalSequenceCoordinatorConfig config,
        CoordinatorRuntime<GlobalSequenceCoordinatorShard, CoordinatorRecord> runtime,
        Timer timer,
        Runnable createIndexTopic
    ) {
        this.log = logContext.logger(GlobalSequenceCoordinatorService.class);
        this.config = config;
        this.runtime = runtime;
        this.timer = timer;
        this.createIndexTopic = createIndexTopic;
    }

    @Override
    public synchronized void startup(IntSupplier indexPartitionCount) {
        if (closed) throw new IllegalStateException("A shut down coordinator cannot be restarted");
        if (active) return;
        int actual = indexPartitionCount.getAsInt();
        if (actual != config.indexTopicNumPartitions()) {
            throw new IllegalStateException("Index partition count " + actual + " differs from configured fixed count " + config.indexTopicNumPartitions());
        }
        active = true;
        log.info("Started with {} index partitions.", actual);
    }

    @Override
    public int partitionFor(Uuid topicId) {
        requireActive();
        requireNonZeroId(topicId, "topicId");
        return Utils.abs(topicId.hashCode()) % config.indexTopicNumPartitions();
    }

    @Override
    public Properties indexTopicConfigs() {
        return config.indexTopicConfigs();
    }

    @Override
    public void onElection(int partition, int leaderEpoch) {
        requireActive();
        if (leaderEpoch < 0) throw new IllegalArgumentException("leaderEpoch must be non-negative");
        runtime.scheduleLoadOperation(indexPartition(partition), leaderEpoch);
    }

    @Override
    public void onResignation(int partition, OptionalInt leaderEpoch) {
        // Resignation must still unload shards after invalid index metadata was detected.
        if (!active) throw new CoordinatorNotAvailableException("The global sequence coordinator is not active");
        runtime.scheduleUnloadOperation(indexPartition(partition), leaderEpoch);
    }

    private TopicPartition indexPartition(int partition) {
        if (partition < 0 || partition >= config.indexTopicNumPartitions()) {
            throw new IllegalArgumentException("Invalid index partition " + partition);
        }
        return new TopicPartition(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, partition);
    }

    @Override
    public synchronized void onNewMetadataImage(MetadataImage image, MetadataDelta delta) {
        requireActive();
        TopicImage index = image.topics().getTopic(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME);
        try {
            validateIndexMetadata(image, index);
        } catch (IllegalStateException e) {
            metadataFailure = e;
            stopTopicCreation();
            // Fail outstanding waits and prevent serving the old state after index loss/remapping.
            for (int partition = 0; partition < config.indexTopicNumPartitions(); partition++) {
                runtime.scheduleUnloadOperation(indexPartition(partition), OptionalInt.empty());
            }
            throw e;
        }
        metadataImage = image;
        runtime.onNewMetadataImage(image, delta);
        needsTopicCreation = index == null && hasEnabledTopic(image);
        if (needsTopicCreation) {
            if (topicCreationTask == null) scheduleTopicCreation(0);
        } else {
            stopTopicCreation();
        }
    }

    private void validateIndexMetadata(MetadataImage image, TopicImage index) {
        if (index == null) {
            if (indexTopicId != null) throw new IllegalStateException("The global sequence index topic was deleted; its history must be restored");
            return;
        }
        if (indexTopicId != null && !indexTopicId.equals(index.id())) {
            throw new IllegalStateException("The global sequence index topic was replaced; its history must be restored");
        }
        if (index.partitions().size() != config.indexTopicNumPartitions()) {
            throw new IllegalStateException("The global sequence index topic partition count must remain " + config.indexTopicNumPartitions());
        }
        Map<String, String> configs = image.configs().configMapForResource(new ConfigResource(ConfigResource.Type.TOPIC, index.name()));
        GlobalSequenceCoordinatorConfig.REQUIRED_INDEX_TOPIC_CONFIGS.forEach((key, value) -> {
            if (!value.equals(configs.get(key))) {
                throw new IllegalStateException("The global sequence index topic requires explicit " + key + "=" + value);
            }
        });
        indexTopicId = index.id();
    }

    private static boolean hasEnabledTopic(MetadataImage image) {
        return image.topics().topicsByName().values().stream().anyMatch(topic -> isEnabled(image, topic.name()));
    }

    private static boolean isEnabled(MetadataImage image, String topic) {
        String enabled = image.configs().configMapForResource(new ConfigResource(ConfigResource.Type.TOPIC, topic))
            .getOrDefault(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "false");
        return !isInternal(topic) && Boolean.parseBoolean(enabled.trim());
    }

    private void scheduleTopicCreation(long delayMs) {
        topicCreationTask = new TimerTask(delayMs) {
            @Override
            public void run() {
                synchronized (GlobalSequenceCoordinatorService.this) {
                    // A canceled task may already have been dequeued; ignore it by identity.
                    if (topicCreationTask != this || !active || !needsTopicCreation) return;
                    try {
                        createIndexTopic.run();
                    } catch (Exception e) {
                        log.warn("Could not request global sequence index topic creation; retrying.", e);
                    } finally {
                        if (topicCreationTask == this && active && needsTopicCreation) {
                            scheduleTopicCreation(TOPIC_CREATION_RETRY_MS);
                        }
                    }
                }
            }
        };
        timer.add(topicCreationTask);
    }

    private void stopTopicCreation() {
        needsTopicCreation = false;
        if (topicCreationTask != null) topicCreationTask.cancel();
        topicCreationTask = null;
    }

    @Override
    public CompletableFuture<AppendResponse> appendIndex(AppendRequest request) {
        return appendIndex(request, -1);
    }

    @Override
    public CompletableFuture<AppendResponse> appendIndex(AppendRequest request, int expectedCoordinatorEpoch) {
        try {
            requireReady();
            TopicPartition partition = indexPartition(partitionFor(request.batch().partition().topicId()));
            return runtime.scheduleWriteOperationWithContext("append-global-index", partition,
                Duration.ofMillis(config.writeTimeoutMs()), (shard, context) -> {
                    requireReady();
                    validateCoordinatorEpoch(expectedCoordinatorEpoch, context.leaderEpoch());
                    requireSourceLeader(request.batch().partition(), request.indexer().brokerId(), request.indexer().leaderEpoch());
                    return shard.prepareAppend(request, context);
                });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<RegistrationResponse> registerIndexer(RegistrationRequest request, int expectedCoordinatorEpoch) {
        try {
            requireReady();
            return runtime.scheduleWriteOperationWithContext("register-global-indexer", indexPartition(partitionFor(request.partition().topicId())),
                Duration.ofMillis(config.writeTimeoutMs()), (shard, context) -> {
                    requireReady();
                    validateCoordinatorEpoch(expectedCoordinatorEpoch, context.leaderEpoch());
                    requireSourceLeader(request.partition(), request.sourceBrokerId(), request.sourceLeaderEpoch());
                    return shard.prepareRegistration(request, context);
                });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<PartitionDescription> describePartition(PartitionKey partition) {
        try {
            requireReady();
            return runtime.scheduleReadOperation("describe-global-index-partition", indexPartition(partitionFor(partition.topicId())),
                (shard, committedOffset) -> {
                    requireReady();
                    requireEnabledTopic(partition);
                    return shard.describePartition(partition);
                });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static void validateCoordinatorEpoch(int expected, int actual) {
        if (expected < -1) throw new IllegalArgumentException("expectedCoordinatorEpoch must be -1 or non-negative");
        if (expected >= 0 && expected != actual) {
            throw new NotCoordinatorException("Expected coordinator epoch " + expected + ", current epoch is " + actual);
        }
    }

    private void requireSourceLeader(PartitionKey partition, int brokerId, int leaderEpoch) {
        TopicImage topic = requireEnabledTopic(partition);
        var source = topic.partitions().get(partition.partition());
        if (source.leader != brokerId || source.leaderEpoch != leaderEpoch) {
            throw new FencedLeaderEpochException("The sender is not the current source leader");
        }
    }

    @Override
    public CompletableFuture<Optional<PhysicalBatch>> committedProgress(PartitionKey partition) {
        try {
            requireReady();
            return runtime.scheduleReadOperation("global-index-progress", indexPartition(partitionFor(partition.topicId())),
                (shard, committedOffset) -> {
                    requireReady();
                    requireEnabledTopic(partition);
                    return shard.committedProgress(partition);
                });
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private TopicImage requireEnabledTopic(PartitionKey partition) {
        MetadataImage image = metadataImage;
        TopicImage topic = image.topics().getTopic(partition.topicId());
        if (topic == null || !topic.partitions().containsKey(partition.partition())) {
            throw new UnknownTopicOrPartitionException("Unknown global sequence data partition " + partition);
        }
        if (!isEnabled(image, topic.name())) throw new InvalidRequestException("Global sequence is not enabled on " + topic.name());
        return topic;
    }

    private void requireActive() {
        if (!active) throw new CoordinatorNotAvailableException("The global sequence coordinator is not active");
        if (metadataFailure != null) throw new CoordinatorNotAvailableException(metadataFailure.getMessage(), metadataFailure);
    }

    private void requireReady() {
        requireActive();
        if (metadataImage.topics().getTopic(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME) == null) {
            throw new CoordinatorNotAvailableException("The global sequence index topic is not yet available");
        }
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            if (closed) return;
            closed = true;
            active = false;
            stopTopicCreation();
        }
        // Also close resources when broker startup failed before startup() was called.
        Utils.closeQuietly(runtime, "global sequence coordinator runtime");
        log.info("Shut down.");
    }
}
