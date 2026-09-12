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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.CoordinatorReadOperation;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.CoordinatorWriteOperationWithContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorWriteContext;
import org.apache.kafka.coordinator.common.runtime.MockPartitionWriter;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendResponse;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.IndexerIdentity;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PartitionKey;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PhysicalBatch;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.internals.Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class GlobalSequenceCoordinatorServiceTest {
    private static final Uuid TOPIC = new Uuid(1, 2);
    private static final Uuid INDEX = new Uuid(3, 4);
    private static final PartitionKey PARTITION = new PartitionKey(TOPIC, 0);
    private static final PhysicalBatch BATCH = new PhysicalBatch(PARTITION, 0, 2, 3);
    private static final IndexerIdentity OWNER = new IndexerIdentity(1, 3, 0, new Uuid(5, 6));
    private static final AppendRequest APPEND = new AppendRequest(BATCH, -1, 3, OWNER);

    private static GlobalSequenceCoordinatorConfig config() {
        return new GlobalSequenceCoordinatorConfig(new AbstractConfig(GlobalSequenceCoordinatorConfig.CONFIG_DEF,
            Map.of(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_NUM_PARTITIONS_CONFIG, 2), false));
    }

    static MetadataImage image(int indexPartitions, boolean enabled) {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        addTopic(delta, "data", TOPIC, 1);
        config(delta, "data", TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, Boolean.toString(enabled));
        if (indexPartitions > 0) {
            addTopic(delta, GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, INDEX, indexPartitions);
            GlobalSequenceCoordinatorConfig.REQUIRED_INDEX_TOPIC_CONFIGS.forEach((key, value) ->
                config(delta, GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, key, value));
        }
        return delta.apply(MetadataProvenance.EMPTY);
    }

    private static void addTopic(MetadataDelta delta, String name, Uuid id, int partitions) {
        delta.replay(new TopicRecord().setName(name).setTopicId(id));
        for (int partition = 0; partition < partitions; partition++) {
            delta.replay(new PartitionRecord().setTopicId(id).setPartitionId(partition).setLeader(1).setLeaderEpoch(3)
                .setReplicas(List.of(1)).setIsr(List.of(1)));
        }
    }

    private static void config(MetadataDelta delta, String topic, String key, String value) {
        delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName(topic).setName(key).setValue(value));
    }

    private static class Context {
        final MockTimer timer = new MockTimer();
        final CoordinatorRuntime<GlobalSequenceCoordinatorShard, CoordinatorRecord> runtime = mock(CoordinatorRuntime.class);
        final AtomicInteger creations = new AtomicInteger();
        final GlobalSequenceCoordinatorService service = new GlobalSequenceCoordinatorService(
            new LogContext(), config(), runtime, timer, creations::incrementAndGet);

        void start() {
            service.startup(() -> 2);
        }

        void publish(MetadataImage image) {
            service.onNewMetadataImage(image, new MetadataDelta.Builder().setImage(image).build());
        }
    }

    @Test
    void testLifecycleAndFixedPartitionMapping() throws Exception {
        Context ctx = new Context();
        assertThrows(CoordinatorNotAvailableException.class, () -> ctx.service.partitionFor(TOPIC));
        assertFutureThrows(CoordinatorNotAvailableException.class, ctx.service.committedProgress(PARTITION));
        assertFutureThrows(CoordinatorNotAvailableException.class, ctx.service.appendIndex(APPEND));
        assertThrows(IllegalStateException.class, () -> ctx.service.startup(() -> 3));
        ctx.start();
        ctx.service.startup(() -> {
            throw new AssertionError("startup must be idempotent");
        });
        assertEquals(1, ctx.service.partitionFor(TOPIC));
        assertEquals(0, ctx.service.partitionFor(new Uuid(0, 2)));
        assertThrows(IllegalArgumentException.class, () -> ctx.service.partitionFor(Uuid.ZERO_UUID));
        ctx.service.onElection(1, 10);
        ctx.service.onResignation(1, OptionalInt.of(11));
        ctx.service.onResignation(0, OptionalInt.empty());
        verify(ctx.runtime).scheduleLoadOperation(new TopicPartition(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, 1), 10);
        verify(ctx.runtime).scheduleUnloadOperation(new TopicPartition(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, 1), OptionalInt.of(11));
        verify(ctx.runtime).scheduleUnloadOperation(new TopicPartition(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, 0), OptionalInt.empty());
        assertThrows(IllegalArgumentException.class, () -> ctx.service.onElection(2, 10));
        assertThrows(IllegalArgumentException.class, () -> ctx.service.onElection(0, -1));
        ctx.service.shutdown();
        ctx.service.shutdown();
        verify(ctx.runtime).close();
        assertThrows(IllegalStateException.class, ctx::start);
        assertThrows(CoordinatorNotAvailableException.class, () -> ctx.service.onElection(0, 12));
    }

    @Test
    void testCreationRetriesUntilMetadataArrives() throws Exception {
        Context ctx = new Context();
        ctx.start();
        ctx.publish(image(0, false));
        ctx.timer.advanceClock(2000);
        assertEquals(0, ctx.creations.get());
        ctx.publish(image(0, true));
        ctx.timer.advanceClock(1);
        assertEquals(1, ctx.creations.get());
        assertFutureThrows(CoordinatorNotAvailableException.class, ctx.service.committedProgress(PARTITION));
        ctx.timer.advanceClock(1001);
        assertEquals(2, ctx.creations.get());
        MetadataImage ready = image(2, true);
        ctx.publish(ready);
        ctx.timer.advanceClock(2000);
        assertEquals(2, ctx.creations.get());
        verify(ctx.runtime).onNewMetadataImage(eq(ready), any());
        ctx.service.shutdown();
    }

    @Test
    void testCreationStopsWhenLastEnabledTopicDisappearsOrServiceShutsDown() throws Exception {
        Context ctx = new Context();
        ctx.start();
        ctx.publish(image(0, true));
        ctx.publish(image(0, false));
        ctx.timer.advanceClock(1);
        assertEquals(1, ctx.creations.get());
        ctx.publish(image(0, true));
        int requestsBeforeShutdown = ctx.creations.get();
        ctx.service.shutdown();
        ctx.timer.advanceClock(2000);
        assertEquals(requestsBeforeShutdown, ctx.creations.get());
    }

    @Test
    void testCreationExceptionDoesNotStopRetries() throws Exception {
        MockTimer timer = new MockTimer();
        AtomicInteger attempts = new AtomicInteger();
        GlobalSequenceCoordinatorService service = new GlobalSequenceCoordinatorService(new LogContext(), config(),
            mock(CoordinatorRuntime.class), timer, () -> {
                if (attempts.incrementAndGet() == 1) throw new IllegalStateException("controller unavailable");
            });
        service.startup(() -> 2);
        MetadataImage image = image(0, true);
        service.onNewMetadataImage(image, new MetadataDelta.Builder().setImage(image).build());
        timer.advanceClock(1);
        timer.advanceClock(1001);
        assertEquals(2, attempts.get());
        service.shutdown();
    }

    @Test
    void testIndexLossAndCountChangeStopServiceWithoutRecreatingHistory() throws Exception {
        for (int partitions : List.of(0, 3)) {
            Context ctx = new Context();
            ctx.start();
            ctx.publish(image(2, true));
            assertThrows(IllegalStateException.class, () -> ctx.publish(image(partitions, true)));
            assertFutureThrows(CoordinatorNotAvailableException.class, ctx.service.appendIndex(APPEND));
            for (int partition = 0; partition < 2; partition++) {
                verify(ctx.runtime).scheduleUnloadOperation(new TopicPartition(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, partition), OptionalInt.empty());
            }
            ctx.timer.advanceClock(2000);
            assertEquals(0, ctx.creations.get());
            ctx.service.shutdown();
        }
    }

    @Test
    void testUnsafeIndexConfigurationIsRejected() {
        for (String key : GlobalSequenceCoordinatorConfig.REQUIRED_INDEX_TOPIC_CONFIGS.keySet()) {
            Context ctx = new Context();
            ctx.start();
            MetadataDelta delta = new MetadataDelta.Builder().setImage(image(2, true)).build();
            config(delta, GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, key, null);
            assertThrows(IllegalStateException.class, () -> ctx.publish(delta.apply(MetadataProvenance.EMPTY)));
            ctx.service.shutdown();
        }
    }

    @Test
    void testReadIsScheduledOnMappedPartitionAndValidatesLatestTopicMetadata() {
        Context ctx = new Context();
        ctx.start();
        ctx.publish(image(2, true));
        CompletableFuture<Optional<PhysicalBatch>> future = new CompletableFuture<>();
        when(ctx.runtime.<Optional<PhysicalBatch>>scheduleReadOperation(anyString(), any(), any())).thenReturn(future);
        assertSame(future, ctx.service.committedProgress(PARTITION));
        ArgumentCaptor<CoordinatorReadOperation<GlobalSequenceCoordinatorShard, Optional<PhysicalBatch>>> operation = ArgumentCaptor.forClass(CoordinatorReadOperation.class);
        verify(ctx.runtime).scheduleReadOperation(eq("global-index-progress"),
            eq(new TopicPartition(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, 1)), operation.capture());
        GlobalSequenceCoordinatorShard shard = mock(GlobalSequenceCoordinatorShard.class);
        when(shard.committedProgress(PARTITION)).thenReturn(Optional.of(BATCH));
        assertEquals(Optional.of(BATCH), operation.getValue().generateResponse(shard, 3));
        ctx.publish(image(2, false));
        assertThrows(InvalidRequestException.class, () -> operation.getValue().generateResponse(shard, 3));
        ctx.service.shutdown();
    }

    @Test
    void testAppendUsesRuntimeContextAndRechecksSourceLeader() {
        Context ctx = new Context();
        ctx.start();
        ctx.publish(image(2, true));
        CompletableFuture<AppendResponse> future = new CompletableFuture<>();
        when(ctx.runtime.<AppendResponse>scheduleWriteOperationWithContext(anyString(), any(), any(), any())).thenReturn(future);
        assertSame(future, ctx.service.appendIndex(APPEND));
        ArgumentCaptor<CoordinatorWriteOperationWithContext<GlobalSequenceCoordinatorShard, AppendResponse, CoordinatorRecord>> operation =
            ArgumentCaptor.forClass(CoordinatorWriteOperationWithContext.class);
        verify(ctx.runtime).scheduleWriteOperationWithContext(eq("append-global-index"),
            eq(new TopicPartition(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, 1)), eq(Duration.ofMillis(5000)), operation.capture());
        GlobalSequenceCoordinatorShard shard = mock(GlobalSequenceCoordinatorShard.class);
        CoordinatorWriteContext writeContext = new CoordinatorWriteContext(3, 10);
        CoordinatorResult<AppendResponse, CoordinatorRecord> expected = new CoordinatorResult<>(List.of());
        when(shard.prepareAppend(APPEND, writeContext)).thenReturn(expected);
        assertSame(expected, operation.getValue().generateRecordsAndResult(shard, writeContext));
        MetadataDelta delta = new MetadataDelta.Builder().setImage(image(2, true)).build();
        delta.replay(new PartitionRecord().setTopicId(TOPIC).setPartitionId(0).setLeader(2).setLeaderEpoch(4)
            .setReplicas(List.of(1, 2)).setIsr(List.of(1, 2)));
        ctx.publish(delta.apply(MetadataProvenance.EMPTY));
        assertThrows(FencedLeaderEpochException.class, () -> operation.getValue().generateRecordsAndResult(shard, writeContext));
        ctx.service.shutdown();
    }

    @Test
    void testUnknownTopicCannotReadOldProgress() {
        Context ctx = new Context();
        ctx.start();
        ctx.publish(image(2, true));
        when(ctx.runtime.scheduleReadOperation(anyString(), any(), any())).thenAnswer(invocation -> {
            CoordinatorReadOperation<GlobalSequenceCoordinatorShard, ?> op = invocation.getArgument(2);
            return CompletableFuture.completedFuture(op.generateResponse(mock(GlobalSequenceCoordinatorShard.class), 0));
        });
        assertFutureThrows(UnknownTopicOrPartitionException.class,
            ctx.service.committedProgress(new PartitionKey(new Uuid(9, 10), 0)));
        ctx.service.shutdown();
    }

    @Test
    void testRealRuntimeResourcesCloseEvenWithoutStartup() throws Exception {
        CoordinatorLoader<CoordinatorRecord> loader = mock(CoordinatorLoader.class);
        MockTimer timer = mock(MockTimer.class);
        try (Metrics metrics = new Metrics()) {
            GlobalSequenceCoordinatorService service = new GlobalSequenceCoordinatorService.Builder(1, config())
                .withWriter(new MockPartitionWriter()).withLoader(loader).withTimer(timer).withMetrics(metrics)
                .withTopicCreation(() -> {
                    throw new AssertionError("No startup or enabled topic");
                }).build();
            assertTrue(metrics.metrics().keySet().stream().anyMatch(name -> name.group().equals(GlobalSequenceCoordinatorService.METRICS_GROUP)));
            service.shutdown();
            service.shutdown();
            verify(loader).close();
            verify(timer).close();
            verify(loader, never()).load(any(), any());
            assertFalse(metrics.metrics().keySet().stream().anyMatch(name -> name.group().equals(GlobalSequenceCoordinatorService.METRICS_GROUP)));
        }
    }
}
