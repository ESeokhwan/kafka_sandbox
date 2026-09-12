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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorPlayback;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShardBuilder;
import org.apache.kafka.coordinator.common.runtime.ManualEventProcessor;
import org.apache.kafka.coordinator.common.runtime.MockPartitionWriter;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendResponse;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.IndexerIdentity;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PartitionKey;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PhysicalBatch;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.RegistrationRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.RegistrationResponse;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newBatchIndexRecord;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newIndexerFenceRecord;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newTopicMetadataRecord;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendStatus.ALREADY_INDEXED;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendStatus.INDEXED;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendStatus.OUT_OF_ORDER;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GlobalSequenceCoordinatorShardRuntimeTest {
    private static final TopicPartition INDEX_PARTITION = new TopicPartition("__global_sequence_index", 0);
    private static final Uuid TOPIC = new Uuid(1, 2);
    private static final PartitionKey P0 = new PartitionKey(TOPIC, 0);
    private static final PartitionKey P1 = new PartitionKey(TOPIC, 1);
    private static final IndexerIdentity OWNER = new IndexerIdentity(1, 3, 0, new Uuid(4, 5));
    private static final PhysicalBatch A = new PhysicalBatch(P0, 0, 2, 3);
    private static final PhysicalBatch B = new PhysicalBatch(P0, 3, 4, 2);
    private static final PhysicalBatch C = new PhysicalBatch(P1, 0, 0, 1);
    private static final Duration TIMEOUT = Duration.ofMillis(100);

    private static CoordinatorRecord fence(PartitionKey partition) {
        return newIndexerFenceRecord(partition.topicId(), partition.partition(), OWNER.brokerId(),
            OWNER.leaderEpoch(), OWNER.generation(), OWNER.registrationId());
    }

    private static class Context implements AutoCloseable {
        final MockTimer timer = new MockTimer();
        final ManualEventProcessor processor = new ManualEventProcessor();
        final MockPartitionWriter writer;
        final int lingerMs;
        final CoordinatorRuntime<GlobalSequenceCoordinatorShard, CoordinatorRecord> runtime;
        GlobalSequenceCoordinatorShard shard;

        Context(int lingerMs, MockPartitionWriter writer) {
            this(lingerMs, writer, List.of(), 0);
        }

        @SuppressWarnings("unchecked")
        Context(int lingerMs, MockPartitionWriter writer, List<CoordinatorRecord> recovered, long highWatermark) {
            this.writer = writer;
            this.lingerMs = lingerMs;
            CoordinatorShardBuilder<GlobalSequenceCoordinatorShard, CoordinatorRecord> builder = mock(CoordinatorShardBuilder.class, RETURNS_SELF);
            doAnswer(invocation -> {
                shard = new GlobalSequenceCoordinatorShard(invocation.getArgument(0));
                return builder;
            }).when(builder).withSnapshotRegistry(any());
            when(builder.build()).thenAnswer(ignored -> shard);
            CoordinatorLoader<CoordinatorRecord> loader = mock(CoordinatorLoader.class);
            when(loader.load(any(), any())).thenAnswer(invocation -> {
                CoordinatorPlayback<CoordinatorRecord> playback = invocation.getArgument(1);
                for (int i = 0; i < recovered.size(); i++) {
                    playback.replay(i, -1, (short) -1, recovered.get(i));
                }
                if (!recovered.isEmpty()) {
                    playback.updateLastWrittenOffset((long) recovered.size());
                    playback.updateLastCommittedOffset(highWatermark);
                }
                return CompletableFuture.completedFuture(null);
            });
            runtime = new CoordinatorRuntime.Builder<GlobalSequenceCoordinatorShard, CoordinatorRecord>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(TIMEOUT)
                .withLoader(loader)
                .withEventProcessor(processor)
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(() -> builder)
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new GlobalSequenceCoordinatorRecordSerde())
                .withExecutorService(mock(ExecutorService.class))
                .withAppendLingerMs(lingerMs)
                .build();
            runtime.scheduleLoadOperation(INDEX_PARTITION, 10);
            drain();
        }

        void register(PartitionKey... partitions) throws InterruptedException {
            List<CoordinatorRecord> records = new ArrayList<>();
            for (PartitionKey partition : partitions) records.add(fence(partition));
            CompletableFuture<Void> registered = runtime.scheduleWriteOperation("register", INDEX_PARTITION, TIMEOUT,
                ignored -> new CoordinatorResult<>(records, (Void) null));
            drain();
            flush();
            commit(records.size());
            assertTrue(registered.isDone());
            registered.join();
        }

        CompletableFuture<AppendResponse> append(PhysicalBatch batch, long predecessor, Duration timeout) {
            CompletableFuture<AppendResponse> future = runtime.scheduleWriteOperationWithContext("append-index", INDEX_PARTITION, timeout,
                (coordinator, context) -> coordinator.prepareAppend(new AppendRequest(batch, predecessor, batch.resumeOffset(), OWNER), context));
            drain();
            return future;
        }

        void drain() {
            while (processor.poll()) {
                // Advance the event queue and mock clock explicitly, without wall-clock sleeps.
            }
        }

        void flush() throws InterruptedException {
            if (lingerMs > 0) timer.advanceClock(lingerMs + 1);
            drain();
        }

        void commit(long offset) {
            writer.commit(INDEX_PARTITION, offset);
            drain();
        }

        @Override
        public void close() throws Exception {
            runtime.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10})
    void testRegistrationRetryWaitsForFenceAndPrecedingAllocation(int lingerMs) throws Exception {
        try (Context ctx = new Context(lingerMs, new MockPartitionWriter())) {
            ctx.register(P0);
            CompletableFuture<AppendResponse> allocation = ctx.append(A, -1, TIMEOUT);
            RegistrationRequest registration = new RegistrationRequest(P0, 2, 4, 0, new Uuid(7, 8));
            CompletableFuture<RegistrationResponse> first = ctx.runtime.scheduleWriteOperationWithContext("register", INDEX_PARTITION,
                Duration.ofMillis(3), (shard, context) -> shard.prepareRegistration(registration, context));
            ctx.drain();
            ctx.timer.advanceClock(4);
            ctx.drain();
            assertFutureThrows(TimeoutException.class, first);
            assertEquals(0, ctx.shard.committedNextGlobalOffset(TOPIC));
            assertEquals(1, ctx.shard.describePartition(P0).currentIndexer().orElseThrow().generation());
            CompletableFuture<RegistrationResponse> retry = ctx.runtime.scheduleWriteOperationWithContext("retry-register", INDEX_PARTITION,
                TIMEOUT, (shard, context) -> shard.prepareRegistration(registration, context));
            ctx.drain();
            ctx.flush();
            assertFalse(retry.isDone());
            assertFalse(allocation.isDone());
            ctx.commit(4);
            assertTrue(retry.join().registered());
            assertEquals(1, retry.join().indexer().orElseThrow().generation());
            assertEquals(INDEXED, allocation.join().status());
            assertEquals(Optional.of(A), ctx.shard.committedProgress(P0));
            assertEquals(3, ctx.shard.committedNextGlobalOffset(TOPIC));
            assertEquals(1, ctx.shard.committedIndexer(P0).orElseThrow().generation());
            assertFutureThrows(TimeoutException.class, first);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10})
    void testTimeoutRetryUsesSameAllocationAndWaitsForCommit(int lingerMs) throws Exception {
        try (Context ctx = new Context(lingerMs, new MockPartitionWriter())) {
            ctx.register(P0);
            CompletableFuture<AppendResponse> original = ctx.append(A, -1, Duration.ofMillis(3));
            assertFalse(original.isDone());
            assertEquals(3, ctx.shard.nextGlobalOffset(TOPIC));
            assertEquals(0, ctx.shard.committedNextGlobalOffset(TOPIC));
            ctx.timer.advanceClock(4);
            ctx.drain();
            assertFutureThrows(TimeoutException.class, original);
            assertEquals(1, ctx.shard.pendingAllocationCount());

            CompletableFuture<AppendResponse> retry = ctx.append(A, -1, TIMEOUT);
            CompletableFuture<Optional<PhysicalBatch>> progressAtCompletion = retry.thenApply(ignored -> ctx.shard.committedProgress(P0));
            assertFalse(retry.isDone());
            ctx.flush();
            assertEquals(2, ctx.writer.entries(INDEX_PARTITION).size()); // one fence batch, one allocation batch
            assertFalse(retry.isDone());
            ctx.commit(3);
            assertEquals(INDEXED, retry.join().status());
            assertEquals(OptionalLong.of(0), retry.join().globalBaseOffset());
            assertEquals(Optional.of(A), progressAtCompletion.join());
            assertEquals(0, ctx.shard.pendingAllocationCount());
            assertEquals(3, ctx.shard.committedNextGlobalOffset(TOPIC));
            assertFutureThrows(TimeoutException.class, original);
            assertEquals(ALREADY_INDEXED, ctx.append(A, -1, TIMEOUT).join().status());
            assertEquals(2, ctx.writer.entries(INDEX_PARTITION).size());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10})
    void testLocalAppendFailureRollsBackReplayedAllocation(int lingerMs) throws Exception {
        try (Context ctx = new Context(lingerMs, new MockPartitionWriter(1))) {
            ctx.register(P0);
            CompletableFuture<AppendResponse> failed = ctx.append(A, -1, TIMEOUT);
            ctx.flush();
            assertFutureThrows(KafkaException.class, failed);
            assertEquals(0, ctx.shard.nextGlobalOffset(TOPIC));
            assertEquals(0, ctx.shard.committedNextGlobalOffset(TOPIC));
            assertEquals(0, ctx.shard.pendingAllocationCount());
            assertEquals(Optional.empty(), ctx.shard.committedProgress(P0));
            assertEquals(Optional.of(OWNER), ctx.shard.committedIndexer(P0));
            assertEquals(1, ctx.writer.entries(INDEX_PARTITION).size());
        }
    }

    @Test
    void testBufferedAppendFailureRollsBackAllTopicsAndRetryWaiters() throws Exception {
        try (Context ctx = new Context(10, new MockPartitionWriter(1))) {
            ctx.register(P0, P1);
            CompletableFuture<AppendResponse> first = ctx.append(A, -1, TIMEOUT);
            CompletableFuture<AppendResponse> retry = ctx.append(A, -1, TIMEOUT);
            CompletableFuture<AppendResponse> second = ctx.append(C, -1, TIMEOUT);
            assertEquals(4, ctx.shard.nextGlobalOffset(TOPIC));
            assertEquals(2, ctx.shard.pendingAllocationCount());
            ctx.flush();
            assertFutureThrows(KafkaException.class, first);
            assertFutureThrows(KafkaException.class, retry);
            assertFutureThrows(KafkaException.class, second);
            assertEquals(0, ctx.shard.nextGlobalOffset(TOPIC));
            assertEquals(0, ctx.shard.pendingAllocationCount());
            assertEquals(Optional.empty(), ctx.shard.committedProgress(P0));
            assertEquals(Optional.empty(), ctx.shard.committedProgress(P1));
        }
    }

    @Test
    void testAppliedHighWatermarkAtExecutionDeterminesRetryAndNextBatch() throws Exception {
        try (Context ctx = new Context(0, new MockPartitionWriter())) {
            ctx.register(P0);
            CompletableFuture<AppendResponse> first = ctx.append(A, -1, TIMEOUT);
            CompletableFuture<AppendResponse> tooEarly = ctx.append(B, 0, TIMEOUT);
            CompletableFuture<AppendResponse> retry = ctx.runtime.scheduleWriteOperationWithContext("queued-retry", INDEX_PARTITION, TIMEOUT,
                (shard, context) -> shard.prepareAppend(new AppendRequest(A, -1, 3, OWNER), context));
            // Notification is queued before the retry's write event; it must observe committed progress.
            ctx.commit(3);
            assertEquals(INDEXED, first.join().status());
            assertEquals(OUT_OF_ORDER, tooEarly.join().status());
            assertEquals(ALREADY_INDEXED, retry.join().status());
            CompletableFuture<AppendResponse> next = ctx.append(B, 0, TIMEOUT);
            assertFalse(next.isDone());
            ctx.commit(5);
            assertEquals(OptionalLong.of(3), next.join().globalBaseOffset());
            assertEquals(Optional.of(B), ctx.shard.committedProgress(P0));
        }
    }

    @Test
    void testLoadedPendingTailRetryDoesNotAppendAndWaitsForHighWatermark() throws Exception {
        List<CoordinatorRecord> recovered = List.of(fence(P0),
            newBatchIndexRecord(TOPIC, 0, 0, 2, 3, 0), newTopicMetadataRecord(TOPIC, 3));
        try (Context ctx = new Context(0, new MockPartitionWriter(), recovered, 1)) {
            assertEquals(0, ctx.shard.committedNextGlobalOffset(TOPIC));
            assertEquals(3, ctx.shard.nextGlobalOffset(TOPIC));
            CompletableFuture<AppendResponse> retry = ctx.append(A, -1, TIMEOUT);
            assertFalse(retry.isDone());
            assertTrue(ctx.writer.entries(INDEX_PARTITION).isEmpty());
            ctx.commit(3);
            assertEquals(OptionalLong.of(0), retry.join().globalBaseOffset());
            assertEquals(Optional.of(A), ctx.shard.committedProgress(P0));
            assertEquals(0, ctx.shard.pendingAllocationCount());
        }
    }
}
