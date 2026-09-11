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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.CoordinatorState.ACTIVE;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.CoordinatorState.FAILED;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class CoordinatorRuntimeWriteContextTest {
    private static final TopicPartition TP = new TopicPartition("__global_sequence_index", 0);
    private static final Duration TIMEOUT = Duration.ofMillis(100);

    private static class Context {
        final MockTimer timer = new MockTimer();
        final ManualEventProcessor processor = new ManualEventProcessor();
        final MockPartitionWriter writer;
        final List<MockCoordinatorShard> shards = new ArrayList<>();
        final CoordinatorRuntime<MockCoordinatorShard, String> runtime;

        Context() {
            this(new MockPartitionWriter(), new MockCoordinatorLoader(), 0);
        }

        Context(MockPartitionWriter writer, CoordinatorLoader<String> loader, int lingerMs) {
            this.writer = writer;
            runtime = new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time())
                .withTimer(timer)
                .withDefaultWriteTimeOut(TIMEOUT)
                .withLoader(loader)
                .withEventProcessor(processor)
                .withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(() -> new MockCoordinatorShardBuilder() {
                    @Override
                    public MockCoordinatorShard build() {
                        MockCoordinatorShard shard = spy(super.build());
                        shards.add(shard);
                        return shard;
                    }
                })
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class))
                .withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer())
                .withExecutorService(mock(ExecutorService.class))
                .withAppendLingerMs(lingerMs)
                .build();
        }

        void load(int epoch) {
            runtime.scheduleLoadOperation(TP, epoch);
            drain();
        }

        void drain() {
            while (processor.poll()) {
                // Execute only queued events; time advances explicitly in each test.
            }
        }

        MockCoordinatorShard shard() {
            return shards.get(shards.size() - 1);
        }
    }

    @Test
    void testContextCapturedAtExecutionAfterHighWatermarkAndEpochUpdates() {
        Context ctx = new Context();
        ctx.load(10);
        CompletableFuture<String> oldWrite = ctx.runtime.scheduleWriteOperation("legacy", TP, TIMEOUT,
            shard -> new CoordinatorResult<>(List.of("A"), "legacy"));
        ctx.drain();

        ctx.runtime.scheduleLoadOperation(TP, 11);
        CompletableFuture<CoordinatorWriteContext> observed = ctx.runtime.scheduleWriteOperationWithContext("context", TP, TIMEOUT,
            (shard, context) -> {
                assertTrue(ctx.runtime.contextOrThrow(TP).lock.isHeldByCurrentThread());
                return new CoordinatorResult<>(List.of(), context);
            });

        // The HW event is placed before the already queued epoch update and write.
        ctx.writer.commit(TP, 1);
        assertFalse(oldWrite.isDone());
        assertFalse(observed.isDone());
        ctx.drain();
        assertEquals("legacy", oldWrite.join());
        assertEquals(new CoordinatorWriteContext(1, 11), observed.join());

        ctx.load(12);
        assertEquals(new CoordinatorWriteContext(1, 11), observed.join());
        assertEquals(1, ctx.writer.entries(TP).size());
    }

    @Test
    void testContextOperationCannotRunWithoutAnActiveCoordinator() {
        Context ctx = new Context();
        CompletableFuture<Void> future = ctx.runtime.scheduleWriteOperationWithContext("missing", TP, TIMEOUT,
            (shard, context) -> {
                throw new AssertionError("An inactive coordinator must not execute the operation");
            });
        ctx.drain();
        assertFutureThrows(NotCoordinatorException.class, future);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 10})
    void testTimeoutDoesNotRollbackAndEmptyRetryWaitsForCommit(int lingerMs) throws InterruptedException {
        Context ctx = new Context(new MockPartitionWriter(), new MockCoordinatorLoader(), lingerMs);
        ctx.load(10);
        CompletableFuture<String> original = ctx.runtime.scheduleWriteOperationWithContext("original", TP, Duration.ofMillis(3),
            (shard, context) -> new CoordinatorResult<>(List.of("A", "B"), "original"));
        ctx.drain();
        ctx.timer.advanceClock(4);
        ctx.drain();
        assertFutureThrows(TimeoutException.class, original);
        assertEquals(Set.of("A", "B"), ctx.shard().records());
        verify(ctx.shard(), never()).onRollback(anyLong());

        List<Long> committed = new ArrayList<>();
        doAnswer(invocation -> {
            committed.add(invocation.getArgument(0));
            return null;
        }).when(ctx.shard()).onHighWatermarkUpdated(anyLong());
        CompletableFuture<String> retry = ctx.runtime.scheduleWriteOperationWithContext("retry", TP, TIMEOUT,
            (shard, context) -> {
                assertEquals(new CoordinatorWriteContext(0, 10), context);
                assertEquals(Set.of("A", "B"), shard.records());
                return new CoordinatorResult<>(List.of(), "already pending");
            });
        // Check the hook's ordering at the exact point the future is completed.
        CompletableFuture<List<Long>> committedAtCompletion = retry.thenApply(ignored -> List.copyOf(committed));
        ctx.drain();
        assertFalse(retry.isDone());
        if (lingerMs > 0) {
            assertTrue(ctx.writer.entries(TP).isEmpty());
            ctx.timer.advanceClock(lingerMs);
            ctx.drain();
        }
        assertEquals(1, ctx.writer.entries(TP).size());
        assertFalse(retry.isDone());
        ctx.writer.commit(TP, 2);
        ctx.drain();
        assertEquals("already pending", retry.join());
        assertEquals(List.of(2L), committedAtCompletion.join());
        assertFutureThrows(TimeoutException.class, original);
        verify(ctx.shard(), never()).onRollback(anyLong());
    }

    @Test
    void testAppendFailureRestoresSnapshotBeforeRollbackHookAndResponse() {
        Context ctx = new Context(new MockPartitionWriter(1), new MockCoordinatorLoader(), 0);
        ctx.load(10);
        ctx.runtime.scheduleWriteOperation("first", TP, TIMEOUT,
            shard -> new CoordinatorResult<>(List.of("A"), "first"));
        ctx.drain();
        ctx.writer.commit(TP, 1);
        ctx.drain();

        CompletableFuture<String> failed = ctx.runtime.scheduleWriteOperationWithContext("failed", TP, TIMEOUT,
            (shard, context) -> new CoordinatorResult<>(List.of("B"), "failed"));
        doAnswer(invocation -> {
            assertFalse(failed.isDone());
            assertEquals(1L, (long) invocation.getArgument(0));
            assertEquals(Set.of("A"), ctx.shard().records());
            assertEquals(1L, ctx.runtime.contextOrThrow(TP).coordinator.lastWrittenOffset());
            return null;
        }).when(ctx.shard()).onRollback(anyLong());
        ctx.drain();

        assertFutureThrows(KafkaException.class, failed);
        verify(ctx.shard()).onRollback(1);
        assertEquals(ACTIVE, ctx.runtime.contextOrThrow(TP).state);
        assertEquals(1L, ctx.runtime.contextOrThrow(TP).coordinator.lastCommittedOffset());
        assertEquals(1, ctx.writer.entries(TP).size());
    }

    @Test
    void testReplayFailureRollsBackUnwrittenStateAtTheSameOffset() {
        Context ctx = new Context();
        ctx.load(10);
        doThrow(new IllegalArgumentException("replay failed")).when(ctx.shard()).replay(
            1, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, "B");
        doAnswer(invocation -> {
            assertTrue(ctx.shard().records().isEmpty());
            return null;
        }).when(ctx.shard()).onRollback(0);
        CompletableFuture<String> failed = ctx.runtime.scheduleWriteOperationWithContext("failed replay", TP, TIMEOUT,
            (shard, context) -> new CoordinatorResult<>(List.of("A", "B"), "failed"));
        ctx.drain();

        assertFutureThrows(IllegalArgumentException.class, failed);
        verify(ctx.shard()).onRollback(0);
        assertEquals(0L, ctx.runtime.contextOrThrow(TP).coordinator.lastWrittenOffset());
        assertTrue(ctx.writer.entries(TP).isEmpty());
    }

    @Test
    void testStaleHighWatermarkListenerCannotCommitNewShardWrites() {
        Context ctx = new Context();
        ctx.load(10);
        PartitionWriter.Listener oldListener = ctx.runtime.contextOrThrow(TP).highWatermarklistener;
        ctx.runtime.scheduleUnloadOperation(TP, OptionalInt.of(11));
        ctx.drain();
        ctx.load(12);
        CompletableFuture<String> write = ctx.runtime.scheduleWriteOperationWithContext("new owner", TP, TIMEOUT,
            (shard, context) -> new CoordinatorResult<>(List.of("A"), "new owner"));
        ctx.drain();

        oldListener.onHighWatermarkUpdated(TP, 1);
        ctx.drain();
        assertFalse(write.isDone());
        assertEquals(0L, ctx.runtime.contextOrThrow(TP).coordinator.lastCommittedOffset());
        verify(ctx.shard(), never()).onHighWatermarkUpdated(anyLong());

        ctx.writer.commit(TP, 1);
        ctx.drain();
        assertEquals("new owner", write.join());
        verify(ctx.shard()).onHighWatermarkUpdated(1);
    }

    @Test
    void testHighWatermarkHookFailureFailsPendingWritesAndUnloadsShard() {
        Context ctx = new Context();
        ctx.load(10);
        doThrow(new IllegalStateException("commit bookkeeping failed")).when(ctx.shard()).onHighWatermarkUpdated(1);
        CompletableFuture<String> write = ctx.runtime.scheduleWriteOperationWithContext("write", TP, TIMEOUT,
            (shard, context) -> new CoordinatorResult<>(List.of("A"), "write"));
        ctx.drain();
        ctx.writer.commit(TP, 1);
        ctx.drain();

        assertFutureThrows(NotCoordinatorException.class, write);
        assertEquals(FAILED, ctx.runtime.contextOrThrow(TP).state);
        assertNull(ctx.runtime.contextOrThrow(TP).coordinator);
        verify(ctx.shard()).onUnloaded();
    }

    @Test
    void testRollbackHookFailureReleasesBatchAndFailsAllItsWaiters() throws InterruptedException {
        Context ctx = new Context(new MockPartitionWriter(0), new MockCoordinatorLoader(), 10);
        ctx.load(10);
        doThrow(new IllegalStateException("rollback bookkeeping failed")).when(ctx.shard()).onRollback(0);
        CompletableFuture<String> write = ctx.runtime.scheduleWriteOperationWithContext("write", TP, TIMEOUT,
            (shard, context) -> new CoordinatorResult<>(List.of("A"), "write"));
        CompletableFuture<String> waiter = ctx.runtime.scheduleWriteOperationWithContext("waiter", TP, TIMEOUT,
            (shard, context) -> new CoordinatorResult<>(List.of(), "waiter"));
        ctx.drain();
        assertFalse(write.isDone());
        assertFalse(waiter.isDone());
        // MockTimer fires only after the deadline, not at the exact deadline.
        ctx.timer.advanceClock(11);
        ctx.drain();

        assertTrue(write.isDone());
        assertTrue(waiter.isDone());
        assertFutureThrows(KafkaException.class, write);
        assertFutureThrows(KafkaException.class, waiter);
        assertEquals(FAILED, ctx.runtime.contextOrThrow(TP).state);
        assertNull(ctx.runtime.contextOrThrow(TP).currentBatch);
        assertNull(ctx.runtime.contextOrThrow(TP).coordinator);
        verify(ctx.shard()).onUnloaded();
    }

    @Test
    void testTransactionRollbackHookFailureUnloadsShard() {
        Context ctx = new Context(new MockPartitionWriter(true), new MockCoordinatorLoader(), 0);
        ctx.load(10);
        doThrow(new IllegalStateException("rollback bookkeeping failed")).when(ctx.shard()).onRollback(0);
        CompletableFuture<Void> future = ctx.runtime.scheduleTransactionCompletion(
            "transaction", TP, 1, (short) 0, 10, TransactionResult.COMMIT, TIMEOUT);
        ctx.drain();

        assertFutureThrows(KafkaException.class, future);
        assertEquals(FAILED, ctx.runtime.contextOrThrow(TP).state);
        assertNull(ctx.runtime.contextOrThrow(TP).coordinator);
    }

    @Test
    void testLoaderNotifiesCommittedOffsetsBeforeOnLoaded() {
        Context ctx = new Context(new MockPartitionWriter(),
            new MockCoordinatorLoader(null, List.of(5L, 15L, 27L), List.of(5L, 15L)), 0);
        ctx.load(10);
        InOrder order = inOrder(ctx.shard());
        order.verify(ctx.shard()).onHighWatermarkUpdated(5);
        order.verify(ctx.shard()).onHighWatermarkUpdated(15);
        order.verify(ctx.shard()).onLoaded(MetadataImage.EMPTY);

        AtomicReference<CoordinatorWriteContext> observed = new AtomicReference<>();
        CompletableFuture<Void> future = ctx.runtime.scheduleWriteOperationWithContext("loaded context", TP, TIMEOUT,
            (shard, context) -> {
                observed.set(context);
                return new CoordinatorResult<>(List.of());
            });
        ctx.drain();
        assertEquals(new CoordinatorWriteContext(15, 10), observed.get());
        assertFalse(future.isDone()); // The loaded tail through offset 27 is still uncommitted.
    }
}
