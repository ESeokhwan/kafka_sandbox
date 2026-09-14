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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.coordinator.common.runtime.CoordinatorReadContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.CoordinatorReadOperationWithContext;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorServiceTest.Context;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PartitionKey;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PhysicalBatch;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorServiceTest.image;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class GlobalSequenceLookupServiceTest {
    private static final Uuid TOPIC = new Uuid(1, 2);
    private static final Uuid INDEX = new Uuid(3, 4);
    private static final PhysicalBatch BATCH = new PhysicalBatch(new PartitionKey(TOPIC, 0), 0, 2, 3);

    @Test
    void testLookupCapturesCommittedSnapshotWithoutWaitingForPendingWrites() throws Exception {
        Context ctx = new Context();
        ctx.start();
        ctx.publish(image(2, true));
        GlobalSequenceCoordinatorShard shard = mock(GlobalSequenceCoordinatorShard.class);
        when(shard.committedNextGlobalOffset(TOPIC)).thenReturn(3L);
        when(ctx.runtime.scheduleReadOperationWithContext(anyString(), any(), any())).thenAnswer(invocation -> {
            CoordinatorReadOperationWithContext<GlobalSequenceCoordinatorShard, Object> op = invocation.getArgument(2);
            return CompletableFuture.completedFuture(op.generateResponse(shard, new CoordinatorReadContext(6L, 10)));
        });
        CompletableFuture<GlobalSequenceLookup.Result> scan = new CompletableFuture<>();
        when(ctx.reader.read(any(), any(), anyLong())).thenReturn(scan);
        GlobalSequenceLookup.Request request = new GlobalSequenceLookup.Request(TOPIC, 1, 100, 2, 1000);
        long start = ctx.time.nanoseconds();
        CompletableFuture<GlobalSequenceLookup.Result> result = ctx.service.lookupIndex(request, 10);
        ArgumentCaptor<GlobalSequenceLookup.Snapshot> snapshot = ArgumentCaptor.forClass(GlobalSequenceLookup.Snapshot.class);
        verify(ctx.reader).read(eq(request), snapshot.capture(), eq(start + 1000000000L));
        assertEquals(new GlobalSequenceLookup.Snapshot(INDEX, 1, 10, 6, 3), snapshot.getValue());
        assertFalse(result.isDone());
        when(shard.committedNextGlobalOffset(TOPIC)).thenReturn(6L);
        GlobalSequenceLookup.Result page = new GlobalSequenceLookup.Result(snapshot.getValue(),
            List.of(new GlobalSequenceLookup.Mapping(0, BATCH)), 3);
        scan.complete(page);
        assertSame(page, result.get());
        ctx.service.shutdown();
        verify(ctx.reader).close();
    }

    @Test
    void testLookupRejectsChangedEpochShardWatermarkTopicAndExpiredDeadline() {
        for (String change : List.of("epoch", "shard", "hw", "topic", "timeout")) {
            Context ctx = new Context();
            ctx.start();
            ctx.publish(image(2, true));
            GlobalSequenceCoordinatorShard initial = mock(GlobalSequenceCoordinatorShard.class);
            when(initial.committedNextGlobalOffset(TOPIC)).thenReturn(3L);
            java.util.concurrent.atomic.AtomicReference<GlobalSequenceCoordinatorShard> shard = new java.util.concurrent.atomic.AtomicReference<>(initial);
            java.util.concurrent.atomic.AtomicReference<CoordinatorReadContext> context =
                new java.util.concurrent.atomic.AtomicReference<>(new CoordinatorReadContext(6, 10));
            when(ctx.runtime.scheduleReadOperationWithContext(anyString(), any(), any())).thenAnswer(invocation -> {
                CoordinatorReadOperationWithContext<GlobalSequenceCoordinatorShard, Object> op = invocation.getArgument(2);
                try {
                    return CompletableFuture.completedFuture(op.generateResponse(shard.get(), context.get()));
                } catch (Exception error) {
                    return CompletableFuture.failedFuture(error);
                }
            });
            CompletableFuture<GlobalSequenceLookup.Result> scan = new CompletableFuture<>();
            when(ctx.reader.read(any(), any(), anyLong())).thenReturn(scan);
            CompletableFuture<GlobalSequenceLookup.Result> result = ctx.service.lookupIndex(new GlobalSequenceLookup.Request(TOPIC, 0, 3, 1, 1000), 10);
            switch (change) {
                case "epoch" -> context.set(new CoordinatorReadContext(6, 11));
                case "shard" -> shard.set(mock(GlobalSequenceCoordinatorShard.class));
                case "hw" -> context.set(new CoordinatorReadContext(5, 10));
                case "topic" -> ctx.publish(image(2, false));
                case "timeout" -> ctx.time.sleep(1000);
                default -> throw new AssertionError(change);
            }
            scan.complete(new GlobalSequenceLookup.Result(new GlobalSequenceLookup.Snapshot(INDEX, 1, 10, 6, 3),
                List.of(new GlobalSequenceLookup.Mapping(0, BATCH)), 3));
            if (change.equals("timeout")) assertFutureThrows(org.apache.kafka.common.errors.TimeoutException.class, result);
            else if (change.equals("topic")) assertFutureThrows(InvalidRequestException.class, result);
            else assertFutureThrows(NotCoordinatorException.class, result);
            ctx.service.shutdown();
        }
    }

    @Test
    void testLookupRejectsStartAfterCommittedEndAndStaleEpochBeforeScanning() {
        Context ctx = new Context();
        ctx.start();
        ctx.publish(image(2, true));
        GlobalSequenceCoordinatorShard shard = mock(GlobalSequenceCoordinatorShard.class);
        when(shard.committedNextGlobalOffset(TOPIC)).thenReturn(3L);
        when(ctx.runtime.scheduleReadOperationWithContext(anyString(), any(), any())).thenAnswer(invocation -> {
            CoordinatorReadOperationWithContext<GlobalSequenceCoordinatorShard, Object> op = invocation.getArgument(2);
            try {
                return CompletableFuture.completedFuture(op.generateResponse(shard, new CoordinatorReadContext(6, 10)));
            } catch (Exception error) {
                return CompletableFuture.failedFuture(error);
            }
        });
        assertFutureThrows(org.apache.kafka.common.errors.OffsetOutOfRangeException.class,
            ctx.service.lookupIndex(new GlobalSequenceLookup.Request(TOPIC, 4, 4, 1, 1000), 10));
        assertFutureThrows(NotCoordinatorException.class,
            ctx.service.lookupIndex(new GlobalSequenceLookup.Request(TOPIC, 0, 3, 1, 1000), 9));
        verify(ctx.reader, never()).read(any(), any(), anyLong());
        ctx.service.shutdown();
    }

}
