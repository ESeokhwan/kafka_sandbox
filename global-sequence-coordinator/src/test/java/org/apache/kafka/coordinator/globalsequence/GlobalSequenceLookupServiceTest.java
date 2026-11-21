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
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.coordinator.common.runtime.CoordinatorReadContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime.CoordinatorState;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorServiceTest.Context;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.CommittedTopic;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PartitionKey;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PhysicalBatch;
import org.apache.kafka.server.immutable.ImmutableMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.common.internals.Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorServiceTest.image;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class GlobalSequenceLookupServiceTest {
    private static final Uuid TOPIC = new Uuid(1, 2);
    private static final Uuid INDEX = new Uuid(3, 4);
    private static final TopicPartition INDEX_PARTITION = new TopicPartition(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, 1);
    private static final PhysicalBatch BATCH = new PhysicalBatch(new PartitionKey(TOPIC, 0), 0, 2, 3);
    private static final GlobalSequenceLookup.Position FIRST = new GlobalSequenceLookup.Position(0, 4);

    private static GlobalSequenceCoordinatorShard shard() {
        GlobalSequenceCoordinatorShard shard = mock(GlobalSequenceCoordinatorShard.class);
        GlobalSequenceSparseIndex positions = new GlobalSequenceSparseIndex(1, 10, 10);
        positions.add(FIRST);
        when(shard.lookupGeneration()).thenReturn(new Object());
        when(shard.committedTopics()).thenReturn(ImmutableMap.singleton(TOPIC, new CommittedTopic(3, positions)));
        return shard;
    }

    private static void publish(Context ctx, GlobalSequenceCoordinatorShard shard, int epoch, long highWatermark) {
        ctx.lookupPartitions.onUpdate(INDEX_PARTITION, CoordinatorState.ACTIVE, new CoordinatorReadContext(highWatermark, epoch), shard);
    }

    @Test
    void testLookupUsesPublishedBoundaryWhileCommitAdvancesWithoutCallingRuntime() {
        Context ctx = new Context();
        ctx.start();
        ctx.publish(image(2, true));
        clearInvocations(ctx.runtime);
        GlobalSequenceCoordinatorShard shard = shard();
        publish(ctx, shard, 3, 6);

        CompletableFuture<GlobalSequenceLookup.Result> scan = new CompletableFuture<>();
        when(ctx.reader.read(any(), any(), any(), any(), anyLong())).thenReturn(scan);
        var request = new GlobalSequenceLookup.Request(TOPIC, 1, 100, 2, 1000);
        long start = ctx.time.nanoseconds();
        var result = ctx.service.lookupIndex(request, 3);
        var snapshot = ArgumentCaptor.forClass(GlobalSequenceLookup.Snapshot.class);
        Object generation = shard.lookupGeneration();
        verify(ctx.reader).read(eq(request), snapshot.capture(), eq(FIRST), eq(generation), eq(start + 1_000_000_000L));
        assertEquals(new GlobalSequenceLookup.Snapshot(INDEX, 1, 3, 6, 3), snapshot.getValue());
        assertFalse(result.isDone());

        GlobalSequenceSparseIndex positions = shard.committedTopics().get(TOPIC).positions();
        positions.add(new GlobalSequenceLookup.Position(3, 6));
        when(shard.committedTopics()).thenReturn(ImmutableMap.singleton(TOPIC, new CommittedTopic(6, positions)));
        publish(ctx, shard, 3, 8);
        var page = new GlobalSequenceLookup.Result(snapshot.getValue(), List.of(new GlobalSequenceLookup.Mapping(0, BATCH)), 3);
        scan.complete(page);

        assertSame(page, result.join());
        verifyNoInteractions(ctx.runtime);
        ctx.service.shutdown();
        verify(ctx.reader).close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"epoch", "reload", "unload", "hw", "topic", "resignation", "timeout"})
    void testLookupFencesInFlightRead(String change) {
        Context ctx = new Context();
        ctx.start();
        ctx.publish(image(2, true));
        GlobalSequenceCoordinatorShard shard = shard();
        publish(ctx, shard, 3, 6);

        CompletableFuture<GlobalSequenceLookup.Result> scan = new CompletableFuture<>();
        when(ctx.reader.read(any(), any(), any(), any(), anyLong())).thenReturn(scan);
        var result = ctx.service.lookupIndex(new GlobalSequenceLookup.Request(TOPIC, 0, 3, 1, 1000), 3);
        switch (change) {
            case "epoch" -> {
                ctx.publish(image(2, true, 1, 4));
                publish(ctx, shard, 4, 6);
            }
            case "reload" -> publish(ctx, shard(), 3, 6);
            case "unload" -> ctx.lookupPartitions.onUpdate(INDEX_PARTITION, CoordinatorState.CLOSED, null, null);
            case "hw" -> publish(ctx, shard, 3, 5);
            case "topic" -> ctx.publish(image(2, false));
            case "resignation" -> ctx.publish(image(2, true, 2, 4));
            case "timeout" -> ctx.time.sleep(1000);
            default -> throw new AssertionError(change);
        }
        scan.complete(new GlobalSequenceLookup.Result(new GlobalSequenceLookup.Snapshot(INDEX, 1, 3, 6, 3),
            List.of(new GlobalSequenceLookup.Mapping(0, BATCH)), 3));
        Class<? extends RuntimeException> error = switch (change) {
            case "timeout" -> TimeoutException.class;
            case "topic" -> InvalidRequestException.class;
            default -> NotCoordinatorException.class;
        };
        assertFutureThrows(error, result);
        ctx.service.shutdown();
    }

    @Test
    void testLookupRejectsInactiveLoadingOutOfRangeAndStaleEpochBeforeScanning() {
        Context ctx = new Context();
        ctx.start();
        ctx.publish(image(2, true));
        var request = new GlobalSequenceLookup.Request(TOPIC, 4, 4, 1, 1000);
        assertFutureThrows(NotCoordinatorException.class, ctx.service.lookupIndex(request, 3));
        ctx.lookupPartitions.onUpdate(INDEX_PARTITION, CoordinatorState.LOADING, null, null);
        assertFutureThrows(CoordinatorLoadInProgressException.class, ctx.service.lookupIndex(request, 3));
        publish(ctx, shard(), 3, 6);
        assertFutureThrows(OffsetOutOfRangeException.class, ctx.service.lookupIndex(request, 3));
        assertFutureThrows(NotCoordinatorException.class, ctx.service.lookupIndex(request, 2));
        verify(ctx.reader, never()).read(any(), any(), any(), any(), anyLong());
        ctx.service.shutdown();
    }
}
