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
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.globalsequence.metrics.GlobalSequenceCoordinatorMetrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GlobalSequenceCoordinatorTest {
    private static final int COMMIT_TIMEOUT_MS = 1234;
    private static final int NUM_PARTITIONS = 7;

    private CoordinatorRuntime<GlobalSequenceCoordinatorShard, CoordinatorRecord> runtime;
    private GlobalSequenceCoordinatorMetrics metrics;
    private GlobalSequenceCoordinator coordinator;

    @BeforeEach
    void setUp() {
        runtime = mockRuntime();
        metrics = mock(GlobalSequenceCoordinatorMetrics.class);
        coordinator = new GlobalSequenceCoordinator(
            new LogContext(),
            config(),
            runtime,
            metrics
        );
    }

    @Test
    void testAppendIndexRejectsRequestWhenCoordinatorIsInactive() {
        assertThrows(CoordinatorNotAvailableException.class, () -> coordinator.appendIndex(request()));
    }

    @Test
    void testAppendIndexRoutesAndDelegatesWithConfiguredTimeout() {
        GlobalSequenceAppendRequest request = request();
        GlobalSequenceAppendResult expectedResponse = new GlobalSequenceAppendResult(10L, request.recordCount(), false);
        CompletableFuture<GlobalSequenceAppendResult> expectedFuture =
            CompletableFuture.completedFuture(expectedResponse);
        TopicPartition expectedTopicPartition = new TopicPartition(
            Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME,
            Utils.abs(request.topicId().hashCode()) % NUM_PARTITIONS
        );

        when(runtime.<GlobalSequenceAppendResult>scheduleWriteOperation(
            eq("append-global-sequence-index"),
            eq(expectedTopicPartition),
            eq(Duration.ofMillis(COMMIT_TIMEOUT_MS)),
            any()
        )).thenReturn(expectedFuture);

        coordinator.startup(() -> NUM_PARTITIONS);

        assertSame(expectedFuture, coordinator.appendIndex(request));

        ArgumentCaptor<CoordinatorRuntime.CoordinatorWriteOperation<
            GlobalSequenceCoordinatorShard,
            GlobalSequenceAppendResult,
            CoordinatorRecord
            >> operationCaptor = writeOperationCaptor();
        verify(runtime).scheduleWriteOperation(
            eq("append-global-sequence-index"),
            eq(expectedTopicPartition),
            eq(Duration.ofMillis(COMMIT_TIMEOUT_MS)),
            operationCaptor.capture()
        );

        GlobalSequenceCoordinatorShard shard = mock(GlobalSequenceCoordinatorShard.class);
        CoordinatorResult<GlobalSequenceAppendResult, CoordinatorRecord> expectedResult =
            new CoordinatorResult<>(List.of(), expectedResponse);
        when(shard.appendIndex(request)).thenReturn(expectedResult);

        assertSame(expectedResult, operationCaptor.getValue().generateRecordsAndResult(shard));
    }

    @Test
    void testElectionAndResignationDelegateToRuntime() {
        assertThrows(CoordinatorNotAvailableException.class, () -> coordinator.onElection(3, 8));
        assertThrows(
            CoordinatorNotAvailableException.class,
            () -> coordinator.onResignation(3, OptionalInt.of(8))
        );

        coordinator.startup(() -> NUM_PARTITIONS);
        coordinator.onElection(3, 8);
        coordinator.onResignation(3, OptionalInt.of(9));
        coordinator.onResignation(4, OptionalInt.empty());

        verify(runtime).scheduleLoadOperation(
            new TopicPartition(Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, 3),
            8
        );
        verify(runtime).scheduleUnloadOperation(
            new TopicPartition(Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, 3),
            OptionalInt.of(9)
        );
        verify(runtime).scheduleUnloadOperation(
            new TopicPartition(Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, 4),
            OptionalInt.empty()
        );
    }

    @Test
    void testStartupAndShutdownAreIdempotent() throws Exception {
        coordinator.startup(() -> NUM_PARTITIONS);
        coordinator.startup(() -> NUM_PARTITIONS + 1);

        coordinator.shutdown();
        coordinator.shutdown();

        verify(runtime, times(1)).close();
        verify(metrics, times(1)).close();
        assertThrows(CoordinatorNotAvailableException.class, () -> coordinator.appendIndex(request()));
    }

    private static GlobalSequenceCoordinatorConfig config() {
        return new GlobalSequenceCoordinatorConfig(new AbstractConfig(
            GlobalSequenceCoordinatorConfig.CONFIG_DEF,
            Map.of(GlobalSequenceCoordinatorConfig.COMMIT_TIMEOUT_MS_CONFIG, COMMIT_TIMEOUT_MS)
        ));
    }

    private static GlobalSequenceAppendRequest request() {
        return new GlobalSequenceAppendRequest(Uuid.randomUuid(), 2, 100L, 3);
    }

    @SuppressWarnings("unchecked")
    private static CoordinatorRuntime<GlobalSequenceCoordinatorShard, CoordinatorRecord> mockRuntime() {
        return (CoordinatorRuntime<GlobalSequenceCoordinatorShard, CoordinatorRecord>) mock(CoordinatorRuntime.class);
    }

    @SuppressWarnings("unchecked")
    private static ArgumentCaptor<CoordinatorRuntime.CoordinatorWriteOperation<
        GlobalSequenceCoordinatorShard,
        GlobalSequenceAppendResult,
        CoordinatorRecord
        >> writeOperationCaptor() {
        return ArgumentCaptor.forClass(CoordinatorRuntime.CoordinatorWriteOperation.class);
    }
}
