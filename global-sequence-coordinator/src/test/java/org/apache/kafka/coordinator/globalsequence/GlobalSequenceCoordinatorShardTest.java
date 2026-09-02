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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer;
import org.apache.kafka.coordinator.globalsequence.generated.GlobalSequenceIndexLogKey;
import org.apache.kafka.coordinator.globalsequence.generated.GlobalSequenceIndexLogValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class GlobalSequenceCoordinatorShardTest {
    private static final Uuid TOPIC_ID = Uuid.randomUuid();

    private GlobalSequenceStateRegistry stateRegistry;
    private GlobalSequenceCoordinatorShard shard;

    @BeforeEach
    void setUp() {
        LogContext logContext = new LogContext();
        MockTime time = new MockTime();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
        stateRegistry = new GlobalSequenceStateRegistry(snapshotRegistry);

        GlobalSequenceCoordinatorConfig config = new GlobalSequenceCoordinatorConfig(
            new AbstractConfig(GlobalSequenceCoordinatorConfig.CONFIG_DEF, Map.of())
        );
        shard = new GlobalSequenceCoordinatorShard(
            logContext,
            stateRegistry,
            time,
            new MockCoordinatorTimer<>(time),
            config,
            mock(CoordinatorMetrics.class),
            mock(CoordinatorMetricsShard.class)
        );
    }

    @Test
    void testAppendIndexCreatesVersionZeroRecordAndResult() {
        GlobalSequenceAppendRequest request = new GlobalSequenceAppendRequest(
            TOPIC_ID,
            1,
            20L,
            3
        );

        CoordinatorResult<GlobalSequenceAppendResult, CoordinatorRecord> result = shard.appendIndex(request);

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new GlobalSequenceIndexLogKey()
                .setTopicId(TOPIC_ID)
                .setGlobalOffset(0L),
            new ApiMessageAndVersion(
                new GlobalSequenceIndexLogValue()
                    .setRecordsCount(3)
                    .setPartitionIndex(1)
                    .setPartitionOffset(20L),
                (short) 0
            )
        );

        assertEquals(new GlobalSequenceAppendResult(0L, 3, false), result.response());
        assertEquals(1, result.records().size());
        assertEquals(expectedRecord, result.records().get(0));
        assertTrue(result.replayRecords());
        assertFalse(stateRegistry.contains(TOPIC_ID));

        replay(expectedRecord);

        assertEquals(
            new GlobalSequenceLookupResult(List.of(
                new GlobalSequenceIndexRecord(TOPIC_ID, 0L, 3, 1, 20L)
            )),
            shard.lookupIndex(
                new GlobalSequenceLookupRequest(TOPIC_ID, 0L, 3L),
                SnapshotRegistry.LATEST_EPOCH
            )
        );
    }

    @Test
    void testAppendAllocatesContiguousRangesAndDeduplicatesRetry() {
        GlobalSequenceAppendRequest firstRequest = request(TOPIC_ID, 1, 20L, 3);
        CoordinatorResult<GlobalSequenceAppendResult, CoordinatorRecord> first = shard.appendIndex(firstRequest);
        replay(first.records().get(0));

        CoordinatorResult<GlobalSequenceAppendResult, CoordinatorRecord> duplicate = shard.appendIndex(firstRequest);
        assertEquals(new GlobalSequenceAppendResult(0L, 3, true), duplicate.response());
        assertEquals(0, duplicate.records().size());

        CoordinatorResult<GlobalSequenceAppendResult, CoordinatorRecord> second = shard.appendIndex(
            request(TOPIC_ID, 0, 8L, 2)
        );
        assertEquals(new GlobalSequenceAppendResult(3L, 2, false), second.response());
        replay(second.records().get(0));

        assertEquals(
            new GlobalSequenceLookupResult(List.of(
                new GlobalSequenceIndexRecord(TOPIC_ID, 0L, 3, 1, 20L),
                new GlobalSequenceIndexRecord(TOPIC_ID, 3L, 2, 0, 8L)
            )),
            shard.lookupIndex(
                new GlobalSequenceLookupRequest(TOPIC_ID, 0L, 5L),
                SnapshotRegistry.LATEST_EPOCH
            )
        );
    }

    @Test
    void testAppendRejectsConflictingRetryWithoutAdvancing() {
        CoordinatorResult<GlobalSequenceAppendResult, CoordinatorRecord> first = shard.appendIndex(
            request(TOPIC_ID, 1, 20L, 3)
        );
        replay(first.records().get(0));

        assertThrows(
            IllegalArgumentException.class,
            () -> shard.appendIndex(request(TOPIC_ID, 1, 20L, 4))
        );

        CoordinatorResult<GlobalSequenceAppendResult, CoordinatorRecord> next = shard.appendIndex(
            request(TOPIC_ID, 1, 21L, 1)
        );
        assertEquals(new GlobalSequenceAppendResult(3L, 1, false), next.response());
    }

    @Test
    void testReplayRestoresAllocationAndNextOffset() {
        CoordinatorRecord recoveredRecord = coordinatorRecord(
            new GlobalSequenceIndexRecord(TOPIC_ID, 7L, 3, 2, 30L)
        );

        replay(recoveredRecord);

        CoordinatorResult<GlobalSequenceAppendResult, CoordinatorRecord> duplicate = shard.appendIndex(
            request(TOPIC_ID, 2, 30L, 3)
        );
        assertEquals(new GlobalSequenceAppendResult(7L, 3, true), duplicate.response());
        assertEquals(0, duplicate.records().size());

        CoordinatorResult<GlobalSequenceAppendResult, CoordinatorRecord> next = shard.appendIndex(
            request(TOPIC_ID, 0, 40L, 2)
        );
        assertEquals(new GlobalSequenceAppendResult(10L, 2, false), next.response());
    }

    @Test
    void testReplayIsIdempotentAndRejectsConflicts() {
        GlobalSequenceIndexRecord existing = new GlobalSequenceIndexRecord(TOPIC_ID, 0L, 3, 1, 20L);
        CoordinatorRecord record = coordinatorRecord(existing);
        replay(record);
        replay(record);

        assertEquals(
            new GlobalSequenceLookupResult(List.of(existing)),
            shard.lookupIndex(
                new GlobalSequenceLookupRequest(TOPIC_ID, 0L, 3L),
                SnapshotRegistry.LATEST_EPOCH
            )
        );
        assertThrows(
            IllegalStateException.class,
            () -> replay(coordinatorRecord(new GlobalSequenceIndexRecord(TOPIC_ID, 3L, 4, 1, 20L)))
        );
        assertThrows(
            IllegalStateException.class,
            () -> replay(coordinatorRecord(new GlobalSequenceIndexRecord(TOPIC_ID, 0L, 3, 2, 50L)))
        );
        assertThrows(
            IllegalStateException.class,
            () -> replay(coordinatorRecord(new GlobalSequenceIndexRecord(TOPIC_ID, 2L, 2, 3, 60L)))
        );
        assertEquals(
            new GlobalSequenceLookupResult(List.of(existing)),
            shard.lookupIndex(
                new GlobalSequenceLookupRequest(TOPIC_ID, 0L, 3L),
                SnapshotRegistry.LATEST_EPOCH
            )
        );
    }

    @Test
    void testReplayTombstoneRemovesAllocationWithoutRewinding() {
        replay(coordinatorRecord(new GlobalSequenceIndexRecord(TOPIC_ID, 0L, 3, 1, 20L)));

        replay(CoordinatorRecord.tombstone(
            new GlobalSequenceIndexLogKey()
                .setTopicId(TOPIC_ID)
                .setGlobalOffset(0L)
        ));
        replay(CoordinatorRecord.tombstone(
            new GlobalSequenceIndexLogKey()
                .setTopicId(TOPIC_ID)
                .setGlobalOffset(0L)
        ));

        assertThrows(
            OffsetOutOfRangeException.class,
            () -> shard.lookupIndex(
                new GlobalSequenceLookupRequest(TOPIC_ID, 0L, 3L),
                SnapshotRegistry.LATEST_EPOCH
            )
        );
        CoordinatorResult<GlobalSequenceAppendResult, CoordinatorRecord> replacement = shard.appendIndex(
            request(TOPIC_ID, 1, 20L, 3)
        );
        assertEquals(new GlobalSequenceAppendResult(3L, 3, false), replacement.response());
    }

    @Test
    void testTopicsHaveIndependentSequences() {
        Uuid otherTopicId = Uuid.randomUuid();
        CoordinatorResult<GlobalSequenceAppendResult, CoordinatorRecord> first = shard.appendIndex(
            request(TOPIC_ID, 0, 0L, 2)
        );
        replay(first.records().get(0));
        CoordinatorResult<GlobalSequenceAppendResult, CoordinatorRecord> other = shard.appendIndex(
            request(otherTopicId, 0, 0L, 4)
        );

        assertEquals(0L, first.response().globalBaseOffset());
        assertEquals(0L, other.response().globalBaseOffset());
    }

    @Test
    void testLookupIndexUsesReplayedState() {
        GlobalSequenceIndexRecord first = new GlobalSequenceIndexRecord(TOPIC_ID, 0L, 3, 1, 20L);
        GlobalSequenceIndexRecord second = new GlobalSequenceIndexRecord(TOPIC_ID, 3L, 2, 0, 8L);
        replay(coordinatorRecord(first));
        replay(coordinatorRecord(second));

        assertEquals(
            new GlobalSequenceLookupResult(List.of(first, second)),
            shard.lookupIndex(
                new GlobalSequenceLookupRequest(TOPIC_ID, 1L, 5L),
                SnapshotRegistry.LATEST_EPOCH
            )
        );
    }

    private void replay(CoordinatorRecord record) {
        shard.replay(
            0L,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            record
        );
    }

    private static GlobalSequenceAppendRequest request(
        Uuid topicId,
        int partitionIndex,
        long partitionBaseOffset,
        int recordCount
    ) {
        return new GlobalSequenceAppendRequest(
            topicId,
            partitionIndex,
            partitionBaseOffset,
            recordCount
        );
    }

    private static CoordinatorRecord coordinatorRecord(GlobalSequenceIndexRecord indexRecord) {
        return CoordinatorRecord.record(
            new GlobalSequenceIndexLogKey()
                .setTopicId(indexRecord.topicId())
                .setGlobalOffset(indexRecord.globalBaseOffset()),
            new ApiMessageAndVersion(
                new GlobalSequenceIndexLogValue()
                    .setRecordsCount(indexRecord.recordCount())
                    .setPartitionIndex(indexRecord.partitionIndex())
                    .setPartitionOffset(indexRecord.partitionBaseOffset()),
                (short) 0
            )
        );
    }
}
