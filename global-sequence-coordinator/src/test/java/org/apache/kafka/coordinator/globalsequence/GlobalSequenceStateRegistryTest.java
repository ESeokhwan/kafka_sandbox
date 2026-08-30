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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalSequenceStateRegistryTest {
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final Uuid OTHER_TOPIC_ID = Uuid.randomUuid();

    @Test
    void testPrepareAppendDoesNotCommitOrAdvanceState() {
        GlobalSequenceStateRegistry registry = newRegistry();
        GlobalSequenceAppendRequest request = request(TOPIC_ID, 0, 10L, 3);

        GlobalSequenceStateRegistry.PreparedAppend first = registry.prepareAppend(request);
        GlobalSequenceStateRegistry.PreparedAppend second = registry.prepareAppend(request);

        GlobalSequenceIndexRecord expected = record(TOPIC_ID, 0L, 3, 0, 10L);
        assertEquals(new GlobalSequenceStateRegistry.PreparedAppend(expected, false), first);
        assertEquals(first, second);
        assertFalse(registry.contains(TOPIC_ID));
    }

    @Test
    void testReplayCommitsPreparedAppendAndAdvancesNextOffset() {
        GlobalSequenceStateRegistry registry = newRegistry();
        GlobalSequenceAppendRequest firstRequest = request(TOPIC_ID, 0, 10L, 3);
        GlobalSequenceIndexRecord first = registry.prepareAppend(firstRequest).indexRecord();

        registry.replay(first);

        assertArrayEquals(
            new GlobalSequenceIndexRecord[] {first},
            registry.getState(TOPIC_ID).getSequenceIndexAsArray()
        );
        assertEquals(3L, registry.getState(TOPIC_ID).nextGlobalOffset());

        GlobalSequenceStateRegistry.PreparedAppend second = registry.prepareAppend(
            request(TOPIC_ID, 1, 20L, 2)
        );
        assertEquals(record(TOPIC_ID, 3L, 2, 1, 20L), second.indexRecord());
        assertFalse(second.duplicate());
    }

    @Test
    void testDuplicateReplayAndPrepareAreIdempotentButConflictingRetryIsRejected() {
        GlobalSequenceStateRegistry registry = newRegistry();
        GlobalSequenceAppendRequest request = request(TOPIC_ID, 0, 10L, 3);
        GlobalSequenceIndexRecord indexRecord = record(TOPIC_ID, 0L, 3, 0, 10L);

        registry.replay(indexRecord);
        registry.replay(indexRecord);

        GlobalSequenceStateRegistry.PreparedAppend duplicate = registry.prepareAppend(request);
        assertEquals(new GlobalSequenceStateRegistry.PreparedAppend(indexRecord, true), duplicate);
        assertArrayEquals(
            new GlobalSequenceIndexRecord[] {indexRecord},
            registry.getState(TOPIC_ID).getSequenceIndexAsArray()
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> registry.prepareAppend(request(TOPIC_ID, 0, 10L, 4))
        );
    }

    @Test
    void testTopicsHaveIndependentIndexesAndOffsets() {
        GlobalSequenceStateRegistry registry = newRegistry();
        GlobalSequenceIndexRecord firstTopicRecord = registry.prepareAppend(
            request(TOPIC_ID, 0, 10L, 3)
        ).indexRecord();
        registry.replay(firstTopicRecord);

        GlobalSequenceStateRegistry.PreparedAppend otherTopicAppend = registry.prepareAppend(
            request(OTHER_TOPIC_ID, 0, 10L, 2)
        );

        assertEquals(record(OTHER_TOPIC_ID, 0L, 2, 0, 10L), otherTopicAppend.indexRecord());
        assertFalse(otherTopicAppend.duplicate());
        assertFalse(registry.contains(OTHER_TOPIC_ID));

        registry.replay(otherTopicAppend.indexRecord());

        assertArrayEquals(
            new GlobalSequenceIndexRecord[] {firstTopicRecord},
            registry.getState(TOPIC_ID).getSequenceIndexAsArray()
        );
        assertArrayEquals(
            new GlobalSequenceIndexRecord[] {otherTopicAppend.indexRecord()},
            registry.getState(OTHER_TOPIC_ID).getSequenceIndexAsArray()
        );
        assertEquals(3L, registry.getState(TOPIC_ID).nextGlobalOffset());
        assertEquals(2L, registry.getState(OTHER_TOPIC_ID).nextGlobalOffset());
    }

    @Test
    void testTombstoneRemovesIndexesWithoutRewindingWatermark() {
        GlobalSequenceStateRegistry registry = newRegistry();
        GlobalSequenceAppendRequest request = request(TOPIC_ID, 0, 10L, 3);
        GlobalSequenceIndexRecord indexRecord = registry.prepareAppend(request).indexRecord();
        registry.replay(indexRecord);

        registry.replayTombstone(TOPIC_ID, indexRecord.globalBaseOffset());

        assertArrayEquals(
            new GlobalSequenceIndexRecord[0],
            registry.getState(TOPIC_ID).getSequenceIndexAsArray()
        );
        assertEquals(3L, registry.getState(TOPIC_ID).nextGlobalOffset());

        GlobalSequenceStateRegistry.PreparedAppend afterTombstone = registry.prepareAppend(request);
        assertEquals(record(TOPIC_ID, 3L, 3, 0, 10L), afterTombstone.indexRecord());
        assertFalse(afterTombstone.duplicate());

        registry.replayTombstone(OTHER_TOPIC_ID, 100L);
        assertFalse(registry.contains(OTHER_TOPIC_ID));
    }

    @Test
    void testReplayRejectsPhysicalBatchConflictsGlobalOffsetCollisionsAndOverlaps() {
        GlobalSequenceStateRegistry registry = newRegistry();
        GlobalSequenceIndexRecord existing = record(TOPIC_ID, 5L, 5, 0, 10L);
        registry.replay(existing);

        assertThrows(
            IllegalStateException.class,
            () -> registry.replay(record(TOPIC_ID, 10L, 1, 0, 10L))
        );
        assertThrows(
            IllegalStateException.class,
            () -> registry.replay(record(TOPIC_ID, 5L, 2, 1, 20L))
        );
        assertThrows(
            IllegalStateException.class,
            () -> registry.replay(record(TOPIC_ID, 9L, 2, 1, 20L))
        );

        assertArrayEquals(
            new GlobalSequenceIndexRecord[] {existing},
            registry.getState(TOPIC_ID).getSequenceIndexAsArray()
        );
        assertEquals(10L, registry.getState(TOPIC_ID).nextGlobalOffset());
    }

    @Test
    void testSnapshotRollbackRestoresIndexesTopicsAndWatermark() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        GlobalSequenceStateRegistry registry = new GlobalSequenceStateRegistry(snapshotRegistry);
        GlobalSequenceIndexRecord first = record(TOPIC_ID, 0L, 2, 0, 10L);
        registry.replay(first);
        snapshotRegistry.idempotentCreateSnapshot(0L);

        registry.replay(record(TOPIC_ID, 2L, 3, 1, 20L));
        registry.replayTombstone(TOPIC_ID, first.globalBaseOffset());
        registry.replay(record(OTHER_TOPIC_ID, 0L, 4, 0, 30L));

        snapshotRegistry.revertToSnapshot(0L);

        assertArrayEquals(
            new GlobalSequenceIndexRecord[] {first},
            registry.getState(TOPIC_ID).getSequenceIndexAsArray()
        );
        assertEquals(2L, registry.getState(TOPIC_ID).nextGlobalOffset());
        assertTrue(registry.prepareAppend(request(TOPIC_ID, 0, 10L, 2)).duplicate());
        assertFalse(registry.contains(OTHER_TOPIC_ID));
    }

    @Test
    void testExhaustionStillAllowsAnExistingAllocationRetry() {
        GlobalSequenceStateRegistry registry = newRegistry();
        GlobalSequenceIndexRecord finalAllocation = record(
            TOPIC_ID,
            Long.MAX_VALUE - 1,
            1,
            0,
            10L
        );
        registry.replay(finalAllocation);

        GlobalSequenceStateRegistry.PreparedAppend duplicate = registry.prepareAppend(
            request(TOPIC_ID, 0, 10L, 1)
        );
        assertTrue(duplicate.duplicate());
        assertEquals(finalAllocation, duplicate.indexRecord());
        assertThrows(
            ArithmeticException.class,
            () -> registry.prepareAppend(request(TOPIC_ID, 0, 11L, 1))
        );
        assertArrayEquals(
            new GlobalSequenceIndexRecord[] {finalAllocation},
            registry.getState(TOPIC_ID).getSequenceIndexAsArray()
        );
        assertEquals(Long.MAX_VALUE, registry.getState(TOPIC_ID).nextGlobalOffset());
    }

    private static GlobalSequenceStateRegistry newRegistry() {
        return new GlobalSequenceStateRegistry(new SnapshotRegistry(new LogContext()));
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

    private static GlobalSequenceIndexRecord record(
        Uuid topicId,
        long globalBaseOffset,
        int recordCount,
        int partitionIndex,
        long partitionBaseOffset
    ) {
        return new GlobalSequenceIndexRecord(
            topicId,
            globalBaseOffset,
            recordCount,
            partitionIndex,
            partitionBaseOffset
        );
    }
}
