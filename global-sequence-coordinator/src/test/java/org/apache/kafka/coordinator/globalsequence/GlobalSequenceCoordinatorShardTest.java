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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorWriteContext;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendResponse;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.IndexerIdentity;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PartitionKey;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PhysicalBatch;
import org.apache.kafka.coordinator.globalsequence.generated.BatchIndexKey;
import org.apache.kafka.coordinator.globalsequence.generated.BatchIndexValue;
import org.apache.kafka.coordinator.globalsequence.generated.TopicMetadataValue;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;

import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newBatchIndexRecord;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newIndexerFenceRecord;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newTopicMetadataRecord;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendStatus.ALREADY_INDEXED;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendStatus.FENCED;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendStatus.INDEXED;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendStatus.OUT_OF_ORDER;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendStatus.OWNER_NOT_COMMITTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalSequenceCoordinatorShardTest {
    private static final Uuid TOPIC = new Uuid(1, 2);
    private static final PartitionKey P0 = new PartitionKey(TOPIC, 0);
    private static final PartitionKey P1 = new PartitionKey(TOPIC, 1);
    private static final IndexerIdentity OWNER = new IndexerIdentity(1, 3, 0, new Uuid(4, 5));
    private static final PhysicalBatch A = batch(P0, 0, 3);
    private static final PhysicalBatch B = batch(P0, 5, 2);
    private static final PhysicalBatch C = batch(P1, 0, 4);

    private static PhysicalBatch batch(PartitionKey partition, long base, int count) {
        return new PhysicalBatch(partition, base, base + count - 1, count);
    }

    private static CoordinatorRecord fence(PartitionKey partition, IndexerIdentity owner) {
        return newIndexerFenceRecord(partition.topicId(), partition.partition(), owner.brokerId(),
            owner.leaderEpoch(), owner.generation(), owner.registrationId());
    }

    private static List<CoordinatorRecord> allocation(PhysicalBatch batch, long globalBaseOffset) {
        return List.of(newBatchIndexRecord(batch.partition().topicId(), batch.partition().partition(),
                batch.baseOffset(), batch.lastOffset(), batch.recordCount(), globalBaseOffset),
            newTopicMetadataRecord(batch.partition().topicId(), globalBaseOffset + batch.recordCount()));
    }

    private static class Context {
        final SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        final GlobalSequenceCoordinatorShard shard = new GlobalSequenceCoordinatorShard(registry);
        long endOffset;
        long highWatermark;

        Context(PartitionKey... partitions) {
            registry.idempotentCreateSnapshot(0);
            for (PartitionKey partition : partitions) {
                replay(List.of(fence(partition, OWNER)));
            }
            commit(endOffset);
        }

        CoordinatorResult<AppendResponse, CoordinatorRecord> prepare(PhysicalBatch batch, long predecessor) {
            return prepare(batch, predecessor, OWNER);
        }

        CoordinatorResult<AppendResponse, CoordinatorRecord> prepare(PhysicalBatch batch, long predecessor, IndexerIdentity owner) {
            return shard.prepareAppend(new AppendRequest(batch, predecessor, batch.resumeOffset(), owner),
                new CoordinatorWriteContext(highWatermark, 10));
        }

        CoordinatorResult<AppendResponse, CoordinatorRecord> append(PhysicalBatch batch, long predecessor) {
            CoordinatorResult<AppendResponse, CoordinatorRecord> result = prepare(batch, predecessor);
            replay(result.records());
            return result;
        }

        void replay(List<CoordinatorRecord> records) {
            for (CoordinatorRecord record : records) {
                shard.replay(endOffset, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, record);
                endOffset++;
            }
            registry.idempotentCreateSnapshot(endOffset);
        }

        void commit(long offset) {
            shard.onHighWatermarkUpdated(offset);
            highWatermark = offset;
            registry.deleteSnapshotsUpTo(offset);
        }

        void rollback(long offset) {
            registry.revertToSnapshot(offset);
            shard.onRollback(offset);
            endOffset = offset;
        }
    }

    @Test
    void testPreparationDoesNotMutateAndAllocationIsAtomic() {
        Context ctx = new Context(P0);
        CoordinatorResult<AppendResponse, CoordinatorRecord> result = ctx.prepare(A, -1);
        assertEquals(INDEXED, result.response().status());
        assertEquals(allocation(A, 0), result.records());
        assertTrue(result.isAtomic());
        assertTrue(result.replayRecords());
        assertEquals(result, ctx.prepare(A, -1));
        assertEquals(0, ctx.shard.nextGlobalOffset(TOPIC));
        assertEquals(0, ctx.shard.pendingAllocationCount());
        assertEquals(Optional.empty(), ctx.shard.committedProgress(P0));

        ctx.replay(result.records());
        assertEquals(3, ctx.shard.nextGlobalOffset(TOPIC));
        assertEquals(0, ctx.shard.committedNextGlobalOffset(TOPIC));
        assertEquals(Optional.empty(), ctx.shard.committedProgress(P0));
        // Even an intermediate boundary cannot publish half of the allocation.
        ctx.commit(2);
        assertEquals(0, ctx.shard.committedNextGlobalOffset(TOPIC));
        ctx.commit(3);
        assertEquals(3, ctx.shard.committedNextGlobalOffset(TOPIC));
        assertEquals(Optional.of(A), ctx.shard.committedProgress(P0));
        assertEquals(0, ctx.shard.pendingAllocationCount());
    }

    @Test
    void testCrossPartitionInterleavingAndPerTopicSequences() {
        PartitionKey otherTopic = new PartitionKey(new Uuid(1, 9), 0);
        Context ctx = new Context(P0, P1, otherTopic);
        assertEquals(OptionalLong.of(0), ctx.append(A, -1).response().globalBaseOffset());
        assertEquals(OptionalLong.of(3), ctx.append(C, -1).response().globalBaseOffset());
        assertEquals(OptionalLong.of(0), ctx.append(batch(otherTopic, 0, 2), -1).response().globalBaseOffset());
        assertEquals(OUT_OF_ORDER, ctx.prepare(B, 0).response().status());
        ctx.commit(ctx.endOffset);
        assertEquals(OptionalLong.of(7), ctx.append(B, 0).response().globalBaseOffset());
        ctx.commit(ctx.endOffset);
        assertEquals(9, ctx.shard.committedNextGlobalOffset(TOPIC));
        assertEquals(2, ctx.shard.committedNextGlobalOffset(otherTopic.topicId()));
    }

    @Test
    void testPredecessorChecksAllowControlGapsButRejectEarlyOrStaleSubmissions() {
        Context ctx = new Context(P0);
        assertEquals(OUT_OF_ORDER, ctx.prepare(B, 0).response().status());
        ctx.append(A, -1);
        ctx.commit(ctx.endOffset);
        assertEquals(OUT_OF_ORDER, ctx.prepare(B, -1).response().status());
        assertEquals(OUT_OF_ORDER, ctx.prepare(B, 1).response().status());
        assertEquals(INDEXED, ctx.append(B, 0).response().status());
        ctx.commit(ctx.endOffset);
        assertEquals(7, ctx.shard.committedProgress(P0).orElseThrow().resumeOffset());

        Context controlPrefix = new Context(P0);
        assertEquals(INDEXED, controlPrefix.append(batch(P0, 10, 2), -1).response().status());
    }

    @Test
    void testPendingRetryReusesAllocationAndOldRetryNeedsNoMapping() {
        Context ctx = new Context(P0);
        ctx.append(A, -1);
        for (int i = 0; i < 3; i++) {
            var retry = ctx.prepare(A, -1);
            assertTrue(retry.records().isEmpty());
            assertEquals(OptionalLong.of(0), retry.response().globalBaseOffset());
            assertEquals(INDEXED, retry.response().status());
            assertEquals(1, ctx.shard.pendingAllocationCount());
            assertEquals(3, ctx.shard.nextGlobalOffset(TOPIC));
        }
        ctx.commit(ctx.endOffset);
        ctx.append(B, 0);
        ctx.commit(ctx.endOffset);
        var retry = ctx.prepare(A, -1);
        assertEquals(ALREADY_INDEXED, retry.response().status());
        assertTrue(retry.records().isEmpty());
        assertEquals(OptionalLong.empty(), retry.response().globalBaseOffset());
        assertEquals(Optional.of(B), retry.response().indexedThrough());
        assertEquals(0, ctx.shard.pendingAllocationCount());
        assertEquals(5, ctx.shard.nextGlobalOffset(TOPIC));
    }

    @Test
    void testKnownBatchConflictsAndOverlapsAreRejected() {
        Context ctx = new Context(P0);
        ctx.append(A, -1);
        assertThrows(IllegalArgumentException.class, () -> ctx.prepare(batch(P0, 0, 2), -1));
        ctx.commit(ctx.endOffset);
        assertThrows(IllegalArgumentException.class, () -> ctx.prepare(batch(P0, 0, 4), -1));
        assertThrows(IllegalArgumentException.class, () -> ctx.prepare(batch(P0, 1, 2), 0));
        ctx.append(B, 0);
        ctx.commit(ctx.endOffset);
        assertThrows(IllegalArgumentException.class, () -> ctx.prepare(batch(P0, 0, 6), -1));
        assertThrows(IllegalArgumentException.class, () -> ctx.prepare(batch(P0, 4, 5), 0));
    }

    @Test
    void testRollbackRestoresSequenceAndProgressWithoutResurrectingCommittedPendingEntries() {
        Context ctx = new Context(P0, P1);
        ctx.append(A, -1);
        long retained = ctx.endOffset;
        ctx.append(C, -1);
        // A becomes committed after the retained snapshot was made.
        ctx.commit(retained);
        ctx.rollback(retained);
        assertEquals(3, ctx.shard.nextGlobalOffset(TOPIC));
        assertEquals(3, ctx.shard.committedNextGlobalOffset(TOPIC));
        assertEquals(Optional.of(A), ctx.shard.committedProgress(P0));
        assertEquals(Optional.empty(), ctx.shard.committedProgress(P1));
        assertEquals(0, ctx.shard.pendingAllocationCount());
        assertEquals(ALREADY_INDEXED, ctx.prepare(A, -1).response().status());
        assertEquals(OptionalLong.of(3), ctx.append(C, -1).response().globalBaseOffset());
    }

    @Test
    void testRollbackOnlyDiscardsSuffixAndPreservesEarlierPendingRetry() {
        Context ctx = new Context(P0, P1);
        long committed = ctx.endOffset;
        ctx.append(A, -1);
        long retained = ctx.endOffset;
        ctx.append(C, -1);
        ctx.rollback(retained);
        assertEquals(committed, ctx.highWatermark);
        assertEquals(1, ctx.shard.pendingAllocationCount());
        assertTrue(ctx.prepare(A, -1).records().isEmpty());
        assertEquals(OUT_OF_ORDER, ctx.prepare(B, 0).response().status());
        ctx.rollback(committed);
        assertEquals(0, ctx.shard.pendingAllocationCount());
        assertEquals(0, ctx.shard.nextGlobalOffset(TOPIC));
        assertEquals(2, ctx.prepare(A, -1).records().size());
    }

    @Test
    void testRollbackOfPartiallyReplayedAllocation() {
        Context ctx = new Context(P0);
        long retained = ctx.endOffset;
        ctx.shard.replay(retained, -1, (short) -1, allocation(A, 0).get(0));
        ctx.registry.revertToSnapshot(retained);
        ctx.shard.onRollback(retained);
        assertEquals(0, ctx.shard.pendingAllocationCount());
        assertEquals(allocation(A, 0), ctx.append(A, -1).records());
    }

    @Test
    void testReplayRestoresCommittedPrefixAndMultiplePendingBatchesInOnePartition() {
        Context ctx = new Context(P0, P1);
        ctx.replay(allocation(A, 0));
        long committed = ctx.endOffset;
        ctx.replay(allocation(C, 3));
        ctx.replay(allocation(B, 7));
        PhysicalBatch next = batch(P0, 9, 1);
        ctx.replay(allocation(next, 9));
        // The loader may replay many batches before reporting its first HW.
        ctx.commit(committed);
        ctx.shard.onLoaded(MetadataImage.EMPTY);
        assertEquals(3, ctx.shard.committedNextGlobalOffset(TOPIC));
        assertEquals(10, ctx.shard.nextGlobalOffset(TOPIC));
        assertEquals(Optional.of(A), ctx.shard.committedProgress(P0));
        assertEquals(OptionalLong.of(7), ctx.prepare(B, 0).response().globalBaseOffset());
        assertEquals(OptionalLong.of(9), ctx.prepare(next, 5).response().globalBaseOffset());
        assertTrue(ctx.prepare(B, 0).records().isEmpty());
        assertEquals(OUT_OF_ORDER, ctx.prepare(batch(P0, 10, 1), 9).response().status());
        assertEquals(3, ctx.shard.pendingAllocationCount());
        ctx.commit(ctx.endOffset);
        assertEquals(Optional.of(next), ctx.shard.committedProgress(P0));
        assertEquals(10, ctx.shard.committedNextGlobalOffset(TOPIC));
        assertEquals(0, ctx.shard.pendingAllocationCount());
    }

    @Test
    void testOwnershipIsRequiredAndPendingFenceBlocksBothOldAndNewOwner() {
        Context ctx = new Context();
        assertEquals(FENCED, ctx.prepare(A, -1).response().status());
        ctx.replay(List.of(fence(P0, OWNER)));
        assertEquals(OWNER_NOT_COMMITTED, ctx.prepare(A, -1).response().status());
        assertEquals(Optional.empty(), ctx.shard.committedIndexer(P0));
        ctx.commit(ctx.endOffset);
        ctx.append(A, -1);
        IndexerIdentity newOwner = new IndexerIdentity(2, 4, 1, new Uuid(7, 8));
        ctx.replay(List.of(fence(P0, newOwner)));
        assertEquals(FENCED, ctx.prepare(A, -1).response().status());
        assertEquals(OWNER_NOT_COMMITTED, ctx.prepare(A, -1, newOwner).response().status());
        assertEquals(Optional.of(OWNER), ctx.shard.committedIndexer(P0));
        ctx.commit(ctx.endOffset);
        assertEquals(Optional.of(newOwner), ctx.shard.committedIndexer(P0));
        assertEquals(ALREADY_INDEXED, ctx.prepare(A, -1, newOwner).response().status());
        assertEquals(FENCED, ctx.prepare(A, -1).response().status());
        assertEquals(3, ctx.shard.nextGlobalOffset(TOPIC));
    }

    @Test
    void testRollbackRestoresOwnership() {
        Context ctx = new Context(P0);
        long retained = ctx.endOffset;
        IndexerIdentity newOwner = new IndexerIdentity(2, 4, 1, new Uuid(7, 8));
        ctx.replay(List.of(fence(P0, newOwner)));
        ctx.rollback(retained);
        assertEquals(INDEXED, ctx.prepare(A, -1).response().status());
        assertEquals(FENCED, ctx.prepare(A, -1, newOwner).response().status());
    }

    @Test
    void testFenceReplayRejectsRegressionAndConflictingGenerations() {
        List<IndexerIdentity> invalid = List.of(
            new IndexerIdentity(2, 4, 0, new Uuid(7, 8)),
            new IndexerIdentity(1, 2, 1, new Uuid(7, 8)),
            new IndexerIdentity(2, 3, 1, new Uuid(7, 8)));
        for (IndexerIdentity identity : invalid) {
            Context ctx = new Context(P0);
            assertThrows(IllegalStateException.class, () -> ctx.replay(List.of(fence(P0, identity))));
            assertEquals(Optional.of(OWNER), ctx.shard.committedIndexer(P0));
        }
        Context ctx = new Context(P0);
        ctx.replay(List.of(fence(P0, OWNER)));
        ctx.commit(ctx.endOffset);
        assertEquals(Optional.of(OWNER), ctx.shard.committedIndexer(P0));
    }

    @Test
    void testOldContextCannotMakeAnAppendDecisionAfterHighWatermarkAdvances() {
        Context ctx = new Context(P0);
        CoordinatorWriteContext stale = new CoordinatorWriteContext(ctx.highWatermark, 10);
        ctx.append(A, -1);
        ctx.commit(ctx.endOffset);
        assertThrows(IllegalStateException.class, () -> ctx.shard.prepareAppend(new AppendRequest(A, -1, 3, OWNER), stale));
        assertEquals(ALREADY_INDEXED, ctx.prepare(A, -1).response().status());
        assertEquals(10, ctx.prepare(A, -1).response().coordinatorLeaderEpoch());
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 0, 2})
    void testBatchMustBeEntirelyBelowSourceHighWatermark(long highWatermark) {
        assertThrows(IllegalArgumentException.class, () -> new AppendRequest(A, -1, highWatermark, OWNER));
    }

    @Test
    void testInputValidation() {
        assertThrows(IllegalArgumentException.class, () -> new PartitionKey(Uuid.ZERO_UUID, 0));
        assertThrows(IllegalArgumentException.class, () -> new PartitionKey(TOPIC, -1));
        assertThrows(NullPointerException.class, () -> new PartitionKey(null, 0));
        assertThrows(IllegalArgumentException.class, () -> new PhysicalBatch(P0, -1, 0, 2));
        assertThrows(IllegalArgumentException.class, () -> new PhysicalBatch(P0, 0, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new PhysicalBatch(P0, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new PhysicalBatch(P0, 1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> batch(P0, Long.MAX_VALUE, 1));
        assertThrows(IllegalArgumentException.class, () -> new AppendRequest(A, 0, 3, OWNER));
        assertThrows(IllegalArgumentException.class, () -> new AppendRequest(A, -2, 3, OWNER));
        assertThrows(IllegalArgumentException.class, () -> new IndexerIdentity(-1, 0, 0, new Uuid(1, 2)));
        assertThrows(IllegalArgumentException.class, () -> new IndexerIdentity(0, -1, 0, new Uuid(1, 2)));
        assertThrows(IllegalArgumentException.class, () -> new IndexerIdentity(0, 0, -1, new Uuid(1, 2)));
        assertThrows(IllegalArgumentException.class, () -> new IndexerIdentity(0, 0, 0, Uuid.ZERO_UUID));
        assertEquals(Long.MAX_VALUE, batch(P0, Long.MAX_VALUE - 1, 1).resumeOffset());
    }

    @Test
    void testReplayRejectsDuplicateReorderedOrMissingGlobalRanges() {
        for (long base : List.of(-1L, 1L, Long.MAX_VALUE)) {
            Context ctx = new Context(P0);
            CoordinatorRecord invalid = CoordinatorRecord.record(new BatchIndexKey().setTopicId(TOPIC).setGlobalBaseOffset(base),
                new ApiMessageAndVersion(new BatchIndexValue().setPhysicalPartition(0).setPhysicalBaseOffset(0)
                    .setPhysicalLastOffset(2).setRecordCount(3), (short) 0));
            assertThrows(IllegalStateException.class, () -> ctx.replay(List.of(invalid)));
        }
        Context ctx = new Context(P0);
        ctx.replay(allocation(A, 0));
        assertThrows(IllegalStateException.class, () -> ctx.replay(allocation(A, 0)));
        assertThrows(IllegalStateException.class, () -> ctx.replay(allocation(A, 3)));
        assertThrows(IllegalStateException.class, () -> ctx.replay(allocation(batch(P0, 2, 2), 3)));
    }

    @Test
    void testReplayRejectsOrphanMissingOrMismatchedMetadata() {
        Context orphan = new Context(P0);
        assertThrows(IllegalStateException.class, () -> orphan.replay(List.of(newTopicMetadataRecord(TOPIC, 0))));
        Context missing = new Context(P0);
        missing.replay(List.of(allocation(A, 0).get(0)));
        assertThrows(IllegalStateException.class, () -> missing.shard.onLoaded(MetadataImage.EMPTY));
        assertThrows(IllegalStateException.class, () -> missing.prepare(A, -1));
        assertThrows(IllegalStateException.class, () -> missing.replay(List.of(fence(P1, OWNER))));
        assertThrows(IllegalStateException.class, () -> missing.replay(List.of(newTopicMetadataRecord(TOPIC, 4))));
        assertThrows(IllegalStateException.class, () -> missing.replay(List.of(newTopicMetadataRecord(new Uuid(8, 9), 3))));
        assertEquals(0, missing.shard.nextGlobalOffset(TOPIC));
        assertEquals(0, missing.shard.pendingAllocationCount());
    }

    @Test
    void testReplayDoesNotKeepMutableGeneratedMessages() {
        Context ctx = new Context(P0);
        List<CoordinatorRecord> records = allocation(A, 0);
        ctx.replay(records);
        ((BatchIndexValue) records.get(0).value().message()).setPhysicalLastOffset(100);
        ((TopicMetadataValue) records.get(1).value().message()).setNextGlobalOffset(100);
        ctx.commit(ctx.endOffset);
        assertEquals(3, ctx.shard.committedNextGlobalOffset(TOPIC));
        assertEquals(Optional.of(A), ctx.shard.committedProgress(P0));
    }

    @Test
    void testReplayRejectsLogGapsTombstonesVersionsAndTransactions() {
        Context ctx = new Context(P0);
        assertThrows(IllegalStateException.class, () -> ctx.shard.replay(2, -1, (short) -1, allocation(A, 0).get(0)));
        assertThrows(IllegalStateException.class, () -> ctx.shard.replay(0, -1, (short) -1, allocation(A, 0).get(0)));
        assertThrows(IllegalStateException.class, () -> ctx.shard.replay(1, 5, (short) 0, allocation(A, 0).get(0)));
        assertThrows(IllegalStateException.class, () -> ctx.shard.replayEndTransactionMarker(5, (short) 0, TransactionResult.ABORT));
        assertThrows(IllegalStateException.class, () -> ctx.replay(List.of(CoordinatorRecord.tombstone(allocation(A, 0).get(0).key()))));
        CoordinatorRecord record = allocation(A, 0).get(0);
        assertThrows(UnsupportedVersionException.class, () -> ctx.replay(List.of(CoordinatorRecord.record(record.key(),
            new ApiMessageAndVersion(record.value().message(), (short) 1)))));
        Context unfenced = new Context();
        assertThrows(IllegalStateException.class, () -> unfenced.replay(allocation(A, 0)));
    }

    @Test
    void testInvalidHighWatermarkAndRollbackBoundaries() {
        Context ctx = new Context(P0);
        assertThrows(IllegalStateException.class, () -> ctx.shard.onHighWatermarkUpdated(0));
        assertThrows(IllegalStateException.class, () -> ctx.shard.onHighWatermarkUpdated(2));
        assertThrows(IllegalStateException.class, () -> ctx.shard.onRollback(0));
        assertThrows(IllegalStateException.class, () -> ctx.shard.onRollback(2));
        ctx.shard.onHighWatermarkUpdated(1);
        assertEquals(Optional.of(OWNER), ctx.shard.committedIndexer(P0));
    }

    @Test
    void testLongHistoryRetainsOnlyLatestProgressAndPendingAllocations() {
        Context ctx = new Context(P0, P1);
        ctx.append(A, -1);
        ctx.commit(ctx.endOffset);
        for (int i = 0; i < 1000; i++) {
            ctx.append(batch(P1, i, 1), i - 1);
            ctx.commit(ctx.endOffset);
            assertEquals(0, ctx.shard.pendingAllocationCount());
        }
        assertEquals(Optional.of(A), ctx.shard.committedProgress(P0));
        assertEquals(ALREADY_INDEXED, ctx.prepare(batch(P1, 0, 1), -1).response().status());
        assertEquals(1003, ctx.shard.committedNextGlobalOffset(TOPIC));
        assertTrue(ctx.registry.epochsList().size() <= 1);
    }

    @Test
    void testDeterministicInterleavingAgainstReferenceSequence() {
        Context ctx = new Context(P0, P1);
        Random random = new Random(7189);
        long[] nextPhysical = {0, 0};
        long[] predecessor = {-1, -1};
        long globalEnd = 0;
        for (int iteration = 0; iteration < 200; iteration++) {
            int partition = random.nextInt(2);
            int count = 1 + random.nextInt(5);
            PhysicalBatch batch = batch(partition == 0 ? P0 : P1, nextPhysical[partition], count);
            var result = ctx.append(batch, predecessor[partition]);
            assertEquals(OptionalLong.of(globalEnd), result.response().globalBaseOffset());
            assertEquals(globalEnd, ctx.shard.committedNextGlobalOffset(TOPIC));
            assertTrue(ctx.prepare(batch, predecessor[partition]).records().isEmpty());
            ctx.commit(ctx.endOffset);
            globalEnd += count;
            assertEquals(globalEnd, ctx.shard.committedNextGlobalOffset(TOPIC));
            assertEquals(ALREADY_INDEXED, ctx.prepare(batch, predecessor[partition]).response().status());
            predecessor[partition] = batch.baseOffset();
            nextPhysical[partition] = batch.resumeOffset() + random.nextInt(3);
            assertFalse(ctx.shard.committedProgress(batch.partition()).isEmpty());
        }
    }
}
