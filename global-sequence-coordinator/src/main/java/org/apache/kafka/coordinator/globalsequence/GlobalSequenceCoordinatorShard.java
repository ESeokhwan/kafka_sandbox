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
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorWriteContext;
import org.apache.kafka.coordinator.globalsequence.generated.BatchIndexKey;
import org.apache.kafka.coordinator.globalsequence.generated.BatchIndexValue;
import org.apache.kafka.coordinator.globalsequence.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.globalsequence.generated.IndexerFenceKey;
import org.apache.kafka.coordinator.globalsequence.generated.IndexerFenceValue;
import org.apache.kafka.coordinator.globalsequence.generated.TopicMetadataKey;
import org.apache.kafka.coordinator.globalsequence.generated.TopicMetadataValue;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineLong;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newBatchIndexRecord;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newIndexerFenceRecord;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newTopicMetadataRecord;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.requireNonNegative;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.requireNonZeroId;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.validatePhysicalBatch;

/**
 * Ordered allocator for the topics assigned to one index partition. All access is serialized
 * by CoordinatorRuntime. Only replay changes speculative state; only index HW advancement
 * changes committed state. Historical batch mappings are deliberately not retained here.
 *
 * The caller must validate the source leader against cluster metadata and submit batches read
 * without omissions from its committed data log. Predecessors validate submission order, not
 * the contents of physical gaps (which may contain control batches).
 */
public class GlobalSequenceCoordinatorShard implements CoordinatorShard<CoordinatorRecord> {
    public record PartitionKey(Uuid topicId, int partition) {
        public PartitionKey {
            requireNonZeroId(topicId, "topicId");
            requireNonNegative(partition, "physicalPartition");
        }
    }

    public record PhysicalBatch(PartitionKey partition, long baseOffset, long lastOffset, int recordCount) {
        public PhysicalBatch {
            Objects.requireNonNull(partition, "partition");
            validatePhysicalBatch(partition.topicId(), partition.partition(), baseOffset, lastOffset, recordCount);
        }

        public long resumeOffset() {
            return lastOffset + 1;
        }

        private BatchId id() {
            return new BatchId(partition, baseOffset);
        }
    }

    /** Source ownership is separate from both physical identity and the coordinator epoch. */
    public record IndexerIdentity(int brokerId, int leaderEpoch, long generation, Uuid registrationId) {
        public IndexerIdentity {
            requireNonNegative(brokerId, "sourceBrokerId");
            requireNonNegative(leaderEpoch, "sourceLeaderEpoch");
            requireNonNegative(generation, "indexerGeneration");
            requireNonZeroId(registrationId, "registrationId");
        }
    }

    /** predecessorBaseOffset is -1 for the first non-control batch, even after control-only data. */
    public record AppendRequest(
        PhysicalBatch batch,
        long predecessorBaseOffset,
        long dataHighWatermark,
        IndexerIdentity indexer
    ) {
        public AppendRequest {
            Objects.requireNonNull(batch, "batch");
            Objects.requireNonNull(indexer, "indexer");
            if (predecessorBaseOffset < -1 || predecessorBaseOffset >= batch.baseOffset()) {
                throw new IllegalArgumentException("The predecessor must be -1 or an earlier physical batch base offset");
            }
            if (dataHighWatermark <= batch.lastOffset()) {
                throw new IllegalArgumentException("The entire physical batch must be below the data high watermark");
            }
        }
    }

    public enum AppendStatus {
        INDEXED((byte) 0),
        ALREADY_INDEXED((byte) 1),
        FENCED((byte) 2),
        OWNER_NOT_COMMITTED((byte) 3),
        OUT_OF_ORDER((byte) 4);

        private final byte code;

        AppendStatus(byte code) {
            this.code = code;
        }

        public byte code() {
            return code;
        }

        public static AppendStatus forCode(byte code) {
            for (AppendStatus status : values()) {
                if (status.code == code) return status;
            }
            throw new IllegalArgumentException("Unknown global sequence append status " + code);
        }
    }

    /** expectedGeneration is -1 if no ownership has been recorded. Preserve this request on retry. */
    public record RegistrationRequest(
        PartitionKey partition,
        int sourceBrokerId,
        int sourceLeaderEpoch,
        long expectedGeneration,
        Uuid registrationId
    ) {
        public RegistrationRequest {
            Objects.requireNonNull(partition, "partition");
            requireNonNegative(sourceBrokerId, "sourceBrokerId");
            requireNonNegative(sourceLeaderEpoch, "sourceLeaderEpoch");
            requireNonZeroId(registrationId, "registrationId");
            if (expectedGeneration < -1 || expectedGeneration == Long.MAX_VALUE) {
                throw new IllegalArgumentException("expectedGeneration must allow a non-negative successor without overflow");
            }
        }
    }

    /** A registered identity is usable only after the runtime write future completes. */
    public record RegistrationResponse(boolean registered, Optional<IndexerIdentity> indexer, int coordinatorLeaderEpoch) { }

    /** Ownership includes the speculative fence for CAS; progress includes only committed batches. */
    public record PartitionDescription(Optional<PhysicalBatch> committedProgress, Optional<IndexerIdentity> currentIndexer) { }

    /**
     * INDEXED is successful only after the runtime's write future completes. indexedThrough is
     * the physical prefix guaranteed at that time; it need not be the latest prefix by then.
     * ALREADY_INDEXED does not promise the historical global mapping.
     */
    public record AppendResponse(
        AppendStatus status,
        PhysicalBatch batch,
        OptionalLong globalBaseOffset,
        Optional<PhysicalBatch> indexedThrough,
        int coordinatorLeaderEpoch
    ) { }

    private record BatchId(PartitionKey partition, long baseOffset) { }

    private sealed interface PendingChange permits Allocation, Fence { }

    private record Allocation(PhysicalBatch batch, long globalBaseOffset, long endOffset) implements PendingChange {
        long globalEndOffset() {
            return globalBaseOffset + batch.recordCount();
        }
    }

    private record Fence(PartitionKey partition, IndexerIdentity identity, long endOffset) implements PendingChange { }

    // These immutable map values describe the entire replayed prefix, including the local tail.
    private final TimelineHashMap<Uuid, Long> nextGlobalOffsets;
    private final TimelineHashMap<PartitionKey, Allocation> latestAllocations;
    private final TimelineHashMap<PartitionKey, Fence> latestFences;
    private final TimelineLong nextReplayOffset;

    // HW never rolls back. These maps contain only the latest committed state per topic/partition.
    private final Map<Uuid, Long> committedGlobalOffsets = new HashMap<>();
    private final Map<PartitionKey, Allocation> committedAllocations = new HashMap<>();
    private final Map<PartitionKey, Fence> committedFences = new HashMap<>();

    // Retained only until commit or actual rollback, never removed by a request timeout.
    private final TreeMap<Long, PendingChange> pendingChanges = new TreeMap<>();
    private final Map<BatchId, Allocation> pendingAllocations = new HashMap<>();
    private long highWatermark = 0;

    // A BatchIndex must be immediately followed by its TopicMetadata in the same atomic write.
    // No runtime snapshot or successful load may end between these two records.
    private Allocation stagedAllocation;

    public GlobalSequenceCoordinatorShard(SnapshotRegistry snapshotRegistry) {
        Objects.requireNonNull(snapshotRegistry, "snapshotRegistry");
        nextGlobalOffsets = new TimelineHashMap<>(snapshotRegistry, 0);
        latestAllocations = new TimelineHashMap<>(snapshotRegistry, 0);
        latestFences = new TimelineHashMap<>(snapshotRegistry, 0);
        nextReplayOffset = new TimelineLong(snapshotRegistry);
    }

    public CoordinatorResult<RegistrationResponse, CoordinatorRecord> prepareRegistration(
        RegistrationRequest request,
        CoordinatorWriteContext context
    ) {
        validateWriteContext(context);
        Fence current = latestFences.get(request.partition());
        IndexerIdentity candidate = new IndexerIdentity(request.sourceBrokerId(), request.sourceLeaderEpoch(),
            request.expectedGeneration() + 1, request.registrationId());
        if (current != null && current.identity().equals(candidate)) {
            // The identical registration may still be pending. Reattach a runtime wait to its fence.
            return new CoordinatorResult<>(List.of(), new RegistrationResponse(true, Optional.of(candidate), context.leaderEpoch()));
        }
        if (!canRegister(request, current)) {
            return new CoordinatorResult<>(List.of(), new RegistrationResponse(false,
                Optional.ofNullable(current).map(Fence::identity), context.leaderEpoch()));
        }
        CoordinatorRecord record = newIndexerFenceRecord(request.partition().topicId(), request.partition().partition(),
            candidate.brokerId(), candidate.leaderEpoch(), candidate.generation(), candidate.registrationId());
        // This fence follows all previously accepted writes. Its commit is the new owner's barrier.
        return new CoordinatorResult<>(List.of(record), new RegistrationResponse(true, Optional.of(candidate), context.leaderEpoch()));
    }

    private static boolean canRegister(RegistrationRequest request, Fence current) {
        if (current == null) return request.expectedGeneration() == -1;
        IndexerIdentity owner = current.identity();
        return request.expectedGeneration() == owner.generation() &&
            !request.registrationId().equals(owner.registrationId()) &&
            request.sourceLeaderEpoch() >= owner.leaderEpoch() &&
            (request.sourceLeaderEpoch() != owner.leaderEpoch() || request.sourceBrokerId() == owner.brokerId());
    }

    public PartitionDescription describePartition(PartitionKey partition) {
        return new PartitionDescription(committedProgress(partition),
            Optional.ofNullable(latestFences.get(partition)).map(Fence::identity));
    }

    private void validateWriteContext(CoordinatorWriteContext context) {
        Objects.requireNonNull(context, "context");
        if (context.highWatermark() != highWatermark) {
            throw new IllegalStateException("Write requires the runtime's current applied high watermark");
        }
        ensureCompleteAllocation();
    }

    /**
     * Invoke within scheduleWriteOperationWithContext. An empty result still needs to pass
     * through the runtime: pending retries must attach a new wait, even after an earlier wait
     * timed out. Returning this response directly would incorrectly acknowledge local state.
     */
    public CoordinatorResult<AppendResponse, CoordinatorRecord> prepareAppend(
        AppendRequest request,
        CoordinatorWriteContext context
    ) {
        Objects.requireNonNull(request, "request");
        validateWriteContext(context);
        PhysicalBatch batch = request.batch();
        Fence owner = latestFences.get(batch.partition());
        if (owner == null || !owner.identity().equals(request.indexer())) {
            return response(AppendStatus.FENCED, batch, context);
        }
        if (owner.endOffset() > highWatermark) {
            return response(AppendStatus.OWNER_NOT_COMMITTED, batch, context);
        }
        return prepareOwnedAppend(request, context);
    }

    private CoordinatorResult<AppendResponse, CoordinatorRecord> prepareOwnedAppend(
        AppendRequest request,
        CoordinatorWriteContext context
    ) {
        PhysicalBatch batch = request.batch();
        Allocation committed = committedAllocations.get(batch.partition());
        if (committed != null && batch.baseOffset() <= committed.batch().baseOffset()) {
            validateCommittedRetry(batch, committed.batch());
            return response(AppendStatus.ALREADY_INDEXED, batch, context);
        }
        Allocation pending = pendingAllocations.get(batch.id());
        if (pending != null) {
            requireSameBatch(batch, pending.batch());
            return indexedResponse(batch, pending.globalBaseOffset(), context, List.of());
        }
        if (committed != null && batch.baseOffset() <= committed.batch().lastOffset()) {
            throw new IllegalArgumentException("The physical batch overlaps the committed batch");
        }
        if (!Objects.equals(latestAllocations.get(batch.partition()), committed)) {
            return response(AppendStatus.OUT_OF_ORDER, batch, context);
        }
        long predecessor = committed == null ? -1 : committed.batch().baseOffset();
        if (request.predecessorBaseOffset() != predecessor) {
            return response(AppendStatus.OUT_OF_ORDER, batch, context);
        }

        long globalBaseOffset = nextGlobalOffset(batch.partition().topicId());
        CoordinatorRecord indexRecord = newBatchIndexRecord(batch.partition().topicId(), batch.partition().partition(),
            batch.baseOffset(), batch.lastOffset(), batch.recordCount(), globalBaseOffset);
        return indexedResponse(batch, globalBaseOffset, context, List.of(indexRecord,
            newTopicMetadataRecord(batch.partition().topicId(), globalBaseOffset + batch.recordCount())));
    }

    private CoordinatorResult<AppendResponse, CoordinatorRecord> response(
        AppendStatus status,
        PhysicalBatch batch,
        CoordinatorWriteContext context
    ) {
        return new CoordinatorResult<>(List.of(), new AppendResponse(status, batch, OptionalLong.empty(),
            committedProgress(batch.partition()), context.leaderEpoch()));
    }

    private CoordinatorResult<AppendResponse, CoordinatorRecord> indexedResponse(
        PhysicalBatch batch,
        long globalBaseOffset,
        CoordinatorWriteContext context,
        List<CoordinatorRecord> records
    ) {
        return new CoordinatorResult<>(records, new AppendResponse(AppendStatus.INDEXED, batch,
            OptionalLong.of(globalBaseOffset), Optional.of(batch), context.leaderEpoch()));
    }

    private static void validateCommittedRetry(PhysicalBatch batch, PhysicalBatch committed) {
        if (batch.baseOffset() == committed.baseOffset()) {
            requireSameBatch(batch, committed);
        } else if (batch.lastOffset() >= committed.baseOffset()) {
            throw new IllegalArgumentException("The old physical batch overlaps the latest committed batch");
        }
    }

    private static void requireSameBatch(PhysicalBatch batch, PhysicalBatch known) {
        if (!batch.equals(known)) {
            throw new IllegalArgumentException("The same physical batch identity has conflicting bounds or record count");
        }
    }

    public Optional<PhysicalBatch> committedProgress(PartitionKey partition) {
        return Optional.ofNullable(committedAllocations.get(partition)).map(Allocation::batch);
    }

    public Optional<IndexerIdentity> committedIndexer(PartitionKey partition) {
        return Optional.ofNullable(committedFences.get(partition)).map(Fence::identity);
    }

    public long committedNextGlobalOffset(Uuid topicId) {
        return committedGlobalOffsets.getOrDefault(topicId, 0L);
    }

    /** Includes allocations which have not yet committed. Not a public read boundary. */
    long nextGlobalOffset(Uuid topicId) {
        return nextGlobalOffsets.getOrDefault(topicId, 0L);
    }

    int pendingAllocationCount() {
        return pendingAllocations.size();
    }

    @Override
    public void replay(long offset, long producerId, short producerEpoch, CoordinatorRecord record) {
        Objects.requireNonNull(record, "record");
        if (offset != nextReplayOffset.get() || offset == Long.MAX_VALUE) {
            throw new IllegalStateException("Expected index record at " + nextReplayOffset.get() + ", received " + offset);
        }
        if (producerId != RecordBatch.NO_PRODUCER_ID || producerEpoch != RecordBatch.NO_PRODUCER_EPOCH) {
            throw new IllegalStateException("Index records must not be transactional");
        }
        if (record.value() == null) {
            throw new IllegalStateException("Index tombstones are not supported without a checkpoint/GC contract");
        }
        if (record.value().version() != 0) {
            throw new UnsupportedVersionException("Unsupported global sequence record version " + record.value().version());
        }
        CoordinatorRecordType type = CoordinatorRecordType.fromId(record.key().apiKey());
        if (stagedAllocation != null && type != CoordinatorRecordType.TOPIC_METADATA) {
            throw new IllegalStateException("BatchIndex must be immediately followed by TopicMetadata");
        }
        switch (type) {
            case BATCH_INDEX -> replayBatch(offset, (BatchIndexKey) record.key(), (BatchIndexValue) record.value().message());
            case TOPIC_METADATA -> replayMetadata(offset, (TopicMetadataKey) record.key(), (TopicMetadataValue) record.value().message());
            case INDEXER_FENCE -> replayFence(offset, (IndexerFenceKey) record.key(), (IndexerFenceValue) record.value().message());
        }
        nextReplayOffset.set(offset + 1);
    }

    private void replayBatch(long offset, BatchIndexKey key, BatchIndexValue value) {
        PartitionKey partition = new PartitionKey(key.topicId(), value.physicalPartition());
        PhysicalBatch batch = new PhysicalBatch(partition, value.physicalBaseOffset(), value.physicalLastOffset(), value.recordCount());
        if (!latestFences.containsKey(partition)) {
            throw new IllegalStateException("BatchIndex has no preceding indexer ownership record");
        }
        long expectedGlobalOffset = nextGlobalOffset(key.topicId());
        if (key.globalBaseOffset() != expectedGlobalOffset || expectedGlobalOffset > Long.MAX_VALUE - batch.recordCount()) {
            throw new IllegalStateException("BatchIndex must extend the topic's contiguous global sequence without overflow");
        }
        Allocation previous = latestAllocations.get(partition);
        if (previous != null && batch.baseOffset() <= previous.batch().lastOffset()) {
            throw new IllegalStateException("BatchIndex physical ranges must be strictly ordered and non-overlapping");
        }
        stagedAllocation = new Allocation(batch, key.globalBaseOffset(), Math.addExact(offset, 2));
    }

    private void replayMetadata(long offset, TopicMetadataKey key, TopicMetadataValue value) {
        Allocation allocation = stagedAllocation;
        if (allocation == null || !allocation.batch().partition().topicId().equals(key.topicId()) ||
            allocation.globalEndOffset() != value.nextGlobalOffset() || allocation.endOffset() != offset + 1) {
            throw new IllegalStateException("TopicMetadata must complete the immediately preceding BatchIndex allocation");
        }
        nextGlobalOffsets.put(key.topicId(), value.nextGlobalOffset());
        latestAllocations.put(allocation.batch().partition(), allocation);
        pendingAllocations.put(allocation.batch().id(), allocation);
        pendingChanges.put(allocation.endOffset(), allocation);
        stagedAllocation = null;
    }

    private void replayFence(long offset, IndexerFenceKey key, IndexerFenceValue value) {
        PartitionKey partition = new PartitionKey(key.topicId(), key.physicalPartition());
        IndexerIdentity identity = new IndexerIdentity(value.sourceBrokerId(), value.sourceLeaderEpoch(),
            value.indexerGeneration(), value.registrationId());
        Fence previous = latestFences.get(partition);
        if (previous != null && !previous.identity().equals(identity)) {
            IndexerIdentity old = previous.identity();
            if (identity.generation() <= old.generation() || identity.leaderEpoch() < old.leaderEpoch() ||
                (identity.leaderEpoch() == old.leaderEpoch() && identity.brokerId() != old.brokerId())) {
                throw new IllegalStateException("IndexerFence must advance generation without regressing source leadership");
            }
        }
        Fence fence = new Fence(partition, identity, offset + 1);
        latestFences.put(partition, fence);
        pendingChanges.put(fence.endOffset(), fence);
    }

    @Override
    public void onHighWatermarkUpdated(long newHighWatermark) {
        if (newHighWatermark < highWatermark || newHighWatermark > nextReplayOffset.get()) {
            throw new IllegalStateException("Index HW must advance within the replayed log");
        }
        while (!pendingChanges.isEmpty() && pendingChanges.firstKey() <= newHighWatermark) {
            PendingChange change = pendingChanges.pollFirstEntry().getValue();
            if (change instanceof Allocation allocation) {
                committedGlobalOffsets.put(allocation.batch().partition().topicId(), allocation.globalEndOffset());
                committedAllocations.put(allocation.batch().partition(), allocation);
                pendingAllocations.remove(allocation.batch().id());
            } else if (change instanceof Fence fence) {
                committedFences.put(fence.partition(), fence);
            }
        }
        highWatermark = newHighWatermark;
    }

    @Override
    public void onRollback(long offset) {
        if (offset < highWatermark || nextReplayOffset.get() != offset) {
            throw new IllegalStateException("Rollback requires a restored snapshot at or above the index HW");
        }
        // A cut between BatchIndex and TopicMetadata cannot be a valid atomic runtime snapshot.
        if (pendingChanges.get(offset + 1) instanceof Allocation ||
            (stagedAllocation != null && stagedAllocation.endOffset() == offset + 1)) {
            throw new IllegalStateException("Rollback cannot split an atomic allocation");
        }
        stagedAllocation = null;
        var iterator = pendingChanges.tailMap(offset, false).values().iterator();
        while (iterator.hasNext()) {
            if (iterator.next() instanceof Allocation allocation) {
                pendingAllocations.remove(allocation.batch().id());
            }
            iterator.remove();
        }
    }

    @Override
    public void onLoaded(MetadataImage newImage) {
        ensureCompleteAllocation();
    }

    private void ensureCompleteAllocation() {
        if (stagedAllocation != null) {
            throw new IllegalStateException("The index log ends with an incomplete allocation");
        }
    }

    @Override
    public void replayEndTransactionMarker(long producerId, short producerEpoch, TransactionResult result) {
        throw new IllegalStateException("Transactions are not supported in the index log");
    }
}
