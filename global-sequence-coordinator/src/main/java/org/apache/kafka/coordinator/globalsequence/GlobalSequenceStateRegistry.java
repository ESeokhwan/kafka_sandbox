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
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineLong;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The replay-driven state owned by one global sequence coordinator shard.
 */
public class GlobalSequenceStateRegistry {
    private final SnapshotRegistry snapshotRegistry;
    private final TimelineHashMap<Uuid, GlobalSequenceState> stateMap;

    public GlobalSequenceStateRegistry(SnapshotRegistry snapshotRegistry) {
        this.snapshotRegistry = Objects.requireNonNull(snapshotRegistry, "snapshotRegistry");
        this.stateMap = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    public GlobalSequenceState getState(Uuid topicId) {
        return stateMap.get(topicId);
    }

    public boolean contains(Uuid topicId) {
        return stateMap.containsKey(topicId);
    }

    private void createNewTopicState(Uuid topicId) {
        validateTopicId(topicId);
        if (!stateMap.containsKey(topicId)) {
            stateMap.put(topicId, new GlobalSequenceState(snapshotRegistry));
        }
    }

    PreparedAppend prepareAppend(GlobalSequenceAppendRequest request) {
        Objects.requireNonNull(request, "request");
        GlobalSequenceState state = stateMap.get(request.topicId());
        if (state == null) {
            return new PreparedAppend(new GlobalSequenceIndexRecord(
                request.topicId(),
                0L,
                request.recordCount(),
                request.partitionIndex(),
                request.partitionBaseOffset()
            ), false);
        }
        return state.prepareAppend(request);
    }

    void replay(GlobalSequenceIndexRecord indexRecord) {
        Objects.requireNonNull(indexRecord, "indexRecord");
        createNewTopicState(indexRecord.topicId());
        stateMap.get(indexRecord.topicId()).replay(indexRecord);
    }

    void replayTombstone(Uuid topicId, long globalBaseOffset) {
        validateTopicId(topicId);
        if (globalBaseOffset < 0) {
            throw new IllegalArgumentException("globalBaseOffset must not be negative");
        }

        GlobalSequenceState state = stateMap.get(topicId);
        if (state != null) {
            state.replayTombstone(globalBaseOffset);
        }
    }

    GlobalSequenceLookupResult lookup(GlobalSequenceLookupRequest request, long indexLogHighWatermark) {
        Objects.requireNonNull(request, "request");
        GlobalSequenceState state = stateMap.get(request.topicId(), indexLogHighWatermark);
        if (state == null) {
            throw outOfRange(request, request.globalStartOffset());
        }
        return state.lookup(request, indexLogHighWatermark);
    }

    private static OffsetOutOfRangeException outOfRange(
        GlobalSequenceLookupRequest request,
        long missingOffset
    ) {
        return new OffsetOutOfRangeException(
            "Global offset " + missingOffset + " is not indexed for topic ID " + request.topicId() +
                " in requested range [" + request.globalStartOffset() + ", " +
                request.globalEndOffsetExclusive() + ")"
        );
    }

    private static void validateTopicId(Uuid topicId) {
        Objects.requireNonNull(topicId, "topicId");
        if (Uuid.ZERO_UUID.equals(topicId)) {
            throw new IllegalArgumentException("topicId must not be ZERO_UUID");
        }
    }

    record PreparedAppend(GlobalSequenceIndexRecord indexRecord, boolean duplicate) { }

    public static class GlobalSequenceState {
        private final TimelineHashMap<Long, GlobalSequenceIndexRecord> sequenceByGlobalBaseOffset;
        // Allocations are replayed in increasing global-offset order, so their dense ordinals provide
        // a snapshot-aware binary-search index without rebuilding and sorting every lookup.
        private final TimelineHashMap<Long, GlobalSequenceIndexRecord> sequenceByAllocationOrdinal;
        private final TimelineHashMap<PhysicalBatchId, GlobalSequenceIndexRecord> sequenceByPhysicalBatch;
        private final GlobalOffsetSequencer offsetSequencer;
        private final TimelineLong allocationCount;

        GlobalSequenceState(SnapshotRegistry snapshotRegistry) {
            this.sequenceByGlobalBaseOffset = new TimelineHashMap<>(snapshotRegistry, 0);
            this.sequenceByAllocationOrdinal = new TimelineHashMap<>(snapshotRegistry, 0);
            this.sequenceByPhysicalBatch = new TimelineHashMap<>(snapshotRegistry, 0);
            this.offsetSequencer = new BasicGlobalOffsetSequencer(snapshotRegistry);
            this.allocationCount = new TimelineLong(snapshotRegistry);
        }

        long nextGlobalOffset() {
            return offsetSequencer.nextOffset();
        }

        PreparedAppend prepareAppend(GlobalSequenceAppendRequest request) {
            GlobalSequenceIndexRecord existing = sequenceByPhysicalBatch.get(request.physicalBatchId());
            if (existing != null) {
                if (existing.recordCount() != request.recordCount()) {
                    throw new IllegalArgumentException(
                        "Physical batch " + request.physicalBatchId() + " is already allocated with recordCount=" +
                            existing.recordCount() + ", but the retry has recordCount=" + request.recordCount()
                    );
                }
                return new PreparedAppend(existing, true);
            }

            return new PreparedAppend(new GlobalSequenceIndexRecord(
                request.topicId(),
                offsetSequencer.nextOffset(),
                request.recordCount(),
                request.partitionIndex(),
                request.partitionBaseOffset()
            ), false);
        }

        void replay(GlobalSequenceIndexRecord indexRecord) {
            PhysicalBatchId physicalBatchId = new PhysicalBatchId(
                indexRecord.topicId(),
                indexRecord.partitionIndex(),
                indexRecord.partitionBaseOffset()
            );
            GlobalSequenceIndexRecord byPhysicalBatch = sequenceByPhysicalBatch.get(physicalBatchId);
            GlobalSequenceIndexRecord byGlobalBaseOffset = sequenceByGlobalBaseOffset.get(
                indexRecord.globalBaseOffset()
            );

            if (byPhysicalBatch != null && !byPhysicalBatch.equals(indexRecord)) {
                throw new IllegalStateException(
                    "Conflicting allocation for physical batch " + physicalBatchId + ": existing=" +
                        byPhysicalBatch + ", replayed=" + indexRecord
                );
            }
            if (byGlobalBaseOffset != null && !byGlobalBaseOffset.equals(indexRecord)) {
                throw new IllegalStateException(
                    "Conflicting allocation at global base offset " + indexRecord.globalBaseOffset() +
                        ": existing=" + byGlobalBaseOffset + ", replayed=" + indexRecord
                );
            }
            if (byPhysicalBatch != null || byGlobalBaseOffset != null) {
                if (byPhysicalBatch == null || byGlobalBaseOffset == null) {
                    throw new IllegalStateException("Global sequence indexes are inconsistent for " + indexRecord);
                }
                return;
            }

            validateReplayOrder(indexRecord);
            long allocationOrdinal = allocationCount.get();
            long nextAllocationCount = Math.addExact(allocationOrdinal, 1L);
            GlobalSequenceIndexRecord byAllocationOrdinal = sequenceByAllocationOrdinal.get(allocationOrdinal);
            if (byAllocationOrdinal != null) {
                throw new IllegalStateException(
                    "Conflicting allocation at ordinal " + allocationOrdinal + ": existing=" +
                        byAllocationOrdinal + ", replayed=" + indexRecord
                );
            }

            sequenceByGlobalBaseOffset.put(indexRecord.globalBaseOffset(), indexRecord);
            sequenceByPhysicalBatch.put(physicalBatchId, indexRecord);
            sequenceByAllocationOrdinal.put(allocationOrdinal, indexRecord);
            offsetSequencer.replayAllocation(indexRecord.globalBaseOffset(), indexRecord.recordCount());
            allocationCount.set(nextAllocationCount);
        }

        void replayTombstone(long globalBaseOffset) {
            GlobalSequenceIndexRecord existing = sequenceByGlobalBaseOffset.get(globalBaseOffset);
            if (existing == null) {
                return;
            }

            PhysicalBatchId physicalBatchId = new PhysicalBatchId(
                existing.topicId(),
                existing.partitionIndex(),
                existing.partitionBaseOffset()
            );
            if (!existing.equals(sequenceByPhysicalBatch.get(physicalBatchId))) {
                throw new IllegalStateException("Global sequence indexes are inconsistent for " + existing);
            }

            // Tombstones define the end of the retry-safety window for this physical batch. They
            // must not be emitted until a durable per-topic watermark is added to the log schema;
            // otherwise compaction could make the in-memory next offset unrecoverable on restart.
            // The ordinal entry remains so lookups can locate and reject the deleted global range.
            sequenceByGlobalBaseOffset.remove(globalBaseOffset);
            sequenceByPhysicalBatch.remove(physicalBatchId);
        }

        GlobalSequenceLookupResult lookup(GlobalSequenceLookupRequest request, long indexLogHighWatermark) {
            List<GlobalSequenceIndexRecord> matches = new ArrayList<>();
            long nextOffsetToCover = request.globalStartOffset();
            long count = allocationCount.get(indexLogHighWatermark);
            long firstOrdinal = findFirstCandidateOrdinal(
                request.globalStartOffset(),
                count,
                indexLogHighWatermark
            );

            for (long ordinal = firstOrdinal; ordinal < count; ordinal++) {
                GlobalSequenceIndexRecord indexRecord = indexRecordAt(ordinal, indexLogHighWatermark);
                if (indexRecord.globalEndOffsetExclusive() <= nextOffsetToCover) {
                    continue;
                }
                if (indexRecord.globalBaseOffset() > nextOffsetToCover) {
                    throw outOfRange(request, nextOffsetToCover);
                }
                if (!isActive(indexRecord, indexLogHighWatermark)) {
                    throw outOfRange(request, nextOffsetToCover);
                }

                matches.add(indexRecord);
                nextOffsetToCover = Math.min(
                    indexRecord.globalEndOffsetExclusive(),
                    request.globalEndOffsetExclusive()
                );
                if (nextOffsetToCover == request.globalEndOffsetExclusive()) {
                    return new GlobalSequenceLookupResult(matches);
                }
            }

            throw outOfRange(request, nextOffsetToCover);
        }

        private long findFirstCandidateOrdinal(long globalStartOffset, long count, long indexLogHighWatermark) {
            long low = 0L;
            long high = count;
            while (low < high) {
                long middle = low + ((high - low) >>> 1);
                GlobalSequenceIndexRecord indexRecord = indexRecordAt(middle, indexLogHighWatermark);
                if (indexRecord.globalBaseOffset() <= globalStartOffset) {
                    low = middle + 1L;
                } else {
                    high = middle;
                }
            }
            return low == 0L ? 0L : low - 1L;
        }

        private GlobalSequenceIndexRecord indexRecordAt(long ordinal, long indexLogHighWatermark) {
            GlobalSequenceIndexRecord indexRecord = sequenceByAllocationOrdinal.get(
                ordinal,
                indexLogHighWatermark
            );
            if (indexRecord == null) {
                throw new IllegalStateException("Missing global sequence allocation at ordinal " + ordinal);
            }
            return indexRecord;
        }

        private boolean isActive(GlobalSequenceIndexRecord indexRecord, long indexLogHighWatermark) {
            GlobalSequenceIndexRecord active = sequenceByGlobalBaseOffset.get(
                indexRecord.globalBaseOffset(),
                indexLogHighWatermark
            );
            if (active == null) {
                return false;
            }
            if (!active.equals(indexRecord)) {
                throw new IllegalStateException(
                    "Global sequence indexes are inconsistent at global base offset " +
                        indexRecord.globalBaseOffset() + ": ordered=" + indexRecord + ", active=" + active
                );
            }
            return true;
        }

        private void validateReplayOrder(GlobalSequenceIndexRecord candidate) {
            if (candidate.globalBaseOffset() < offsetSequencer.nextOffset()) {
                throw new IllegalStateException(
                    "Overlapping or out-of-order global sequence allocation: nextGlobalOffset=" +
                        offsetSequencer.nextOffset() + ", replayed=" + candidate
                );
            }
        }
    }
}
