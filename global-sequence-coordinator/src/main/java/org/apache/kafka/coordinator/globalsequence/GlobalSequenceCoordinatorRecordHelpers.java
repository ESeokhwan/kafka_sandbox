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
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.globalsequence.generated.BatchIndexKey;
import org.apache.kafka.coordinator.globalsequence.generated.BatchIndexValue;
import org.apache.kafka.coordinator.globalsequence.generated.IndexerFenceKey;
import org.apache.kafka.coordinator.globalsequence.generated.IndexerFenceValue;
import org.apache.kafka.coordinator.globalsequence.generated.TopicMetadataKey;
import org.apache.kafka.coordinator.globalsequence.generated.TopicMetadataValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.Objects;

/**
 * Creates version 0 index records. Ownership, ordering and deduplication are checked by the shard.
 */
public final class GlobalSequenceCoordinatorRecordHelpers {
    private static final short VALUE_VERSION = 0;

    private GlobalSequenceCoordinatorRecordHelpers() {
    }

    public static CoordinatorRecord newBatchIndexRecord(
        Uuid topicId,
        int physicalPartition,
        long physicalBaseOffset,
        long physicalLastOffset,
        int recordCount,
        long globalBaseOffset
    ) {
        validatePhysicalBatch(topicId, physicalPartition, physicalBaseOffset, physicalLastOffset, recordCount);
        requireNonNegative(globalBaseOffset, "globalBaseOffset");
        if (globalBaseOffset > Long.MAX_VALUE - recordCount) {
            throw new IllegalArgumentException("The global end offset must not overflow");
        }
        return CoordinatorRecord.record(
            new BatchIndexKey().setTopicId(topicId).setGlobalBaseOffset(globalBaseOffset),
            new ApiMessageAndVersion(new BatchIndexValue()
                .setPhysicalPartition(physicalPartition)
                .setPhysicalBaseOffset(physicalBaseOffset)
                .setPhysicalLastOffset(physicalLastOffset)
                .setRecordCount(recordCount), VALUE_VERSION));
    }

    public static CoordinatorRecord newTopicMetadataRecord(Uuid topicId, long nextGlobalOffset) {
        requireNonZeroId(topicId, "topicId");
        requireNonNegative(nextGlobalOffset, "nextGlobalOffset");
        return CoordinatorRecord.record(
            new TopicMetadataKey().setTopicId(topicId),
            new ApiMessageAndVersion(new TopicMetadataValue().setNextGlobalOffset(nextGlobalOffset), VALUE_VERSION));
    }

    public static CoordinatorRecord newIndexerFenceRecord(
        Uuid topicId,
        int physicalPartition,
        int sourceBrokerId,
        int sourceLeaderEpoch,
        long indexerGeneration,
        Uuid registrationId
    ) {
        requireNonZeroId(topicId, "topicId");
        requireNonZeroId(registrationId, "registrationId");
        requireNonNegative(physicalPartition, "physicalPartition");
        requireNonNegative(sourceBrokerId, "sourceBrokerId");
        requireNonNegative(sourceLeaderEpoch, "sourceLeaderEpoch");
        requireNonNegative(indexerGeneration, "indexerGeneration");
        return CoordinatorRecord.record(
            new IndexerFenceKey().setTopicId(topicId).setPhysicalPartition(physicalPartition),
            new ApiMessageAndVersion(new IndexerFenceValue()
                .setSourceBrokerId(sourceBrokerId)
                .setSourceLeaderEpoch(sourceLeaderEpoch)
                .setIndexerGeneration(indexerGeneration)
                .setRegistrationId(registrationId), VALUE_VERSION));
    }

    static void validatePhysicalBatch(Uuid topicId, int partition, long baseOffset, long lastOffset, int recordCount) {
        requireNonZeroId(topicId, "topicId");
        requireNonNegative(partition, "physicalPartition");
        requireNonNegative(baseOffset, "physicalBaseOffset");
        if (recordCount <= 0 || lastOffset < baseOffset || lastOffset - baseOffset != (long) recordCount - 1) {
            throw new IllegalArgumentException("The physical range must contain exactly recordCount contiguous records");
        }
        if (lastOffset == Long.MAX_VALUE) {
            throw new IllegalArgumentException("The physical resume offset must not overflow");
        }
    }

    static void requireNonZeroId(Uuid id, String name) {
        Objects.requireNonNull(id, name);
        if (Uuid.ZERO_UUID.equals(id)) {
            throw new IllegalArgumentException(name + " must not be the zero UUID");
        }
    }

    static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }
}
