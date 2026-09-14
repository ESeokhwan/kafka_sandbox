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
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PhysicalBatch;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.requireNonZeroId;

/** Immutable, bounded lookup contract. All end offsets are exclusive. */
public final class GlobalSequenceLookup {
    public static final int MAX_BATCHES = 1000;
    public static final int MAX_TIMEOUT_MS = 30000;

    private GlobalSequenceLookup() { }

    public record Request(Uuid topicId, long startOffset, long endOffset, int maxBatches, int timeoutMs) {
        public Request {
            requireNonZeroId(topicId, "topicId");
            if (startOffset < 0 || endOffset < startOffset)
                throw new IllegalArgumentException("Invalid global lookup range");
            if (maxBatches < 1 || maxBatches > MAX_BATCHES)
                throw new IllegalArgumentException("maxBatches must be between 1 and " + MAX_BATCHES);
            if (timeoutMs < 1 || timeoutMs > MAX_TIMEOUT_MS)
                throw new IllegalArgumentException("timeoutMs must be between 1 and " + MAX_TIMEOUT_MS);
        }
    }

    public record Snapshot(Uuid indexTopicId, int indexPartition, int leaderEpoch,
                           long indexHighWatermark, long committedGlobalEnd) {
        public Snapshot {
            requireNonZeroId(indexTopicId, "indexTopicId");
            if (indexPartition < 0 || leaderEpoch < 0 || indexHighWatermark < 0 || committedGlobalEnd < 0)
                throw new IllegalArgumentException("Invalid global lookup snapshot");
        }
    }

    public record Mapping(long globalBaseOffset, PhysicalBatch batch) {
        public Mapping {
            Objects.requireNonNull(batch, "batch");
            if (globalBaseOffset < 0 || globalBaseOffset > Long.MAX_VALUE - batch.recordCount())
                throw new IllegalArgumentException("Invalid global mapping range");
        }

        public long globalEndOffset() {
            return globalBaseOffset + batch.recordCount();
        }
    }

    public record Result(Snapshot snapshot, List<Mapping> mappings, long nextGlobalOffset) {
        public Result {
            Objects.requireNonNull(snapshot, "snapshot");
            mappings = List.copyOf(mappings);
            if (mappings.size() > MAX_BATCHES || nextGlobalOffset < 0 || nextGlobalOffset > snapshot.committedGlobalEnd())
                throw new IllegalArgumentException("Invalid global lookup result");
        }
    }

    /** Broker-owned log I/O, performed outside coordinator event threads and locks. */
    public interface Reader extends AutoCloseable {
        CompletableFuture<Result> read(Request request, Snapshot snapshot, long deadlineNs);

        @Override
        default void close() { }
    }
}
