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

import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookup.Position;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Bounded positions for one topic, accessed only on its coordinator event thread.
 * Level zero samples every N committed source batches, starting with the first batch.
 * When a level overflows, its oldest pair is replaced by its first position in the
 * next level. Each older level therefore doubles the spacing. The final level drops
 * its oldest position on overflow. Replaying and committing the log rebuilds this index.
 */
final class GlobalSequenceSparseIndex {
    private final int baseIntervalBatches;
    private final int entriesPerLevel;
    private final int maxEntries;
    private final List<Deque<Position>> levels = new ArrayList<>();
    private final TreeMap<Long, Position> positions = new TreeMap<>();
    private int batchesUntilSample;

    GlobalSequenceSparseIndex(int baseIntervalBatches, int entriesPerLevel, int maxEntries) {
        if (baseIntervalBatches < 1 || entriesPerLevel < 1 || maxEntries < 1) {
            throw new IllegalArgumentException("Sparse index limits must be positive");
        }
        this.baseIntervalBatches = baseIntervalBatches;
        this.entriesPerLevel = entriesPerLevel;
        this.maxEntries = maxEntries;
    }

    void add(Position position) {
        if (batchesUntilSample > 0) {
            batchesUntilSample--;
            return;
        }
        batchesUntilSample = baseIntervalBatches - 1;
        positions.put(position.globalBaseOffset(), position);
        for (int level = 0; ; level++) {
            if (level == levels.size()) levels.add(new ArrayDeque<>());
            Deque<Position> entries = levels.get(level);
            entries.addLast(position);
            long remaining = maxEntries - (long) level * entriesPerLevel;
            int capacity = (int) Math.min(entriesPerLevel, remaining);
            if (entries.size() <= capacity) return;
            position = entries.removeFirst();
            if (remaining <= entriesPerLevel) {
                positions.remove(position.globalBaseOffset());
                return;
            }
            // Keep the earlier position so floor lookups also cover the discarded interval.
            positions.remove(entries.removeFirst().globalBaseOffset());
        }
    }

    Optional<Position> floor(long globalOffset) {
        var entry = positions.floorEntry(globalOffset);
        return entry == null ? Optional.empty() : Optional.of(entry.getValue());
    }

    int size() {
        return positions.size();
    }
}
