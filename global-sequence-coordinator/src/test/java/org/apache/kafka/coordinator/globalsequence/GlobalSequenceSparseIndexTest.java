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

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalSequenceSparseIndexTest {
    @Test
    void testBaseIntervalCountsBatchesAndFloorIncludesBatchInterior() {
        GlobalSequenceSparseIndex index = new GlobalSequenceSparseIndex(3, 100, 100);
        assertTrue(index.floor(0).isEmpty());
        for (int i = 0; i < 15; i++) index.add(new Position(i * 10L, i * 2L + 1));
        assertEquals(5, index.size());
        assertEquals(Optional.of(new Position(0, 1)), index.floor(29));
        assertEquals(Optional.of(new Position(30, 7)), index.floor(30));
        assertEquals(Optional.of(new Position(120, 25)), index.floor(149));
    }

    @Test
    void testOlderLevelsDoubleSpacing() {
        GlobalSequenceSparseIndex index = new GlobalSequenceSparseIndex(1, 2, 6);
        for (int i = 0; i < 10; i++) index.add(new Position(i, i * 2L));
        assertEquals(5, index.size());
        for (long offset : new long[] {0, 4, 6, 8, 9}) {
            assertEquals(offset, index.floor(offset).orElseThrow().globalBaseOffset());
        }
        assertEquals(0, index.floor(3).orElseThrow().globalBaseOffset());
        assertEquals(4, index.floor(5).orElseThrow().globalBaseOffset());
        assertEquals(6, index.floor(7).orElseThrow().globalBaseOffset());
    }

    @Test
    void testHardBoundWithPartialLevelsAndOldHistoryFallsBack() {
        for (int capacity : new int[] {1, 2, 3, 5, 9}) {
            for (int perLevel : new int[] {1, 2, 4, 16}) {
                GlobalSequenceSparseIndex index = new GlobalSequenceSparseIndex(1, perLevel, capacity);
                for (int i = 0; i < 10000; i++) {
                    index.add(new Position(i, i * 2L));
                    assertTrue(index.size() <= capacity);
                    assertTrue(index.floor(i).orElseThrow().globalBaseOffset() <= i);
                }
                assertTrue(index.floor(0).isEmpty());
            }
        }
    }
}
