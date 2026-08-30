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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BasicGlobalOffsetSequencerTest {
    @Test
    void testReplayAllocationAdvancesToEndOfRange() {
        BasicGlobalOffsetSequencer sequencer = newSequencer();

        assertEquals(0L, sequencer.nextOffset());

        sequencer.replayAllocation(4L, 3);
        assertEquals(7L, sequencer.nextOffset());

        sequencer.replayAllocation(7L, 2);
        assertEquals(9L, sequencer.nextOffset());
    }

    @Test
    void testReplayAllocationDoesNotRewind() {
        BasicGlobalOffsetSequencer sequencer = newSequencer();

        sequencer.replayAllocation(10L, 3);
        sequencer.replayAllocation(2L, 4);

        assertEquals(13L, sequencer.nextOffset());
    }

    @Test
    void testReplayAllocationRejectsOverflowWithoutAdvancing() {
        BasicGlobalOffsetSequencer sequencer = newSequencer();
        sequencer.replayAllocation(0L, 2);

        assertThrows(
            ArithmeticException.class,
            () -> sequencer.replayAllocation(Long.MAX_VALUE, 1)
        );
        assertEquals(2L, sequencer.nextOffset());
    }

    @Test
    void testSnapshotRollbackRestoresNextOffset() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        BasicGlobalOffsetSequencer sequencer = new BasicGlobalOffsetSequencer(snapshotRegistry);
        snapshotRegistry.idempotentCreateSnapshot(0L);

        sequencer.replayAllocation(0L, 3);
        assertEquals(3L, sequencer.nextOffset());

        snapshotRegistry.revertToSnapshot(0L);
        assertEquals(0L, sequencer.nextOffset());
    }

    private static BasicGlobalOffsetSequencer newSequencer() {
        return new BasicGlobalOffsetSequencer(new SnapshotRegistry(new LogContext()));
    }
}
