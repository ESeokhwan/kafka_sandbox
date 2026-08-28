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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalSequenceAppendRequestTest {
    private static final Uuid TOPIC_ID = Uuid.randomUuid();

    @Test
    void testValidRequest() {
        GlobalSequenceAppendRequest request = request(3, 10L);
        GlobalSequenceAppendRequest sameRequest = request(3, 10L);

        assertEquals(TOPIC_ID, request.topicId());
        assertEquals(1, request.partitionIndex());
        assertEquals(10L, request.partitionBaseOffset());
        assertEquals(3, request.recordCount());
        assertEquals(request, sameRequest);
        assertEquals(request.hashCode(), sameRequest.hashCode());
        assertTrue(request.toString().contains("partitionBaseOffset=10"));
    }

    @Test
    void testPhysicalBatchIdentityOnlyUsesFinalLocation() {
        GlobalSequenceAppendRequest first = request(3, 10L);
        GlobalSequenceAppendRequest conflicting = request(5, 10L);
        GlobalSequenceAppendRequest otherLocation = request(3, 11L);

        assertEquals(first.physicalBatchId(), conflicting.physicalBatchId());
        assertNotEquals(first.physicalBatchId(), otherLocation.physicalBatchId());
    }

    @Test
    void testRejectsInvalidRequest() {
        assertAll(
            () -> assertThrows(NullPointerException.class, () -> new GlobalSequenceAppendRequest(
                null, 1, 10L, 3)),
            () -> assertThrows(IllegalArgumentException.class, () -> new GlobalSequenceAppendRequest(
                Uuid.ZERO_UUID, 1, 10L, 3)),
            () -> assertThrows(IllegalArgumentException.class, () -> new GlobalSequenceAppendRequest(
                TOPIC_ID, -1, 10L, 3)),
            () -> assertThrows(IllegalArgumentException.class, () -> new GlobalSequenceAppendRequest(
                TOPIC_ID, 1, -1L, 3)),
            () -> assertThrows(IllegalArgumentException.class, () -> new GlobalSequenceAppendRequest(
                TOPIC_ID, 1, 10L, 0))
        );
    }

    private static GlobalSequenceAppendRequest request(
        int recordCount,
        long partitionBaseOffset
    ) {
        return new GlobalSequenceAppendRequest(
            TOPIC_ID,
            1,
            partitionBaseOffset,
            recordCount
        );
    }
}
