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
import org.apache.kafka.coordinator.globalsequence.generated.BatchIndexValue;
import org.apache.kafka.coordinator.globalsequence.generated.TopicMetadataValue;

import org.junit.jupiter.api.Test;

import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newBatchIndexRecord;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newIndexerFenceRecord;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newTopicMetadataRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GlobalSequenceCoordinatorRecordHelpersTest {
    private static final Uuid TOPIC_ID = new Uuid(1, 2);
    private static final Uuid REGISTRATION_ID = new Uuid(3, 4);

    @Test
    void testOffsetBoundaries() {
        CoordinatorRecord first = newBatchIndexRecord(TOPIC_ID, 0, 0, 0, 1, 0);
        assertEquals(1, ((BatchIndexValue) first.value().message()).recordCount());
        CoordinatorRecord last = newBatchIndexRecord(TOPIC_ID, 0, Long.MAX_VALUE - 1, Long.MAX_VALUE - 1, 1, Long.MAX_VALUE - 1);
        assertEquals(Long.MAX_VALUE - 1, ((BatchIndexValue) last.value().message()).physicalLastOffset());
        CoordinatorRecord metadata = newTopicMetadataRecord(TOPIC_ID, Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, ((TopicMetadataValue) metadata.value().message()).nextGlobalOffset());
    }

    @Test
    void testRejectInvalidBatchRanges() {
        assertThrows(IllegalArgumentException.class, () -> newBatchIndexRecord(TOPIC_ID, -1, 0, 0, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> newBatchIndexRecord(TOPIC_ID, 0, -1, 0, 2, 0));
        assertThrows(IllegalArgumentException.class, () -> newBatchIndexRecord(TOPIC_ID, 0, 0, 0, 1, -1));
        assertThrows(IllegalArgumentException.class, () -> newBatchIndexRecord(TOPIC_ID, 0, 1, 0, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> newBatchIndexRecord(TOPIC_ID, 0, 0, 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> newBatchIndexRecord(TOPIC_ID, 0, 0, 0, -1, 0));
        assertThrows(IllegalArgumentException.class, () -> newBatchIndexRecord(TOPIC_ID, 0, 100, 102, 2, 0));
        assertThrows(IllegalArgumentException.class, () -> newBatchIndexRecord(TOPIC_ID, 0, Long.MAX_VALUE, Long.MAX_VALUE, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> newBatchIndexRecord(TOPIC_ID, 0, 0, 0, 1, Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> newBatchIndexRecord(TOPIC_ID, 0, 0, 1, 2, Long.MAX_VALUE - 1));
    }

    @Test
    void testTopicRecreationHasDifferentKeys() {
        Uuid newTopicId = new Uuid(5, 6);
        assertNotEquals(newBatchIndexRecord(TOPIC_ID, 0, 0, 0, 1, 0).key(),
            newBatchIndexRecord(newTopicId, 0, 0, 0, 1, 0).key());
        assertNotEquals(newTopicMetadataRecord(TOPIC_ID, 0).key(), newTopicMetadataRecord(newTopicId, 0).key());
        assertNotEquals(newIndexerFenceRecord(TOPIC_ID, 0, 0, 0, 0, REGISTRATION_ID).key(),
            newIndexerFenceRecord(newTopicId, 0, 0, 0, 0, REGISTRATION_ID).key());
    }

    @Test
    void testRejectInvalidMetadataAndFence() {
        assertThrows(IllegalArgumentException.class, () -> newTopicMetadataRecord(TOPIC_ID, -1));
        assertThrows(IllegalArgumentException.class, () -> newIndexerFenceRecord(TOPIC_ID, -1, 0, 0, 0, REGISTRATION_ID));
        assertThrows(IllegalArgumentException.class, () -> newIndexerFenceRecord(TOPIC_ID, 0, -1, 0, 0, REGISTRATION_ID));
        assertThrows(IllegalArgumentException.class, () -> newIndexerFenceRecord(TOPIC_ID, 0, 0, -1, 0, REGISTRATION_ID));
        assertThrows(IllegalArgumentException.class, () -> newIndexerFenceRecord(TOPIC_ID, 0, 0, 0, -1, REGISTRATION_ID));
    }

    @Test
    void testRejectMissingIdentity() {
        assertThrows(NullPointerException.class, () -> newBatchIndexRecord(null, 0, 0, 0, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> newBatchIndexRecord(Uuid.ZERO_UUID, 0, 0, 0, 1, 0));
        assertThrows(NullPointerException.class, () -> newTopicMetadataRecord(null, 0));
        assertThrows(IllegalArgumentException.class, () -> newTopicMetadataRecord(Uuid.ZERO_UUID, 0));
        assertThrows(NullPointerException.class, () -> newIndexerFenceRecord(null, 0, 0, 0, 0, REGISTRATION_ID));
        assertThrows(IllegalArgumentException.class, () -> newIndexerFenceRecord(Uuid.ZERO_UUID, 0, 0, 0, 0, REGISTRATION_ID));
        assertThrows(NullPointerException.class, () -> newIndexerFenceRecord(TOPIC_ID, 0, 0, 0, 0, null));
        assertThrows(IllegalArgumentException.class, () -> newIndexerFenceRecord(TOPIC_ID, 0, 0, 0, 0, Uuid.ZERO_UUID));
    }
}
