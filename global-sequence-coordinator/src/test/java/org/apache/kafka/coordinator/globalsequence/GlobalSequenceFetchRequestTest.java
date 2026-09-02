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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GlobalSequenceFetchRequestTest {

    @Test
    void testValidatesGlobalFetchRequest() {
        Uuid topicId = Uuid.randomUuid();
        assertDoesNotThrow(() -> new GlobalSequenceFetchRequest(
            topicId,
            0L,
            1L,
            1024,
            IsolationLevel.READ_UNCOMMITTED
        ));
        assertThrows(NullPointerException.class, () -> new GlobalSequenceFetchRequest(
            null,
            0L,
            1L,
            1024,
            IsolationLevel.READ_UNCOMMITTED
        ));
        assertThrows(IllegalArgumentException.class, () -> new GlobalSequenceFetchRequest(
            topicId,
            1L,
            1L,
            1024,
            IsolationLevel.READ_UNCOMMITTED
        ));
        assertThrows(IllegalArgumentException.class, () -> new GlobalSequenceFetchRequest(
            topicId,
            0L,
            1L,
            0,
            IsolationLevel.READ_UNCOMMITTED
        ));
        assertThrows(NullPointerException.class, () -> new GlobalSequenceFetchRequest(
            topicId,
            0L,
            1L,
            1024,
            null
        ));
    }

    @Test
    void testValidatesPhysicalFetchRequest() {
        Uuid topicId = Uuid.randomUuid();
        assertDoesNotThrow(() -> new GlobalSequencePhysicalFetchRequest(
            topicId,
            0,
            10L,
            2,
            1024,
            IsolationLevel.READ_COMMITTED
        ));
        assertThrows(IllegalArgumentException.class, () -> new GlobalSequencePhysicalFetchRequest(
            topicId,
            -1,
            10L,
            2,
            1024,
            IsolationLevel.READ_COMMITTED
        ));
        assertThrows(IllegalArgumentException.class, () -> new GlobalSequencePhysicalFetchRequest(
            topicId,
            0,
            10L,
            0,
            1024,
            IsolationLevel.READ_COMMITTED
        ));
    }
}
