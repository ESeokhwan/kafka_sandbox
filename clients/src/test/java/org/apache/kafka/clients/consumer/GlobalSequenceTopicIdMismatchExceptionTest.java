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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.RetriableException;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalSequenceTopicIdMismatchExceptionTest {
    private static final Uuid EXPECTED = new Uuid(Long.MIN_VALUE, 123);
    private static final Uuid ACTUAL = new Uuid(456, Long.MAX_VALUE);

    @Test
    void testIdentityMismatchIsNotRetriableAndRetainsBothIdentitiesAfterSerialization() throws Exception {
        var error = new GlobalSequenceTopicIdMismatchException("events", EXPECTED, ACTUAL);
        assertFalse(RetriableException.class.isInstance(error));
        assertTrue(error.getMessage().contains("events"));
        assertTrue(error.getMessage().contains(EXPECTED.toString()));
        assertTrue(error.getMessage().contains(ACTUAL.toString()));
        var buffer = new ByteArrayOutputStream();
        try (var out = new ObjectOutputStream(buffer)) {
            out.writeObject(error);
        }
        try (var in = new ObjectInputStream(new ByteArrayInputStream(buffer.toByteArray()))) {
            var restored = (GlobalSequenceTopicIdMismatchException) in.readObject();
            assertEquals("events", restored.topic());
            assertEquals(EXPECTED, restored.expectedTopicId());
            assertEquals(ACTUAL, restored.actualTopicId());
            assertEquals(error.getMessage(), restored.getMessage());
        }
    }

    @Test
    void testUnknownOrEqualIdentityIsNotAMismatch() {
        assertThrows(IllegalArgumentException.class, () -> new GlobalSequenceTopicIdMismatchException("events", EXPECTED, EXPECTED));
        assertThrows(IllegalArgumentException.class, () -> new GlobalSequenceTopicIdMismatchException("events", Uuid.ZERO_UUID, ACTUAL));
        assertThrows(IllegalArgumentException.class, () -> new GlobalSequenceTopicIdMismatchException("events", EXPECTED, Uuid.ZERO_UUID));
        assertThrows(NullPointerException.class, () -> new GlobalSequenceTopicIdMismatchException(null, EXPECTED, ACTUAL));
        assertThrows(NullPointerException.class, () -> new GlobalSequenceTopicIdMismatchException("events", null, ACTUAL));
        assertThrows(NullPointerException.class, () -> new GlobalSequenceTopicIdMismatchException("events", EXPECTED, null));
    }
}
