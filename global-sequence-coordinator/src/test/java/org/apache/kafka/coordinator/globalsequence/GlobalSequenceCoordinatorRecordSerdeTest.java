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
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.Deserializer;
import org.apache.kafka.coordinator.globalsequence.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.globalsequence.generated.GlobalSequenceIndexLogKey;
import org.apache.kafka.coordinator.globalsequence.generated.GlobalSequenceIndexLogValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalSequenceCoordinatorRecordSerdeTest {
    private GlobalSequenceCoordinatorRecordSerde serde;

    @BeforeEach
    void setUp() {
        serde = new GlobalSequenceCoordinatorRecordSerde();
    }

    @Test
    void testSerializeKey() {
        CoordinatorRecord record = indexRecord();

        assertArrayEquals(
            MessageUtil.toCoordinatorTypePrefixedBytes(record.key()),
            serde.serializeKey(record)
        );
    }

    @Test
    void testSerializeValue() {
        CoordinatorRecord record = indexRecord();

        assertArrayEquals(
            MessageUtil.toVersionPrefixedBytes(record.value().version(), record.value().message()),
            serde.serializeValue(record)
        );
    }

    @Test
    void testRoundTripVersionZero() {
        CoordinatorRecord expected = indexRecord();
        CoordinatorRecord actual = serde.deserialize(
            MessageUtil.toCoordinatorTypePrefixedByteBuffer(expected.key()),
            MessageUtil.toVersionPrefixedByteBuffer(
                expected.value().version(),
                expected.value().message()
            )
        );

        assertEquals(expected, actual);
    }

    @Test
    void testTombstoneRoundTrip() {
        GlobalSequenceIndexLogKey key = new GlobalSequenceIndexLogKey()
            .setTopicId(Uuid.randomUuid())
            .setGlobalOffset(10L);

        CoordinatorRecord actual = serde.deserialize(
            MessageUtil.toCoordinatorTypePrefixedByteBuffer(key),
            null
        );

        assertEquals(key, actual.key());
        assertNull(actual.value());
        assertNull(serde.serializeValue(CoordinatorRecord.tombstone(key)));
    }

    @Test
    void testUnknownRecordType() {
        ByteBuffer keyBuffer = ByteBuffer.allocate(2).putShort((short) 255);
        keyBuffer.rewind();

        Deserializer.UnknownRecordTypeException exception = assertThrows(
            Deserializer.UnknownRecordTypeException.class,
            () -> serde.deserialize(keyBuffer, ByteBuffer.allocate(0))
        );
        assertEquals((short) 255, exception.unknownType());
    }

    @Test
    void testEmptyBuffers() {
        RuntimeException keyException = assertThrows(
            RuntimeException.class,
            () -> serde.deserialize(ByteBuffer.allocate(0), ByteBuffer.allocate(0))
        );
        assertEquals("Could not read version from key's buffer.", keyException.getMessage());

        ApiMessage key = new GlobalSequenceIndexLogKey()
            .setTopicId(Uuid.randomUuid())
            .setGlobalOffset(10L);
        RuntimeException valueException = assertThrows(
            RuntimeException.class,
            () -> serde.deserialize(
                MessageUtil.toCoordinatorTypePrefixedByteBuffer(key),
                ByteBuffer.allocate(0)
            )
        );
        assertEquals("Could not read version from value's buffer.", valueException.getMessage());
    }

    @Test
    void testAllRecordVersionsRoundTrip() {
        for (CoordinatorRecordType recordType : CoordinatorRecordType.values()) {
            ApiMessage key = recordType.newRecordKey();
            ApiMessage value = recordType.newRecordValue();

            for (short version = value.lowestSupportedVersion();
                 version <= value.highestSupportedVersion();
                 version++) {
                ApiMessageAndVersion expectedValue = new ApiMessageAndVersion(value, version);
                CoordinatorRecord actual = serde.deserialize(
                    MessageUtil.toCoordinatorTypePrefixedByteBuffer(key),
                    MessageUtil.toVersionPrefixedByteBuffer(version, value)
                );

                assertEquals(key, actual.key());
                assertEquals(expectedValue, actual.value());
            }
        }
    }

    @Test
    void testCorruptRecordBytes() {
        ByteBuffer keyBuffer = ByteBuffer.allocate(2).putShort((short) 0);
        keyBuffer.rewind();
        ByteBuffer valueBuffer = ByteBuffer.allocate(2).putShort((short) 0);
        valueBuffer.rewind();

        RuntimeException exception = assertThrows(
            RuntimeException.class,
            () -> serde.deserialize(keyBuffer, valueBuffer)
        );
        assertTrue(exception.getMessage().startsWith(
            "Could not read record with version 0 from key's buffer due to"
        ));
    }

    private static CoordinatorRecord indexRecord() {
        return CoordinatorRecord.record(
            new GlobalSequenceIndexLogKey()
                .setTopicId(Uuid.randomUuid())
                .setGlobalOffset(10L),
            new ApiMessageAndVersion(
                new GlobalSequenceIndexLogValue()
                    .setRecordsCount(3)
                    .setPartitionIndex(1)
                    .setPartitionOffset(20L),
                (short) 0
            )
        );
    }
}
