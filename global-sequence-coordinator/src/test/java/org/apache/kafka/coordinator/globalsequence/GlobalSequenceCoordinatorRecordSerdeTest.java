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
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.Deserializer;
import org.apache.kafka.coordinator.globalsequence.generated.CoordinatorRecordType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newBatchIndexRecord;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newIndexerFenceRecord;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorRecordHelpers.newTopicMetadataRecord;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GlobalSequenceCoordinatorRecordSerdeTest {
    private final GlobalSequenceCoordinatorRecordSerde serde = new GlobalSequenceCoordinatorRecordSerde();

    static List<CoordinatorRecord> records() {
        Uuid topicId = new Uuid(1, 2);
        return List.of(
            newBatchIndexRecord(topicId, 3, 100, 102, 3, 10),
            newTopicMetadataRecord(topicId, 13),
            newIndexerFenceRecord(topicId, 3, 4, 5, 6, new Uuid(7, 8))
        );
    }

    static Stream<Arguments> versionZeroFixtures() {
        List<CoordinatorRecord> records = records();
        // Fixed bytes deliberately independent of the generated serializers.
        return Stream.of(
            Arguments.of(records.get(0),
                "0000" + "00000000000000010000000000000002" + "000000000000000a",
                "0000" + "00000003" + "0000000000000064" + "0000000000000066" + "00000003" + "00"),
            Arguments.of(records.get(1),
                "0001" + "00000000000000010000000000000002",
                "0000" + "000000000000000d" + "00"),
            Arguments.of(records.get(2),
                "0002" + "00000000000000010000000000000002" + "00000003",
                "0000" + "00000004" + "00000005" + "0000000000000006" + "00000000000000070000000000000008" + "00")
        );
    }

    @ParameterizedTest
    @MethodSource("versionZeroFixtures")
    void testVersionZeroWireFormat(CoordinatorRecord record, String keyHex, String valueHex) {
        byte[] key = HexFormat.of().parseHex(keyHex);
        byte[] value = HexFormat.of().parseHex(valueHex);
        assertArrayEquals(key, serde.serializeKey(record));
        assertArrayEquals(value, serde.serializeValue(record));
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        ByteBuffer valueBuffer = ByteBuffer.wrap(value);
        assertEquals(record, serde.deserialize(keyBuffer, valueBuffer));
        assertFalse(keyBuffer.hasRemaining());
        assertFalse(valueBuffer.hasRemaining());
    }

    @Test
    void testAllRecordTypesCovered() {
        assertEquals(CoordinatorRecordType.values().length, records().size());
        for (CoordinatorRecord record : records()) {
            CoordinatorRecordType type = CoordinatorRecordType.fromId(record.key().apiKey());
            assertEquals(type.newRecordKey().getClass(), record.key().getClass());
            assertEquals(type.newRecordValue().getClass(), record.value().message().getClass());
            assertEquals(0, record.value().version());
        }
    }

    @ParameterizedTest
    @MethodSource("records")
    void testTombstoneRoundTrip(CoordinatorRecord record) {
        CoordinatorRecord tombstone = CoordinatorRecord.tombstone(record.key());
        assertNull(serde.serializeValue(tombstone));
        assertEquals(tombstone, serde.deserialize(ByteBuffer.wrap(serde.serializeKey(tombstone)), null));
    }

    @ParameterizedTest
    @MethodSource("records")
    void testUnknownValueVersionsFail(CoordinatorRecord record) {
        for (short version : new short[] {-1, 1}) {
            assertThrows(Deserializer.UnknownRecordVersionException.class, () -> serde.deserialize(
                ByteBuffer.wrap(serde.serializeKey(record)),
                ByteBuffer.allocate(2).putShort(version).flip()));
        }
    }

    @Test
    void testUnknownRecordTypesFail() {
        for (short type : new short[] {-1, 3, Short.MAX_VALUE}) {
            Deserializer.UnknownRecordTypeException exception = assertThrows(
                Deserializer.UnknownRecordTypeException.class,
                () -> serde.deserialize(ByteBuffer.allocate(2).putShort(type).flip(), null));
            assertEquals(type, exception.unknownType());
        }
    }

    @ParameterizedTest
    @MethodSource("records")
    void testTruncatedRecordsFail(CoordinatorRecord record) {
        byte[] key = serde.serializeKey(record);
        byte[] value = serde.serializeValue(record);
        for (int length = 0; length < key.length; length++) {
            byte[] truncated = Arrays.copyOf(key, length);
            assertThrows(RuntimeException.class, () -> serde.deserialize(
                ByteBuffer.wrap(truncated), ByteBuffer.wrap(value)), "key length " + length);
        }
        for (int length = 0; length < value.length; length++) {
            byte[] truncated = Arrays.copyOf(value, length);
            assertThrows(RuntimeException.class, () -> serde.deserialize(
                ByteBuffer.wrap(key), ByteBuffer.wrap(truncated)), "value length " + length);
        }
    }

    @ParameterizedTest
    @MethodSource("records")
    void testUnknownTaggedFieldsSurviveRoundTrip(CoordinatorRecord record) {
        record.value().message().unknownTaggedFields().add(new RawTaggedField(5, new byte[] {1, 2, 3}));
        assertEquals(record, serde.deserialize(
            ByteBuffer.wrap(serde.serializeKey(record)),
            ByteBuffer.wrap(serde.serializeValue(record))));
    }
}
