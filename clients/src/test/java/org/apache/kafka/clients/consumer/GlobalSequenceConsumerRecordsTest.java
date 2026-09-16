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
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalSequenceConsumerRecordsTest {
    private static final Uuid TOPIC_ID = new Uuid(10, 20);

    private static GlobalSequenceConsumerRecord<String, String> record(long global, int partition, long physical) {
        return new GlobalSequenceConsumerRecord<>("events", global, partition, physical, 123,
            TimestampType.LOG_APPEND_TIME, -1, -1, null, null,
            new RecordHeaders().add("header", new byte[] {1}), Optional.of(7));
    }

    @Test
    void testGlobalOrderAndPhysicalMetadataSurviveWithoutPartitionGrouping() {
        var first = record(2, 1, 80);
        var second = record(4, 0, 30);
        var third = record(5, 1, 81);
        var input = List.of(first, second, third);
        var page = new GlobalSequenceConsumerRecords<>(input, 9, TOPIC_ID, 12, false, Optional.empty());
        List<GlobalSequenceConsumerRecord<String, String>> iterated = new ArrayList<>();
        page.forEach(iterated::add);
        assertEquals(input, iterated);
        assertSame(first, page.records().get(0));
        assertEquals(80, page.records().get(0).physicalOffset());
        assertEquals(30, page.records().get(1).physicalOffset());
        assertEquals(123, page.records().get(0).timestamp());
        assertEquals(TimestampType.LOG_APPEND_TIME, page.records().get(0).timestampType());
        assertSame(first.headers(), page.records().get(0).headers());
        assertEquals(Optional.of(7), page.records().get(0).leaderEpoch());
        assertEquals(3, page.count());
        assertFalse(page.isEmpty());
        assertEquals(9, page.nextGlobalOffset(), "Aborted suffix offsets are not derived from the final returned record");
        assertEquals(TOPIC_ID, page.topicId());
        assertEquals(12, page.committedGlobalEndOffset());
    }

    @Test
    void testListSnapshotAndAllMutationPathsAreImmutable() {
        var original = record(0, 1, 30);
        var input = new ArrayList<>(List.of(original));
        var page = new GlobalSequenceConsumerRecords<>(input, 1, TOPIC_ID, 1, false, Optional.empty());
        input.clear();
        assertEquals(List.of(original), page.records());
        assertThrows(UnsupportedOperationException.class, () -> page.records().add(record(1, 0, 90)));
        assertThrows(UnsupportedOperationException.class, () -> page.records().set(0, record(0, 0, 90)));
        assertThrows(UnsupportedOperationException.class, () -> page.records().clear());
        Iterator<GlobalSequenceConsumerRecord<String, String>> iterator = page.iterator();
        assertSame(original, iterator.next());
        assertThrows(UnsupportedOperationException.class, iterator::remove);
        assertEquals(1, page.count());
    }

    @Test
    void testEmptyPagesDistinguishEndPendingAndFilteredProgress() {
        var end = new GlobalSequenceConsumerRecords<>(List.of(), 12, TOPIC_ID, 12, false, Optional.empty());
        var pending = new GlobalSequenceConsumerRecords<>(List.of(), 3, TOPIC_ID, 12, true, Optional.empty());
        var aborted = new GlobalSequenceConsumerRecords<>(List.of(), 6, TOPIC_ID, 12, false, Optional.empty());
        for (var page : List.of(end, pending, aborted)) {
            assertTrue(page.isEmpty());
            assertEquals(0, page.count());
            assertTrue(page.error().isEmpty());
        }
        assertEquals(end.committedGlobalEndOffset(), end.nextGlobalOffset());
        assertFalse(end.transactionPending());
        assertEquals(3, pending.nextGlobalOffset());
        assertTrue(pending.transactionPending());
        assertEquals(6, aborted.nextGlobalOffset());
        assertFalse(aborted.transactionPending());
    }

    @Test
    void testPendingPageRetainsThePrecedingRecordsAndExactCursor() {
        var prefix = List.of(record(3, 0, 20));
        var page = new GlobalSequenceConsumerRecords<>(prefix, 5, TOPIC_ID, 10, true, Optional.empty());
        assertEquals(prefix, page.records());
        assertEquals(5, page.nextGlobalOffset());
        assertTrue(page.transactionPending());
        assertTrue(page.error().isEmpty());
    }

    @ParameterizedTest
    @MethodSource("partialErrors")
    void testPartialErrorPreservesRecordsOrAnEntirelyFilteredPrefix(ApiException error) {
        var prefix = List.of(record(2, 1, 10));
        var page = new GlobalSequenceConsumerRecords<>(prefix, 5, TOPIC_ID, 10, false, Optional.of(error));
        assertEquals(prefix, page.records());
        assertEquals(5, page.nextGlobalOffset());
        assertEquals(TOPIC_ID, page.topicId());
        assertSame(error, page.error().orElseThrow());
        assertFalse(page.transactionPending());

        var filtered = new GlobalSequenceConsumerRecords<>(List.of(), 5, TOPIC_ID, 10, false, Optional.of(error));
        assertTrue(filtered.isEmpty());
        assertEquals(5, filtered.nextGlobalOffset());
        assertSame(error, filtered.error().orElseThrow());
    }

    static Stream<ApiException> partialErrors() {
        return Stream.of(new TimeoutException("Read timed out after prefix"),
            new OffsetOutOfRangeException("Source data after prefix was deleted"));
    }

    @Test
    void testEmptyRangeOrStartBeyondCommittedEndDoesNotClampTheCursor() {
        var emptyRange = new GlobalSequenceConsumerRecords<>(List.of(), 2, TOPIC_ID, 10, false, Optional.empty());
        var beyondEnd = new GlobalSequenceConsumerRecords<>(List.of(), 15, TOPIC_ID, 10, false, Optional.empty());
        assertTrue(emptyRange.isEmpty());
        assertEquals(2, emptyRange.nextGlobalOffset());
        assertTrue(beyondEnd.isEmpty());
        assertEquals(15, beyondEnd.nextGlobalOffset());
        assertEquals(10, beyondEnd.committedGlobalEndOffset());
    }

    @Test
    void testLegacyConstructorPreservesItsOrderCursorAndUnknownMetadata() {
        // Existing callers could create synthetic pages with arbitrary order or unknown cursors.
        var input = new ArrayList<>(List.of(record(3, 1, 10), record(1, 0, 20)));
        var page = new GlobalSequenceConsumerRecords<>(input, -1);
        var expected = List.copyOf(input);
        input.clear();
        assertEquals(expected, page.records());
        assertEquals(-1, page.nextGlobalOffset());
        assertEquals(Uuid.ZERO_UUID, page.topicId());
        assertEquals(GlobalSequenceConsumerRecords.UNKNOWN_COMMITTED_GLOBAL_END_OFFSET, page.committedGlobalEndOffset());
        assertFalse(page.transactionPending());
        assertTrue(page.error().isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> page.records().clear());
    }

    @Test
    void testRejectsUnknownSnapshotAndContradictoryPendingStatus() {
        assertThrows(IllegalArgumentException.class, () ->
            new GlobalSequenceConsumerRecords<>(List.of(), 0, Uuid.ZERO_UUID, 0, false, Optional.empty()));
        assertThrows(IllegalArgumentException.class, () ->
            new GlobalSequenceConsumerRecords<>(List.of(), -1, TOPIC_ID, 0, false, Optional.empty()));
        assertThrows(IllegalArgumentException.class, () ->
            new GlobalSequenceConsumerRecords<>(List.of(), 0, TOPIC_ID, -1, false, Optional.empty()));
        assertThrows(IllegalArgumentException.class, () ->
            new GlobalSequenceConsumerRecords<>(List.of(), 5, TOPIC_ID, 5, true, Optional.empty()));
        assertThrows(IllegalArgumentException.class, () ->
            new GlobalSequenceConsumerRecords<>(List.of(), 7, TOPIC_ID, 5, true, Optional.empty()));
        assertThrows(IllegalArgumentException.class, () ->
            new GlobalSequenceConsumerRecords<>(List.of(), 0, TOPIC_ID, 5, true, Optional.of(new TimeoutException())));
    }

    @ParameterizedTest
    @MethodSource("invalidRecordOffsets")
    void testRejectsNegativeDuplicateDescendingOrUnprocessedOffsets(List<GlobalSequenceConsumerRecord<String, String>> records) {
        assertThrows(IllegalArgumentException.class, () ->
            new GlobalSequenceConsumerRecords<>(records, 5, TOPIC_ID, 10, false, Optional.empty()));
    }

    static Stream<List<GlobalSequenceConsumerRecord<String, String>>> invalidRecordOffsets() {
        return Stream.of(List.of(record(-1, 0, 0)), List.of(record(1, 0, 0), record(1, 1, 0)),
            List.of(record(3, 0, 0), record(2, 1, 0)), List.of(record(5, 0, 0)), List.of(record(6, 0, 0)));
    }

    @Test
    void testNonemptyPageCannotAdvanceBeyondSnapshotAndSupportsLongMaxBoundary() {
        assertThrows(IllegalArgumentException.class, () -> new GlobalSequenceConsumerRecords<>(
            List.of(record(2, 0, 0)), 15, TOPIC_ID, 10, false, Optional.empty()));
        assertThrows(IllegalArgumentException.class, () -> new GlobalSequenceConsumerRecords<>(
            List.of(record(10, 0, 0)), 11, TOPIC_ID, 10, false, Optional.empty()));
        var page = new GlobalSequenceConsumerRecords<>(List.of(record(Long.MAX_VALUE - 1, 0, 0)),
            Long.MAX_VALUE, TOPIC_ID, Long.MAX_VALUE, false, Optional.empty());
        assertEquals(Long.MAX_VALUE, page.nextGlobalOffset());
        assertEquals(Long.MAX_VALUE - 1, page.records().get(0).globalOffset());
    }

    @Test
    void testNullContainersAndElementsAreRejectedByBothConstructors() {
        assertThrows(NullPointerException.class, () -> new GlobalSequenceConsumerRecords<>(null, 0));
        List<GlobalSequenceConsumerRecord<String, String>> nullElement = Arrays.asList(record(0, 0, 0), null);
        assertThrows(NullPointerException.class, () -> new GlobalSequenceConsumerRecords<>(nullElement, 1));
        assertThrows(NullPointerException.class, () -> new GlobalSequenceConsumerRecords<>(null, 0, TOPIC_ID, 0, false, Optional.empty()));
        assertThrows(NullPointerException.class, () -> new GlobalSequenceConsumerRecords<>(nullElement, 1, TOPIC_ID, 1, false, Optional.empty()));
        assertThrows(NullPointerException.class, () -> new GlobalSequenceConsumerRecords<>(List.of(), 0, null, 0, false, Optional.empty()));
        assertThrows(NullPointerException.class, () -> new GlobalSequenceConsumerRecords<>(List.of(), 0, TOPIC_ID, 0, false, null));
    }
}
