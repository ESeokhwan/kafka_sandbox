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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.GlobalSequenceConsumerRecord;
import org.apache.kafka.clients.consumer.GlobalSequenceConsumerRecords;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.RecordDeserializationException.DeserializationExceptionOrigin;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.FetchGlobalSequenceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalSequencePageDecoderTest {
    private static final String TOPIC = "events";
    private static final Uuid TOPIC_ID = new Uuid(10, 20);
    private static final Uuid INDEX_TOPIC_ID = new Uuid(30, 40);
    private GlobalSequencePageDecoder<byte[], byte[]> decoder;

    @BeforeEach
    void setUp() {
        decoder = decoder(IsolationLevel.READ_UNCOMMITTED, true, 100, 1024 * 1024, () -> { });
    }

    @AfterEach
    void tearDown() {
        decoder.close();
    }

    @Test
    void testDecodesCompressedBatchesAcrossPartitionsAndPreservesRecordMetadata() {
        Header[] firstHeaders = {new RecordHeader("trace", bytes("a"))};
        MemoryRecords first = records(10, 7, Compression.gzip().build(),
            new SimpleRecord(1000, bytes("k0"), bytes("v0"), firstHeaders),
            new SimpleRecord(1001, null, bytes("v1"), new Header[0]),
            new SimpleRecord(1002, bytes("k2"), bytes("v2"), new Header[0]));
        MemoryRecords second = records(20, RecordBatch.NO_PARTITION_LEADER_EPOCH, Compression.NONE,
            new SimpleRecord(2000, bytes("k3"), null, new Header[0]),
            new SimpleRecord(2001, bytes("k4"), bytes("v4"), new Header[0]));
        FetchGlobalSequenceResponseData response = response(110, 105,
            entry(100, 0, 10, 3, 101, 103, first),
            entry(103, 2, 20, 2, 103, 105, second));

        GlobalSequenceConsumerRecords<byte[], byte[]> page =
            decoder.decode(TOPIC, TOPIC_ID, 101, 105, response);

        assertEquals(List.of(101L, 102L, 103L, 104L),
            page.records().stream().map(GlobalSequenceConsumerRecord::globalOffset).collect(Collectors.toList()));
        GlobalSequenceConsumerRecord<byte[], byte[]> firstRecord = page.records().get(0);
        assertEquals(0, firstRecord.physicalPartition());
        assertEquals(11, firstRecord.physicalOffset());
        assertEquals(1001, firstRecord.timestamp());
        assertEquals(TimestampType.CREATE_TIME, firstRecord.timestampType());
        assertEquals(Optional.of(7), firstRecord.leaderEpoch());
        assertEquals(-1, firstRecord.serializedKeySize());
        assertArrayEquals(bytes("v1"), firstRecord.value());
        assertEquals(Optional.empty(), page.records().get(2).leaderEpoch());
        assertEquals(-1, page.records().get(2).serializedValueSize());
        assertEquals(105, page.nextGlobalOffset());
        assertEquals(110, page.committedGlobalEndOffset());
        assertEquals(TOPIC_ID, page.topicId());
    }

    @Test
    void testUsesHeaderAwareDeserializersAndRetainsMutatedHeaders() {
        HeaderAwareDeserializer key = new HeaderAwareDeserializer("key-seen");
        HeaderAwareDeserializer value = new HeaderAwareDeserializer("value-seen");
        try (GlobalSequencePageDecoder<String, String> stringDecoder = new GlobalSequencePageDecoder<>(
            key, value, true, IsolationLevel.READ_UNCOMMITTED, 10, 1024, () -> { })) {
            MemoryRecords records = records(4, 9, Compression.NONE,
                new SimpleRecord(42, bytes("key"), bytes("value"),
                    new Header[] {new RecordHeader("input", bytes("header"))}));
            GlobalSequenceConsumerRecord<String, String> record = stringDecoder.decode(TOPIC, TOPIC_ID, 7, 8,
                response(8, 8, entry(7, 1, 4, 1, 7, 8, records))).records().get(0);

            assertEquals("key", record.key());
            assertEquals("value", record.value());
            assertTrue(key.headerAwareCalled);
            assertTrue(value.headerAwareCalled);
            assertArrayEquals(bytes("header"), record.headers().lastHeader("input").value());
            assertArrayEquals(bytes("yes"), record.headers().lastHeader("key-seen").value());
            assertArrayEquals(bytes("yes"), record.headers().lastHeader("value-seen").value());
        }
    }

    @Test
    void testReadCommittedAllowsAbortedGapsAndRepresentsPendingPage() {
        try (GlobalSequencePageDecoder<byte[], byte[]> committed = decoder(
            IsolationLevel.READ_COMMITTED, true, 100, 1024 * 1024, () -> { })) {
            MemoryRecords visible = records(30, 1, Compression.NONE,
                new SimpleRecord(bytes("v")));
            GlobalSequenceConsumerRecords<byte[], byte[]> page = committed.decode(TOPIC, TOPIC_ID, 5, 10,
                response(12, 9, entry(8, 0, 30, 1, 8, 9, visible)));
            assertEquals(8, page.records().get(0).globalOffset());
            assertEquals(9, page.nextGlobalOffset());

            FetchGlobalSequenceResponseData pending = response(12, 5).setTransactionPending(true);
            GlobalSequenceConsumerRecords<byte[], byte[]> pendingPage =
                committed.decode(TOPIC, TOPIC_ID, 5, 10, pending);
            assertTrue(pendingPage.isEmpty());
            assertTrue(pendingPage.transactionPending());

            GlobalSequenceConsumerRecords<byte[], byte[]> abortedPage =
                committed.decode(TOPIC, TOPIC_ID, 5, 10, response(12, 8));
            assertTrue(abortedPage.isEmpty());
            assertEquals(8, abortedPage.nextGlobalOffset());
        }
        assertThrows(CorruptRecordException.class,
            () -> decoder.decode(TOPIC, TOPIC_ID, 5, 10, response(12, 8)));
    }

    @Test
    void testPreservesPartialErrorAndThrowsEarlyErrorWithoutSnapshot() {
        MemoryRecords records = records(0, 1, Compression.NONE, new SimpleRecord(bytes("v")));
        FetchGlobalSequenceResponseData partial = response(10, 1,
            entry(0, 0, 0, 1, 0, 1, records))
            .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code())
            .setErrorMessage("source data deleted");
        GlobalSequenceConsumerRecords<byte[], byte[]> page =
            decoder.decode(TOPIC, TOPIC_ID, 0, 5, partial);
        assertEquals(1, page.count());
        assertInstanceOf(OffsetOutOfRangeException.class, page.error().orElseThrow());
        assertEquals("source data deleted", page.error().orElseThrow().getMessage());

        FetchGlobalSequenceResponseData early = new FetchGlobalSequenceResponseData()
            .setTopicId(TOPIC_ID)
            .setNextGlobalOffset(0)
            .setErrorCode(Errors.NOT_COORDINATOR.code())
            .setErrorMessage("route moved");
        NotCoordinatorException error = assertThrows(NotCoordinatorException.class,
            () -> decoder.decode(TOPIC, TOPIC_ID, 0, 5, early));
        assertEquals("route moved", error.getMessage());
    }

    @Test
    void testRejectsIdentityCursorMappingAndOrderingViolations() {
        MemoryRecords one = records(0, 1, Compression.NONE, new SimpleRecord(bytes("one")));
        assertCorrupt(() -> decoder.decode(TOPIC, TOPIC_ID, 0, 1,
            response(1, 1, entry(0, 0, 0, 1, 0, 1, one)).setTopicId(new Uuid(1, 2))));
        assertCorrupt(() -> decoder.decode(TOPIC, TOPIC_ID, 0, 1,
            response(1, 2, entry(0, 0, 0, 1, 0, 1, one))));
        assertCorrupt(() -> decoder.decode(TOPIC, TOPIC_ID, 0, 1,
            response(1, 1, entry(0, 0, 0, 1, 1, 1, one))));
        assertCorrupt(() -> decoder.decode(TOPIC, TOPIC_ID, 0, Long.MAX_VALUE,
            response(Long.MAX_VALUE, 1,
                entry(Long.MAX_VALUE, 0, 0, 1, Long.MAX_VALUE, Long.MAX_VALUE, one))));

        MemoryRecords two = records(1, 1, Compression.NONE, new SimpleRecord(bytes("two")));
        assertCorrupt(() -> decoder.decode(TOPIC, TOPIC_ID, 0, 3,
            response(3, 2,
                entry(0, 0, 0, 2, 0, 2, records(0, 1, Compression.NONE,
                    new SimpleRecord(bytes("a")), new SimpleRecord(bytes("b")))),
                entry(1, 1, 1, 1, 1, 2, two))));
    }

    @Test
    void testRejectsWrongPhysicalBatchCrcTruncationAndPageLimits() {
        MemoryRecords records = records(10, 3, Compression.NONE,
            new SimpleRecord(bytes("a")), new SimpleRecord(bytes("b")));
        assertCorrupt(() -> decoder.decode(TOPIC, TOPIC_ID, 0, 2,
            response(2, 2, entry(0, 0, 10, 3, 0, 2, records))));

        MemoryRecords corrupted = corrupted(records);
        assertCorrupt(() -> decoder.decode(TOPIC, TOPIC_ID, 0, 2,
            response(2, 2, entry(0, 0, 10, 2, 0, 2, corrupted))));
        try (GlobalSequencePageDecoder<byte[], byte[]> noCrc = decoder(
            IsolationLevel.READ_UNCOMMITTED, false, 2, 1024 * 1024, () -> { })) {
            assertEquals(2, noCrc.decode(TOPIC, TOPIC_ID, 0, 2,
                response(2, 2, entry(0, 0, 10, 2, 0, 2, corrupted))).count());
        }

        MemoryRecords truncated = truncated(records);
        assertCorrupt(() -> decoder.decode(TOPIC, TOPIC_ID, 0, 2,
            response(2, 2, entry(0, 0, 10, 2, 0, 2, truncated))));

        try (GlobalSequencePageDecoder<byte[], byte[]> oneBatch = decoder(
            IsolationLevel.READ_UNCOMMITTED, true, 1, 1024 * 1024, () -> { })) {
            assertCorrupt(() -> oneBatch.decode(TOPIC, TOPIC_ID, 0, 2,
                response(2, 2,
                    entry(0, 0, 10, 1, 0, 1,
                        records(10, 1, Compression.NONE, new SimpleRecord(bytes("a")))),
                    entry(1, 1, 20, 1, 1, 2,
                        records(20, 1, Compression.NONE, new SimpleRecord(bytes("b")))))));
        }
        try (GlobalSequencePageDecoder<byte[], byte[]> oneByte = decoder(
            IsolationLevel.READ_UNCOMMITTED, true, 2, 1, () -> { })) {
            assertEquals(1, oneByte.decode(TOPIC, TOPIC_ID, 0, 1,
                response(1, 1, entry(0, 0, 10, 1, 0, 1,
                    records(10, 1, Compression.NONE, new SimpleRecord(bytes("a")))))).count());
            assertCorrupt(() -> oneByte.decode(TOPIC, TOPIC_ID, 0, 2,
                response(2, 2,
                    entry(0, 0, 10, 1, 0, 1,
                        records(10, 1, Compression.NONE, new SimpleRecord(bytes("a")))),
                    entry(1, 1, 20, 1, 1, 2,
                        records(20, 1, Compression.NONE, new SimpleRecord(bytes("b")))))));
        }
    }

    @ParameterizedTest
    @EnumSource(DeserializationExceptionOrigin.class)
    void testDeserializationFailureContainsGlobalAndPhysicalIdentity(DeserializationExceptionOrigin origin) {
        Deserializer<byte[]> failing = new FailingDeserializer();
        Deserializer<byte[]> key = origin == DeserializationExceptionOrigin.KEY ? failing : new ByteArrayDeserializer();
        Deserializer<byte[]> value = origin == DeserializationExceptionOrigin.VALUE ? failing : new ByteArrayDeserializer();
        try (GlobalSequencePageDecoder<byte[], byte[]> failingDecoder = new GlobalSequencePageDecoder<>(
            key, value, true, IsolationLevel.READ_UNCOMMITTED, 10, 1024, () -> { })) {
            MemoryRecords records = records(33, 4, Compression.NONE,
                new SimpleRecord(bytes("key"), bytes("value")));
            RecordDeserializationException error = assertThrows(RecordDeserializationException.class,
                () -> failingDecoder.decode(TOPIC, TOPIC_ID, 70, 71,
                    response(71, 71, entry(70, 2, 33, 1, 70, 71, records))));
            assertEquals(origin, error.origin());
            assertEquals(2, error.topicPartition().partition());
            assertEquals(33, error.offset());
            assertTrue(error.getMessage().contains(TOPIC_ID.toString()));
            assertTrue(error.getMessage().contains("global offset 70"));
            assertTrue(error.getMessage().contains("physical partition 2 offset 33"));
        }
    }

    @Test
    void testDecodeBoundaryAndCompressedResourcesAreClosed() {
        TrackingBufferSupplier buffers = new TrackingBufferSupplier();
        AtomicInteger boundaries = new AtomicInteger();
        try (GlobalSequencePageDecoder<byte[], byte[]> resourceDecoder = new GlobalSequencePageDecoder<>(
            new ByteArrayDeserializer(), new ByteArrayDeserializer(), true, IsolationLevel.READ_UNCOMMITTED,
            10, 1024 * 1024, boundaries::incrementAndGet, buffers)) {
            MemoryRecords compressed = records(0, 1, Compression.gzip().build(),
                new SimpleRecord(bytes("value")));
            resourceDecoder.decode(TOPIC, TOPIC_ID, 0, 1,
                response(1, 1, entry(0, 0, 0, 1, 0, 1, compressed)));
            assertTrue(boundaries.get() >= 5);
            assertTrue(buffers.gets > 0);
            assertEquals(buffers.gets, buffers.releases);
        }
        assertTrue(buffers.closed);

        AtomicInteger calls = new AtomicInteger();
        try (GlobalSequencePageDecoder<byte[], byte[]> interrupted = decoder(
            IsolationLevel.READ_UNCOMMITTED, true, 10, 1024, () -> {
                if (calls.incrementAndGet() == 3)
                    throw new WakeupException();
            })) {
            MemoryRecords records = records(0, 1, Compression.NONE, new SimpleRecord(bytes("v")));
            assertThrows(WakeupException.class, () -> interrupted.decode(TOPIC, TOPIC_ID, 0, 1,
                response(1, 1, entry(0, 0, 0, 1, 0, 1, records))));
        }
    }

    private static GlobalSequencePageDecoder<byte[], byte[]> decoder(
        IsolationLevel isolationLevel,
        boolean checkCrcs,
        int maxBatches,
        int maxBytes,
        Runnable boundary
    ) {
        return new GlobalSequencePageDecoder<>(new ByteArrayDeserializer(), new ByteArrayDeserializer(),
            checkCrcs, isolationLevel, maxBatches, maxBytes, boundary);
    }

    private static FetchGlobalSequenceResponseData response(
        long committedEnd,
        long next,
        FetchGlobalSequenceResponseData.FetchedBatch... batches
    ) {
        return new FetchGlobalSequenceResponseData()
            .setTopicId(TOPIC_ID)
            .setIndexTopicId(INDEX_TOPIC_ID)
            .setIndexPartition(0)
            .setCoordinatorLeaderEpoch(3)
            .setIndexHighWatermark(9)
            .setCommittedGlobalEndOffset(committedEnd)
            .setNextGlobalOffset(next)
            .setBatches(List.of(batches));
    }

    private static FetchGlobalSequenceResponseData.FetchedBatch entry(
        long globalBase,
        int partition,
        long physicalBase,
        int count,
        long selectedStart,
        long selectedEnd,
        MemoryRecords records
    ) {
        return new FetchGlobalSequenceResponseData.FetchedBatch()
            .setGlobalBaseOffset(globalBase)
            .setPhysicalPartition(partition)
            .setPhysicalBaseOffset(physicalBase)
            .setPhysicalLastOffset(physicalBase + count - 1)
            .setRecordCount(count)
            .setSelectedGlobalStartOffset(selectedStart)
            .setSelectedGlobalEndOffset(selectedEnd)
            .setRecords(records);
    }

    private static MemoryRecords records(
        long baseOffset,
        int leaderEpoch,
        Compression compression,
        SimpleRecord... records
    ) {
        return MemoryRecords.withRecords(baseOffset, compression, leaderEpoch, records);
    }

    private static MemoryRecords corrupted(MemoryRecords records) {
        ByteBuffer copy = copy(records);
        int crcOffset = copy.position() + DefaultRecordBatch.CRC_OFFSET;
        copy.putInt(crcOffset, copy.getInt(crcOffset) ^ 1);
        return MemoryRecords.readableRecords(copy);
    }

    private static MemoryRecords truncated(MemoryRecords records) {
        ByteBuffer copy = copy(records);
        copy.limit(copy.limit() - 1);
        return MemoryRecords.readableRecords(copy);
    }

    private static ByteBuffer copy(MemoryRecords records) {
        ByteBuffer source = records.buffer().duplicate();
        ByteBuffer copy = ByteBuffer.allocate(source.remaining());
        copy.put(source).flip();
        return copy;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void assertCorrupt(Runnable operation) {
        assertThrows(CorruptRecordException.class, operation::run);
    }

    private static final class HeaderAwareDeserializer implements Deserializer<String> {
        private final String marker;
        private boolean headerAwareCalled;

        private HeaderAwareDeserializer(String marker) {
            this.marker = marker;
        }

        @Override
        public String deserialize(String topic, byte[] data) {
            throw new AssertionError("Header-aware deserialize method was not used");
        }

        @Override
        public String deserialize(String topic, Headers headers, ByteBuffer data) {
            headerAwareCalled = true;
            headers.add(marker, bytes("yes"));
            return new String(Utils.toArray(data), StandardCharsets.UTF_8);
        }
    }

    private static final class FailingDeserializer implements Deserializer<byte[]> {
        @Override
        public byte[] deserialize(String topic, byte[] data) {
            throw new IllegalArgumentException("cannot decode");
        }
    }

    private static final class TrackingBufferSupplier extends BufferSupplier {
        private int gets;
        private int releases;
        private boolean closed;

        @Override
        public ByteBuffer get(int capacity) {
            gets++;
            return ByteBuffer.allocate(capacity);
        }

        @Override
        public void release(ByteBuffer buffer) {
            releases++;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
