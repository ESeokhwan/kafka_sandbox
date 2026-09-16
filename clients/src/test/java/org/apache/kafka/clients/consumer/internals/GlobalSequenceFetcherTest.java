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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.GlobalSequenceConsumerRecords;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.FetchGlobalSequenceRequestData;
import org.apache.kafka.common.message.FetchGlobalSequenceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchGlobalSequenceRequest;
import org.apache.kafka.common.requests.FetchGlobalSequenceResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalSequenceFetcherTest {
    private static final String TOPIC = "events";
    private static final Uuid TOPIC_ID = new Uuid(10, 20);
    private static final Uuid INDEX_TOPIC_ID = new Uuid(30, 40);
    private final MockTime time = new MockTime();
    private final Metadata metadata = new Metadata(10, 100, 60000,
        new LogContext(), new ClusterResourceListeners());
    private final MockClient mockClient = new MockClient(time, metadata);
    private final ConsumerNetworkClient networkClient = new ConsumerNetworkClient(
        new LogContext(), mockClient, metadata, time, 10, 1000, Integer.MAX_VALUE);
    private final List<Optional<Uuid>> expectedIds = new ArrayList<>();
    private GlobalSequenceFetcher<byte[], byte[]> fetcher;

    @BeforeEach
    void setUp() {
        mockClient.updateMetadata(RequestTestUtils.metadataUpdateWithIds(2,
            Map.of(TOPIC, 1), Map.of(TOPIC, TOPIC_ID)));
        fetcher = fetcher(IsolationLevel.READ_UNCOMMITTED,
            (topic, expected, timer) -> {
                assertEquals(TOPIC, topic);
                expectedIds.add(expected);
                return TOPIC_ID;
            });
    }

    @AfterEach
    void tearDown() {
        fetcher.close();
    }

    @Test
    void testSendsOneBoundedPageWithResolvedUuidAndExactRange() {
        mockClient.prepareResponse(request -> {
            FetchGlobalSequenceRequestData data = ((FetchGlobalSequenceRequest) request).data();
            assertEquals(TOPIC_ID, data.topicId());
            assertEquals(7, data.globalStartOffset());
            assertEquals(9, data.globalEndOffsetExclusive());
            assertEquals(5, data.maxBatches());
            assertEquals(4096, data.maxBytes());
            assertEquals(IsolationLevel.READ_UNCOMMITTED.id(), data.isolationLevel());
            assertTrue(data.timeoutMs() > 0 && data.timeoutMs() <= 1000);
            return true;
        }, response(9, 8, Errors.NONE, entry(7, 10, bytes("value"))));

        GlobalSequenceConsumerRecords<byte[], byte[]> page =
            fetcher.fetch(TOPIC, Optional.empty(), 7, 9, time.timer(1000));
        assertEquals(List.of(Optional.empty()), expectedIds);
        assertEquals(1, page.count());
        assertEquals(7, page.records().get(0).globalOffset());
        assertArrayEquals(bytes("value"), page.records().get(0).value());
        assertEquals(8, page.nextGlobalOffset());
    }

    @Test
    void testDisconnectAndNoProgressErrorRefreshSameUuidBeforeRetry() {
        mockClient.prepareResponse(response(1, 1, Errors.NONE, entry(0, 0, bytes("ignored"))), true);
        FetchGlobalSequenceResponse retryResponse = response(1, 0, Errors.NOT_COORDINATOR);
        retryResponse.data().setThrottleTimeMs(25);
        mockClient.prepareResponse(retryResponse);
        mockClient.prepareResponse(response(1, 1, Errors.NONE, entry(0, 0, bytes("ok"))));

        GlobalSequenceConsumerRecords<byte[], byte[]> page =
            fetcher.fetch(TOPIC, Optional.empty(), 0, 1, time.timer(1000));
        assertEquals(1, page.count());
        assertEquals(List.of(Optional.empty(), Optional.of(TOPIC_ID), Optional.of(TOPIC_ID)), expectedIds);
        assertTrue(time.milliseconds() >= 35);
    }

    @Test
    void testPartialErrorReturnsImmediatelyButTerminalErrorWithoutProgressThrows() {
        mockClient.prepareResponse(response(2, 1, Errors.OFFSET_OUT_OF_RANGE,
            entry(0, 0, bytes("prefix"))));
        mockClient.prepareResponse(response(2, 2, Errors.NONE, entry(1, 1, bytes("unused"))));
        GlobalSequenceConsumerRecords<byte[], byte[]> page =
            fetcher.fetch(TOPIC, Optional.of(TOPIC_ID), 0, 2, time.timer(1000));
        assertEquals(1, page.count());
        assertTrue(page.error().orElseThrow() instanceof OffsetOutOfRangeException);
        assertEquals(1, mockClient.numAwaitingResponses());

        mockClient.reset();
        mockClient.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1,
            Map.of(TOPIC, 1), Map.of(TOPIC, TOPIC_ID)));
        mockClient.prepareResponse(response(2, 0, Errors.OFFSET_OUT_OF_RANGE));
        assertThrows(OffsetOutOfRangeException.class,
            () -> fetcher.fetch(TOPIC, Optional.of(TOPIC_ID), 0, 2, time.timer(1000)));
    }

    @Test
    void testRetriesUseTheOriginalDeadlineForWireTimeout() {
        AtomicInteger firstTimeout = new AtomicInteger();
        AtomicInteger secondTimeout = new AtomicInteger();
        mockClient.prepareResponse(request -> {
            firstTimeout.set(((FetchGlobalSequenceRequest) request).data().timeoutMs());
            time.sleep(200);
            return true;
        }, response(1, 1, Errors.NONE, entry(0, 0, bytes("ignored"))), true);
        mockClient.prepareResponse(request -> {
            secondTimeout.set(((FetchGlobalSequenceRequest) request).data().timeoutMs());
            return true;
        }, response(1, 1, Errors.NONE, entry(0, 0, bytes("ok"))));

        fetcher.fetch(TOPIC, Optional.empty(), 0, 1, time.timer(1000));
        assertEquals(1000, firstTimeout.get());
        assertTrue(secondTimeout.get() <= 790 && secondTimeout.get() > 0);
    }

    @Test
    void testReadCommittedRequiresVersionOneWithoutDowngrade() {
        fetcher.close();
        mockClient.setNodeApiVersions(NodeApiVersions.create(
            ApiKeys.FETCH_GLOBAL_SEQUENCE.id, (short) 0, (short) 0));
        fetcher = fetcher(IsolationLevel.READ_COMMITTED,
            (topic, expected, timer) -> TOPIC_ID);
        mockClient.prepareResponse(response(1, 1, Errors.NONE, entry(0, 0, bytes("unused"))));
        assertThrows(UnsupportedVersionException.class,
            () -> fetcher.fetch(TOPIC, Optional.empty(), 0, 1, time.timer(1000)));
    }

    private GlobalSequenceFetcher<byte[], byte[]> fetcher(
        IsolationLevel isolationLevel,
        GlobalSequenceFetcher.TopicIdResolver resolver
    ) {
        return new GlobalSequenceFetcher<>(networkClient, resolver,
            new ByteArrayDeserializer(), new ByteArrayDeserializer(), true, isolationLevel,
            5, 4096, 10);
    }

    private static FetchGlobalSequenceResponse response(
        long committedEnd,
        long next,
        Errors error,
        FetchGlobalSequenceResponseData.FetchedBatch... batches
    ) {
        return new FetchGlobalSequenceResponse(new FetchGlobalSequenceResponseData()
            .setTopicId(TOPIC_ID)
            .setIndexTopicId(INDEX_TOPIC_ID)
            .setIndexPartition(0)
            .setCoordinatorLeaderEpoch(3)
            .setIndexHighWatermark(10)
            .setCommittedGlobalEndOffset(committedEnd)
            .setNextGlobalOffset(next)
            .setErrorCode(error.code())
            .setErrorMessage(error == Errors.NONE ? null : error.message())
            .setBatches(List.of(batches)));
    }

    private static FetchGlobalSequenceResponseData.FetchedBatch entry(
        long globalOffset,
        long physicalOffset,
        byte[] value
    ) {
        MemoryRecords records = MemoryRecords.withRecords(physicalOffset, Compression.NONE,
            new SimpleRecord(value));
        return new FetchGlobalSequenceResponseData.FetchedBatch()
            .setGlobalBaseOffset(globalOffset)
            .setPhysicalPartition(0)
            .setPhysicalBaseOffset(physicalOffset)
            .setPhysicalLastOffset(physicalOffset)
            .setRecordCount(1)
            .setSelectedGlobalStartOffset(globalOffset)
            .setSelectedGlobalEndOffset(globalOffset + 1)
            .setRecords(records);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
