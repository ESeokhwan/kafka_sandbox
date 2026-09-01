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
package kafka.server;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.LookupGlobalSequenceIndexResponseData;
import org.apache.kafka.common.message.WriteGlobalSequenceIndexResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.WriteGlobalSequenceIndexRequest;
import org.apache.kafka.common.requests.WriteGlobalSequenceIndexResponse;
import org.apache.kafka.common.requests.LookupGlobalSequenceIndexRequest;
import org.apache.kafka.common.requests.LookupGlobalSequenceIndexResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceAppendRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceAppendResult;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinator;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceIndexRecord;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookupRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookupResult;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GlobalSequenceIndexRoutingManagerTest {
    private static final int LOCAL_BROKER_ID = 1;
    private static final int INDEX_PARTITION = 2;
    private static final long REQUEST_TIMEOUT_MS = 1_000L;
    private static final long RETRY_BACKOFF_MS = 50L;
    private static final Uuid TOPIC_ID = Uuid.fromString("AAAAAAAAAAAAAAAAAAAAAQ");
    private static final ListenerName LISTENER_NAME = ListenerName.normalised("PLAINTEXT");
    private static final GlobalSequenceAppendRequest APPEND_REQUEST =
        new GlobalSequenceAppendRequest(TOPIC_ID, 3, 42L, 4);
    private static final GlobalSequenceLookupRequest LOOKUP_REQUEST =
        new GlobalSequenceLookupRequest(TOPIC_ID, 2L, 6L);

    private final GlobalSequenceCoordinator coordinator = mock(GlobalSequenceCoordinator.class);
    private final MetadataCache metadataCache = mock(MetadataCache.class);
    private final AutoTopicCreationManager autoTopicCreationManager = mock(AutoTopicCreationManager.class);
    private final KafkaClient networkClient = mock(KafkaClient.class);
    private final MockTime time = new MockTime();
    private GlobalSequenceIndexRoutingManager manager;

    @BeforeEach
    void setUp() {
        when(coordinator.partitionFor(TOPIC_ID)).thenReturn(INDEX_PARTITION);
        when(metadataCache.contains(Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)).thenReturn(true);
        manager = new GlobalSequenceIndexRoutingManager(
            LOCAL_BROKER_ID,
            coordinator,
            metadataCache,
            autoTopicCreationManager,
            LISTENER_NAME,
            networkClient,
            time,
            Math.toIntExact(REQUEST_TIMEOUT_MS),
            RETRY_BACKOFF_MS
        );
    }

    @Test
    void testExecutesAppendLocallyWhenThisBrokerIsLeader() {
        GlobalSequenceAppendResult expected = new GlobalSequenceAppendResult(10L, 4, false);
        when(metadataCache.getPartitionLeaderEndpoint(
            Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME,
            INDEX_PARTITION,
            LISTENER_NAME
        )).thenReturn(Optional.of(new Node(LOCAL_BROKER_ID, "localhost", 9092)));
        when(coordinator.appendIndex(APPEND_REQUEST)).thenReturn(CompletableFuture.completedFuture(expected));

        CompletableFuture<GlobalSequenceAppendResult> result = manager.appendIndex(APPEND_REQUEST);

        assertTrue(manager.generateRequestsForTest().isEmpty());
        assertEquals(expected, result.join());
        verify(coordinator).appendIndex(APPEND_REQUEST);
    }

    @Test
    void testBuildsRemoteRequestForIndexLeader() {
        Node remoteLeader = new Node(2, "remote", 9093);
        when(metadataCache.getPartitionLeaderEndpoint(
            Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME,
            INDEX_PARTITION,
            LISTENER_NAME
        )).thenReturn(Optional.of(remoteLeader));

        CompletableFuture<GlobalSequenceAppendResult> result = manager.appendIndex(APPEND_REQUEST);
        RequestAndCompletionHandler request = onlyRequest(manager.generateRequestsForTest());
        WriteGlobalSequenceIndexRequest wireRequest = (WriteGlobalSequenceIndexRequest) request.request.build();

        assertEquals(remoteLeader, request.destination);
        assertEquals(TOPIC_ID, wireRequest.data().topicId());
        assertEquals(3, wireRequest.data().physicalPartition());
        assertEquals(42L, wireRequest.data().physicalBaseOffset());
        assertEquals(4, wireRequest.data().recordCount());
        verify(coordinator, never()).appendIndex(APPEND_REQUEST);

        request.handler.onComplete(response(new WriteGlobalSequenceIndexResponseData()
            .setGlobalBaseOffset(20L)
            .setRecordCount(4)
            .setDuplicateBatch(true)));

        assertEquals(new GlobalSequenceAppendResult(20L, 4, true), result.join());
    }

    @Test
    void testCreatesMissingIndexTopicBeforeRetryingAppend() {
        GlobalSequenceAppendResult expected = new GlobalSequenceAppendResult(10L, 4, false);
        when(metadataCache.contains(Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)).thenReturn(false, true);
        when(metadataCache.getPartitionLeaderEndpoint(
            Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME,
            INDEX_PARTITION,
            LISTENER_NAME
        )).thenReturn(Optional.of(new Node(LOCAL_BROKER_ID, "localhost", 9092)));
        when(coordinator.appendIndex(APPEND_REQUEST)).thenReturn(CompletableFuture.completedFuture(expected));

        CompletableFuture<GlobalSequenceAppendResult> result = manager.appendIndex(APPEND_REQUEST);

        assertTrue(manager.generateRequestsForTest().isEmpty());
        assertFalse(result.isDone());
        verify(autoTopicCreationManager).createGlobalSequenceIndexTopic();
        verify(coordinator, never()).appendIndex(APPEND_REQUEST);

        time.sleep(RETRY_BACKOFF_MS);

        assertTrue(manager.generateRequestsForTest().isEmpty());
        assertEquals(expected, result.join());
        verify(coordinator).appendIndex(APPEND_REQUEST);
    }

    @Test
    void testExecutesLookupLocallyWhenThisBrokerIsLeader() {
        GlobalSequenceLookupResult expected = new GlobalSequenceLookupResult(List.of(
            new GlobalSequenceIndexRecord(TOPIC_ID, 0L, 4, 3, 42L),
            new GlobalSequenceIndexRecord(TOPIC_ID, 4L, 4, 1, 10L)
        ));
        when(metadataCache.getPartitionLeaderEndpoint(
            Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME,
            INDEX_PARTITION,
            LISTENER_NAME
        )).thenReturn(Optional.of(new Node(LOCAL_BROKER_ID, "localhost", 9092)));
        when(coordinator.lookupIndex(LOOKUP_REQUEST)).thenReturn(CompletableFuture.completedFuture(expected));

        CompletableFuture<GlobalSequenceLookupResult> result = manager.lookup(LOOKUP_REQUEST);

        assertTrue(manager.generateRequestsForTest().isEmpty());
        assertEquals(expected, result.join());
        verify(coordinator).lookupIndex(LOOKUP_REQUEST);
    }

    @Test
    void testBuildsRemoteLookupRequestAndMapsResponse() {
        Node remoteLeader = new Node(2, "remote", 9093);
        when(metadataCache.getPartitionLeaderEndpoint(
            Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME,
            INDEX_PARTITION,
            LISTENER_NAME
        )).thenReturn(Optional.of(remoteLeader));

        CompletableFuture<GlobalSequenceLookupResult> result = manager.lookup(LOOKUP_REQUEST);
        RequestAndCompletionHandler request = onlyRequest(manager.generateRequestsForTest());
        LookupGlobalSequenceIndexRequest wireRequest =
            (LookupGlobalSequenceIndexRequest) request.request.build();

        assertEquals(remoteLeader, request.destination);
        assertEquals(TOPIC_ID, wireRequest.data().topicId());
        assertEquals(2L, wireRequest.data().globalStartOffset());
        assertEquals(6L, wireRequest.data().globalEndOffsetExclusive());
        assertTrue(wireRequest.data().internalRequest());
        verify(coordinator, never()).lookupIndex(LOOKUP_REQUEST);

        request.handler.onComplete(response(new LookupGlobalSequenceIndexResponseData()
            .setIndexEntries(List.of(
                new LookupGlobalSequenceIndexResponseData.GlobalSequenceIndexEntry()
                    .setGlobalBaseOffset(0L)
                    .setRecordCount(4)
                    .setPhysicalPartition(3)
                    .setPhysicalBaseOffset(42L),
                new LookupGlobalSequenceIndexResponseData.GlobalSequenceIndexEntry()
                    .setGlobalBaseOffset(4L)
                    .setRecordCount(4)
                    .setPhysicalPartition(1)
                    .setPhysicalBaseOffset(10L)
            ))));

        assertEquals(new GlobalSequenceLookupResult(List.of(
            new GlobalSequenceIndexRecord(TOPIC_ID, 0L, 4, 3, 42L),
            new GlobalSequenceIndexRecord(TOPIC_ID, 4L, 4, 1, 10L)
        )), result.join());
    }

    @Test
    void testRediscoversLeaderAfterNotCoordinator() {
        Node staleLeader = new Node(2, "stale", 9093);
        when(metadataCache.getPartitionLeaderEndpoint(
            Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME,
            INDEX_PARTITION,
            LISTENER_NAME
        )).thenReturn(
            Optional.of(staleLeader),
            Optional.of(new Node(LOCAL_BROKER_ID, "localhost", 9092))
        );
        GlobalSequenceAppendResult expected = new GlobalSequenceAppendResult(30L, 4, false);
        when(coordinator.appendIndex(APPEND_REQUEST)).thenReturn(CompletableFuture.completedFuture(expected));

        CompletableFuture<GlobalSequenceAppendResult> result = manager.appendIndex(APPEND_REQUEST);
        RequestAndCompletionHandler firstRequest = onlyRequest(manager.generateRequestsForTest());
        firstRequest.handler.onComplete(response(new WriteGlobalSequenceIndexResponseData()
            .setErrorCode(Errors.NOT_COORDINATOR.code())
            .setErrorMessage("leadership changed")));

        assertTrue(manager.generateRequestsForTest().isEmpty());
        time.sleep(RETRY_BACKOFF_MS);
        assertTrue(manager.generateRequestsForTest().isEmpty());
        assertEquals(expected, result.join());
        verify(coordinator).appendIndex(APPEND_REQUEST);
    }

    @Test
    void testFailsNonRetriableRemoteError() {
        when(metadataCache.getPartitionLeaderEndpoint(
            Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME,
            INDEX_PARTITION,
            LISTENER_NAME
        )).thenReturn(Optional.of(new Node(2, "remote", 9093)));

        CompletableFuture<GlobalSequenceAppendResult> result = manager.appendIndex(APPEND_REQUEST);
        onlyRequest(manager.generateRequestsForTest()).handler.onComplete(response(
            new WriteGlobalSequenceIndexResponseData()
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("invalid batch")
        ));

        CompletionException exception = assertThrows(CompletionException.class, result::join);
        assertEquals(Errors.INVALID_REQUEST, Errors.forException(exception.getCause()));
    }

    @Test
    void testTimesOutWhileLeaderIsUnavailable() {
        when(metadataCache.getPartitionLeaderEndpoint(
            Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME,
            INDEX_PARTITION,
            LISTENER_NAME
        )).thenReturn(Optional.empty());

        CompletableFuture<GlobalSequenceAppendResult> result = manager.appendIndex(APPEND_REQUEST);
        assertTrue(manager.generateRequestsForTest().isEmpty());
        time.sleep(REQUEST_TIMEOUT_MS);
        assertTrue(manager.generateRequestsForTest().isEmpty());

        CompletionException exception = assertThrows(CompletionException.class, result::join);
        assertInstanceOf(TimeoutException.class, exception.getCause());
    }

    @Test
    void testRetriesSynchronousLocalLeadershipFailure() {
        when(metadataCache.getPartitionLeaderEndpoint(
            Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME,
            INDEX_PARTITION,
            LISTENER_NAME
        )).thenReturn(Optional.of(new Node(LOCAL_BROKER_ID, "localhost", 9092)));
        when(coordinator.appendIndex(APPEND_REQUEST))
            .thenThrow(new NotCoordinatorException("leadership changed"))
            .thenReturn(CompletableFuture.completedFuture(new GlobalSequenceAppendResult(40L, 4, false)));

        CompletableFuture<GlobalSequenceAppendResult> result = manager.appendIndex(APPEND_REQUEST);
        assertTrue(manager.generateRequestsForTest().isEmpty());
        time.sleep(RETRY_BACKOFF_MS);
        assertTrue(manager.generateRequestsForTest().isEmpty());

        assertEquals(new GlobalSequenceAppendResult(40L, 4, false), result.join());
    }

    @Test
    void testCloseCompletesQueuedOperationsExceptionally() throws InterruptedException {
        CompletableFuture<GlobalSequenceAppendResult> append = manager.appendIndex(APPEND_REQUEST);
        CompletableFuture<GlobalSequenceLookupResult> lookup = manager.lookup(LOOKUP_REQUEST);

        manager.close();

        assertClosed(append);
        assertClosed(lookup);
        assertTrue(manager.generateRequestsForTest().isEmpty());
        verify(coordinator, never()).appendIndex(APPEND_REQUEST);
        verify(coordinator, never()).lookupIndex(LOOKUP_REQUEST);
    }

    @Test
    void testRejectsOperationsAfterClose() throws InterruptedException {
        manager.close();

        CompletableFuture<GlobalSequenceAppendResult> append = manager.appendIndex(APPEND_REQUEST);
        CompletableFuture<GlobalSequenceLookupResult> lookup = manager.lookup(LOOKUP_REQUEST);

        assertClosed(append);
        assertClosed(lookup);
        assertTrue(manager.generateRequestsForTest().isEmpty());
        verify(coordinator, never()).appendIndex(APPEND_REQUEST);
        verify(coordinator, never()).lookupIndex(LOOKUP_REQUEST);
    }

    @Test
    void testCloseCompletesOperationWaitingForRetryBackoff() throws InterruptedException {
        when(metadataCache.getPartitionLeaderEndpoint(
            Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME,
            INDEX_PARTITION,
            LISTENER_NAME
        )).thenReturn(Optional.empty());

        CompletableFuture<GlobalSequenceAppendResult> result = manager.appendIndex(APPEND_REQUEST);
        assertTrue(manager.generateRequestsForTest().isEmpty());
        assertFalse(result.isDone());

        // The retry backoff has not elapsed, so this exercises the backoff requeue path.
        assertTrue(manager.generateRequestsForTest().isEmpty());
        assertFalse(result.isDone());

        manager.close();

        assertClosed(result);
        assertTrue(manager.generateRequestsForTest().isEmpty());
        verify(coordinator, never()).appendIndex(APPEND_REQUEST);
    }

    @Test
    void testCloseCompletesInFlightLocalOperation() throws InterruptedException {
        CompletableFuture<GlobalSequenceAppendResult> coordinatorResult = new CompletableFuture<>();
        when(metadataCache.getPartitionLeaderEndpoint(
            Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME,
            INDEX_PARTITION,
            LISTENER_NAME
        )).thenReturn(Optional.of(new Node(LOCAL_BROKER_ID, "localhost", 9092)));
        when(coordinator.appendIndex(APPEND_REQUEST)).thenReturn(coordinatorResult);

        CompletableFuture<GlobalSequenceAppendResult> result = manager.appendIndex(APPEND_REQUEST);
        assertTrue(manager.generateRequestsForTest().isEmpty());
        assertFalse(result.isDone());

        manager.close();

        assertClosed(result);
        coordinatorResult.complete(new GlobalSequenceAppendResult(50L, 4, false));
        assertClosed(result);
    }

    @Test
    void testLateRemoteResponseAfterCloseDoesNotChangeResult() throws InterruptedException {
        Node remoteLeader = new Node(2, "remote", 9093);
        when(metadataCache.getPartitionLeaderEndpoint(
            Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME,
            INDEX_PARTITION,
            LISTENER_NAME
        )).thenReturn(Optional.of(remoteLeader));

        CompletableFuture<GlobalSequenceAppendResult> result = manager.appendIndex(APPEND_REQUEST);
        RequestAndCompletionHandler request = onlyRequest(manager.generateRequestsForTest());

        manager.close();
        assertClosed(result);

        request.handler.onComplete(response(new WriteGlobalSequenceIndexResponseData()
            .setGlobalBaseOffset(60L)
            .setRecordCount(4)));

        assertClosed(result);
        assertTrue(manager.generateRequestsForTest().isEmpty());
    }

    @Test
    void testCloseIsIdempotent() {
        assertDoesNotThrow(manager::close);
        assertDoesNotThrow(manager::close);
    }

    @Test
    void testStartAfterCloseThrows() throws InterruptedException {
        manager.close();

        IllegalStateException exception = assertThrows(IllegalStateException.class, manager::start);
        assertEquals("The global sequence index routing manager is closed.", exception.getMessage());
    }

    @Test
    void testStartIsIdempotent() throws InterruptedException {
        manager.start();
        try {
            assertDoesNotThrow(manager::start);
        } finally {
            manager.close();
        }
    }

    @Test
    void testCannotRestartAfterClose() throws InterruptedException {
        manager.start();
        manager.close();

        IllegalStateException exception = assertThrows(IllegalStateException.class, manager::start);
        assertEquals("The global sequence index routing manager is closed.", exception.getMessage());
    }

    private static RequestAndCompletionHandler onlyRequest(Collection<RequestAndCompletionHandler> requests) {
        assertEquals(1, requests.size());
        return requests.iterator().next();
    }

    private static ClientResponse response(WriteGlobalSequenceIndexResponseData data) {
        ClientResponse response = mock(ClientResponse.class);
        when(response.hasResponse()).thenReturn(true);
        when(response.responseBody()).thenReturn(new WriteGlobalSequenceIndexResponse(data));
        return response;
    }

    private static ClientResponse response(LookupGlobalSequenceIndexResponseData data) {
        ClientResponse response = mock(ClientResponse.class);
        when(response.hasResponse()).thenReturn(true);
        when(response.responseBody()).thenReturn(new LookupGlobalSequenceIndexResponse(data));
        return response;
    }

    private static void assertClosed(CompletableFuture<?> future) {
        CompletionException exception = assertThrows(CompletionException.class, future::join);
        IllegalStateException cause = assertInstanceOf(IllegalStateException.class, exception.getCause());
        assertEquals("The global sequence index routing manager is closed.", cause.getMessage());
    }
}
