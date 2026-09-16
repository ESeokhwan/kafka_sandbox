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
import org.apache.kafka.clients.consumer.GlobalSequenceTopicIdMismatchException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalSequenceConsumerMetadataTest {
    private static final String TOPIC = "events";
    private static final Uuid TOPIC_ID = new Uuid(10, 20);
    private final MockTime time = new MockTime();
    private final Metadata metadata = new Metadata(10, 100, 60000,
        new LogContext(), new ClusterResourceListeners());
    private final MockClient mockClient = new MockClient(time, metadata);
    private final ConsumerNetworkClient networkClient = new ConsumerNetworkClient(
        new LogContext(), mockClient, metadata, time, 10, 1000, Integer.MAX_VALUE);
    private final List<List<Node>> nodeUpdates = new ArrayList<>();
    private GlobalSequenceConsumerMetadata resolver;

    @BeforeEach
    void setUp() {
        MetadataResponse initial = response(Errors.NONE, TOPIC_ID, 2);
        mockClient.updateMetadata(initial);
        resolver = new GlobalSequenceConsumerMetadata(networkClient, nodeUpdates::add, 10);
    }

    @Test
    void testSingleTopicRequestDisablesAutoCreationAndReturnsUuid() {
        mockClient.prepareResponse(request -> {
            MetadataRequest metadataRequest = (MetadataRequest) request;
            assertEquals(List.of(TOPIC), metadataRequest.topics());
            assertFalse(metadataRequest.data().allowAutoTopicCreation());
            return true;
        }, response(Errors.NONE, TOPIC_ID, 3));
        assertEquals(TOPIC_ID, resolver.resolveTopicId(TOPIC, Optional.empty(), time.timer(1000)));
        assertEquals(1, nodeUpdates.size());
        assertEquals(3, nodeUpdates.get(0).size());
    }

    @Test
    void testDisconnectRetriesSameTopicAndExpectedUuid() {
        mockClient.prepareResponse(response(Errors.NONE, TOPIC_ID, 2), true);
        mockClient.prepareResponse(response(Errors.NONE, TOPIC_ID, 2));
        assertEquals(TOPIC_ID,
            resolver.resolveTopicId(TOPIC, Optional.of(TOPIC_ID), time.timer(1000)));
        assertTrue(time.milliseconds() >= 10);
    }

    @Test
    void testExpectedUuidPreventsNameReuse() {
        Uuid replacement = new Uuid(30, 40);
        mockClient.prepareResponse(response(Errors.NONE, replacement, 2));
        GlobalSequenceTopicIdMismatchException exception = assertThrows(
            GlobalSequenceTopicIdMismatchException.class,
            () -> resolver.resolveTopicId(TOPIC, Optional.of(TOPIC_ID), time.timer(1000)));
        assertEquals(TOPIC_ID, exception.expectedTopicId());
        assertEquals(replacement, exception.actualTopicId());
    }

    @Test
    void testMissingUnauthorizedAndInvalidTopicStayDistinct() {
        mockClient.prepareResponse(response(Errors.UNKNOWN_TOPIC_OR_PARTITION, Uuid.ZERO_UUID, 2));
        assertThrows(UnknownTopicOrPartitionException.class,
            () -> resolver.resolveTopicId(TOPIC, Optional.empty(), time.timer(1000)));

        mockClient.prepareResponse(response(Errors.TOPIC_AUTHORIZATION_FAILED, Uuid.ZERO_UUID, 2));
        TopicAuthorizationException authorizationException = assertThrows(TopicAuthorizationException.class,
            () -> resolver.resolveTopicId(TOPIC, Optional.empty(), time.timer(1000)));
        assertEquals(Set.of(TOPIC), authorizationException.unauthorizedTopics());

        mockClient.prepareResponse(response(Errors.INVALID_TOPIC_EXCEPTION, Uuid.ZERO_UUID, 2));
        assertThrows(InvalidTopicException.class,
            () -> resolver.resolveTopicId(TOPIC, Optional.empty(), time.timer(1000)));
    }

    @Test
    void testSuccessfulMetadataWithoutUuidCannotStartGlobalFetch() {
        mockClient.prepareResponse(response(Errors.NONE, Uuid.ZERO_UUID, 2));
        assertThrows(UnsupportedVersionException.class,
            () -> resolver.resolveTopicId(TOPIC, Optional.empty(), time.timer(1000)));
    }

    @Test
    void testResponseMissingRequestedTopicIsNotAccepted() {
        mockClient.prepareResponse(RequestTestUtils.metadataUpdateWithIds(2,
            Collections.singletonMap("other", 1), Collections.singletonMap("other", TOPIC_ID)));
        assertThrows(UnknownTopicOrPartitionException.class,
            () -> resolver.resolveTopicId(TOPIC, Optional.empty(), time.timer(1000)));
    }

    @Test
    void testRequestValidationPrecedesNetworkWork() {
        assertThrows(IllegalArgumentException.class,
            () -> resolver.resolveTopicId("", Optional.empty(), time.timer(1000)));
        assertThrows(IllegalArgumentException.class,
            () -> resolver.resolveTopicId(TOPIC, Optional.of(Uuid.ZERO_UUID), time.timer(1000)));
        assertFalse(mockClient.hasInFlightRequests());
        assertEquals(0, mockClient.numAwaitingResponses());
    }

    private static MetadataResponse response(Errors error, Uuid id, int nodes) {
        return RequestTestUtils.metadataUpdateWith("cluster", nodes,
            error == Errors.NONE ? Collections.emptyMap() : Map.of(TOPIC, error),
            error == Errors.NONE ? Map.of(TOPIC, 1) : Collections.emptyMap(),
            partition -> 1,
            MetadataResponse.PartitionMetadata::new,
            org.apache.kafka.common.protocol.ApiKeys.METADATA.latestVersion(),
            Map.of(TOPIC, id));
    }
}
