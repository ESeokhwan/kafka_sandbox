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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.GlobalSequenceTopicIdMismatchException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/** Resolves a topic name to one current UUID without retaining an unbounded topic cache. */
public final class GlobalSequenceConsumerMetadata {
    private final ConsumerNetworkClient client;
    private final Consumer<List<Node>> nodeUpdater;
    private final long retryBackoffMs;
    private final Time time;
    private final GlobalSequenceRequestScope requestScope;

    public GlobalSequenceConsumerMetadata(
        ConsumerNetworkClient client,
        Consumer<List<Node>> nodeUpdater,
        long retryBackoffMs
    ) {
        this(client, nodeUpdater, retryBackoffMs, Time.SYSTEM, new GlobalSequenceRequestScope());
    }

    GlobalSequenceConsumerMetadata(
        ConsumerNetworkClient client,
        Consumer<List<Node>> nodeUpdater,
        long retryBackoffMs,
        Time time,
        GlobalSequenceRequestScope requestScope
    ) {
        this.client = Objects.requireNonNull(client, "client");
        this.nodeUpdater = Objects.requireNonNull(nodeUpdater, "nodeUpdater");
        if (retryBackoffMs < 0)
            throw new IllegalArgumentException("retryBackoffMs must be nonnegative");
        this.retryBackoffMs = retryBackoffMs;
        this.time = Objects.requireNonNull(time, "time");
        this.requestScope = Objects.requireNonNull(requestScope, "requestScope");
    }

    /**
     * Resolve a single topic with auto-creation disabled. Retriable transport and
     * metadata errors use the same timer; missing, unauthorized and invalid topics
     * retain their specific exceptions.
     */
    public Uuid resolveTopicId(String topic, Optional<Uuid> expectedTopicId, Timer timer) {
        validateRequest(topic, expectedTopicId, timer);
        try {
            while (timer.notExpired()) {
                Optional<MetadataResponse> response = request(topic, timer);
                if (response.isEmpty())
                    continue;
                Uuid actual = topicId(topic, response.get(), timer);
                if (actual == null)
                    continue;
                if (expectedTopicId.isPresent() && !expectedTopicId.get().equals(actual))
                    throw new GlobalSequenceTopicIdMismatchException(topic, expectedTopicId.get(), actual);
                return actual;
            }
            throw new TimeoutException("Timeout expired while resolving global sequence topic " + topic);
        } catch (Throwable failure) {
            try {
                requestScope.abort(client);
            } catch (Throwable cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private static void validateRequest(String topic, Optional<Uuid> expectedTopicId, Timer timer) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(expectedTopicId, "expectedTopicId");
        Objects.requireNonNull(timer, "timer");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic must not be empty");
        expectedTopicId.ifPresent(id -> {
            if (Uuid.ZERO_UUID.equals(id))
                throw new IllegalArgumentException("expectedTopicId must not be ZERO_UUID");
        });
    }

    private Optional<MetadataResponse> request(String topic, Timer timer) {
        Node node = client.leastLoadedNode();
        if (node == null) {
            client.poll(timer);
            return Optional.empty();
        }
        requestScope.requestStarted(node);
        RequestFuture<ClientResponse> future = client.send(node,
            new MetadataRequest.Builder(List.of(topic), false));
        try {
            if (!client.poll(future, timer))
                return Optional.empty();
        } finally {
            if (future.isDone())
                requestScope.requestCompleted(node);
        }
        if (future.failed()) {
            if (!future.isRetriable())
                throw future.exception();
            backoff(timer);
            return Optional.empty();
        }
        MetadataResponse response = (MetadataResponse) future.value().responseBody();
        updateNodes(response.brokers());
        return Optional.of(response);
    }

    private Uuid topicId(String topic, MetadataResponse response, Timer timer) {
        MetadataResponse.TopicMetadata metadata = findTopic(response, topic);
        Errors error = metadata.error();
        if (error != Errors.NONE) {
            ApiException exception = error.exception("Unable to resolve global sequence topic " + topic);
            if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION)
                throw new UnknownTopicOrPartitionException(exception.getMessage());
            if (error == Errors.TOPIC_AUTHORIZATION_FAILED)
                throw new TopicAuthorizationException(Set.of(topic));
            if (!(exception instanceof RetriableException))
                throw exception;
            backoff(timer);
            return null;
        }
        Uuid actual = metadata.topicId();
        if (actual == null || Uuid.ZERO_UUID.equals(actual))
            throw new UnsupportedVersionException(
                "Metadata for global sequence topic " + topic + " did not contain a topic UUID");
        return actual;
    }

    private void updateNodes(Collection<Node> nodes) {
        if (!nodes.isEmpty())
            nodeUpdater.accept(new ArrayList<>(nodes));
    }

    private static MetadataResponse.TopicMetadata findTopic(MetadataResponse response, String topic) {
        for (MetadataResponse.TopicMetadata metadata : response.topicMetadata()) {
            if (topic.equals(metadata.topic()))
                return metadata;
        }
        throw new UnknownTopicOrPartitionException(
            "Metadata response did not contain global sequence topic " + topic);
    }

    private void backoff(Timer timer) {
        long delayMs = Math.min(retryBackoffMs, timer.remainingMs());
        Timer delayTimer = time.timer(delayMs);
        while (delayTimer.notExpired() && timer.notExpired()) {
            long beforePollMs = delayTimer.currentTimeMs();
            client.poll(delayTimer, () -> true);
            delayTimer.update();
            timer.update();
            if (delayTimer.currentTimeMs() == beforePollMs && delayTimer.notExpired()) {
                time.sleep(Math.min(1, Math.min(delayTimer.remainingMs(), timer.remainingMs())));
                delayTimer.update();
                timer.update();
            }
        }
    }
}
