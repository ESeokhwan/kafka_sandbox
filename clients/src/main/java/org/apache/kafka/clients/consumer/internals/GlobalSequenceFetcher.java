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
import org.apache.kafka.clients.consumer.GlobalSequenceConsumerRecords;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.FetchGlobalSequenceRequestData;
import org.apache.kafka.common.requests.FetchGlobalSequenceRequest;
import org.apache.kafka.common.requests.FetchGlobalSequenceResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Timer;

import java.util.Objects;
import java.util.Optional;

/** Sends one stateless global fetch and retries only attempts with no usable progress. */
public final class GlobalSequenceFetcher<K, V> implements AutoCloseable {
    static final int MAX_WIRE_TIMEOUT_MS = 30_000;

    @FunctionalInterface
    public interface TopicIdResolver {
        Uuid resolve(String topic, Optional<Uuid> expectedTopicId, Timer timer);
    }

    private final ConsumerNetworkClient client;
    private final TopicIdResolver topicIdResolver;
    private final IsolationLevel isolationLevel;
    private final int maxBatches;
    private final int maxBytes;
    private final long retryBackoffMs;
    private final GlobalSequencePageDecoder<K, V> decoder;
    private Timer activeTimer;

    public GlobalSequenceFetcher(
        ConsumerNetworkClient client,
        TopicIdResolver topicIdResolver,
        Deserializer<K> keyDeserializer,
        Deserializer<V> valueDeserializer,
        boolean checkCrcs,
        IsolationLevel isolationLevel,
        int maxBatches,
        int maxBytes,
        long retryBackoffMs
    ) {
        this.client = Objects.requireNonNull(client, "client");
        this.topicIdResolver = Objects.requireNonNull(topicIdResolver, "topicIdResolver");
        this.isolationLevel = Objects.requireNonNull(isolationLevel, "isolationLevel");
        if (retryBackoffMs < 0)
            throw new IllegalArgumentException("retryBackoffMs must be nonnegative");
        this.maxBatches = maxBatches;
        this.maxBytes = maxBytes;
        this.retryBackoffMs = retryBackoffMs;
        this.decoder = new GlobalSequencePageDecoder<>(keyDeserializer, valueDeserializer, checkCrcs,
            isolationLevel, maxBatches, maxBytes, this::checkDecodeDeadline);
    }

    public GlobalSequenceConsumerRecords<K, V> fetch(
        String topic,
        Optional<Uuid> expectedTopicId,
        long globalStartOffset,
        long globalEndOffsetExclusive,
        Timer timer
    ) {
        Objects.requireNonNull(timer, "timer");
        if (!timer.notExpired())
            throw timeout(topic);
        activeTimer = timer;
        try {
            Uuid topicId = topicIdResolver.resolve(topic, expectedTopicId, timer);
            boolean refreshIdentity = false;
            while (timer.notExpired()) {
                if (refreshIdentity)
                    topicIdResolver.resolve(topic, Optional.of(topicId), timer);
                FetchAttempt<K, V> attempt = send(topic, topicId, globalStartOffset,
                    globalEndOffsetExclusive, timer);
                if (attempt.page != null)
                    return attempt.page;
                refreshIdentity = true;
                backoff(timer, attempt.throttleTimeMs);
            }
            throw timeout(topic);
        } finally {
            activeTimer = null;
        }
    }

    private FetchAttempt<K, V> send(
        String topic,
        Uuid topicId,
        long globalStartOffset,
        long globalEndOffsetExclusive,
        Timer timer
    ) {
        Node node = client.leastLoadedNode();
        if (node == null) {
            client.poll(timer);
            return FetchAttempt.retry(0);
        }
        FetchGlobalSequenceRequestData data = new FetchGlobalSequenceRequestData()
            .setTopicId(topicId)
            .setGlobalStartOffset(globalStartOffset)
            .setGlobalEndOffsetExclusive(globalEndOffsetExclusive)
            .setMaxBatches(maxBatches)
            .setMaxBytes(maxBytes)
            .setTimeoutMs(wireTimeoutMs(timer))
            .setIsolationLevel(isolationLevel.id());
        RequestFuture<ClientResponse> future = client.send(node, new FetchGlobalSequenceRequest.Builder(data));
        if (!client.poll(future, timer))
            throw timeout(topic);
        if (future.failed()) {
            RuntimeException failure = future.exception();
            if (retryable(failure))
                return FetchAttempt.retry(0);
            throw failure;
        }
        FetchGlobalSequenceResponse response = (FetchGlobalSequenceResponse) future.value().responseBody();
        return decode(topic, topicId, globalStartOffset, globalEndOffsetExclusive, response);
    }

    private FetchAttempt<K, V> decode(
        String topic,
        Uuid topicId,
        long globalStartOffset,
        long globalEndOffsetExclusive,
        FetchGlobalSequenceResponse response
    ) {
        GlobalSequenceConsumerRecords<K, V> page;
        try {
            page = decoder.decode(topic, topicId, globalStartOffset, globalEndOffsetExclusive, response.data());
        } catch (RuntimeException failure) {
            if (failure instanceof TimeoutException && activeTimer != null && !activeTimer.notExpired())
                throw failure;
            if (retryable(failure))
                return FetchAttempt.retry(response.throttleTimeMs());
            throw failure;
        }
        Optional<ApiException> error = page.error();
        if (error.isEmpty() || page.nextGlobalOffset() > globalStartOffset)
            return FetchAttempt.complete(page);
        if (retryable(error.get()))
            return FetchAttempt.retry(response.throttleTimeMs());
        throw error.get();
    }

    private static boolean retryable(Throwable failure) {
        return failure instanceof RetriableException && !(failure instanceof CorruptRecordException);
    }

    private static int wireTimeoutMs(Timer timer) {
        return (int) Math.max(1, Math.min(MAX_WIRE_TIMEOUT_MS, timer.remainingMs()));
    }

    private void backoff(Timer timer, int throttleTimeMs) {
        long delayMs = Math.max(retryBackoffMs, Math.max(0, throttleTimeMs));
        timer.sleep(Math.min(delayMs, timer.remainingMs()));
    }

    private void checkDecodeDeadline() {
        Timer timer = activeTimer;
        if (timer == null)
            throw new IllegalStateException("No active global fetch timer");
        timer.update();
        if (!timer.notExpired())
            throw new TimeoutException("Global sequence fetch deadline expired while decoding a page");
    }

    private static TimeoutException timeout(String topic) {
        return new TimeoutException("Timeout expired while fetching global sequence topic " + topic);
    }

    @Override
    public void close() {
        decoder.close();
    }

    private static final class FetchAttempt<K, V> {
        private final GlobalSequenceConsumerRecords<K, V> page;
        private final int throttleTimeMs;

        private FetchAttempt(GlobalSequenceConsumerRecords<K, V> page, int throttleTimeMs) {
            this.page = page;
            this.throttleTimeMs = throttleTimeMs;
        }

        private static <K, V> FetchAttempt<K, V> complete(GlobalSequenceConsumerRecords<K, V> page) {
            return new FetchAttempt<>(page, 0);
        }

        private static <K, V> FetchAttempt<K, V> retry(int throttleTimeMs) {
            return new FetchAttempt<>(null, throttleTimeMs);
        }
    }
}
