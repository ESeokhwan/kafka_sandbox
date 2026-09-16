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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.internals.GlobalSequenceConsumerTransport;
import org.apache.kafka.clients.consumer.internals.GlobalSequenceFetcher;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * The standard stateless {@link GlobalSequenceConsumer} implementation.
 *
 * <p>Each fetch resolves the topic UUID and returns one server page. This
 * implementation creates no group coordinator, subscription, position or
 * ordinary partition fetcher.</p>
 */
public class KafkaGlobalSequenceConsumer<K, V> implements GlobalSequenceConsumer<K, V> {
    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30);

    private final Time time;
    private final GlobalSequenceConsumerTransport transport;
    private final GlobalSequenceFetcher<K, V> fetcher;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private volatile boolean closed;

    /** Create a consumer using deserializer classes from the supplied properties. */
    public KafkaGlobalSequenceConsumer(Properties properties) {
        this(new GlobalSequenceConsumerConfig(properties), null, null, Time.SYSTEM);
    }

    /** Create a consumer using the supplied deserializer instances. */
    public KafkaGlobalSequenceConsumer(
        Properties properties,
        Deserializer<K> keyDeserializer,
        Deserializer<V> valueDeserializer
    ) {
        this(new GlobalSequenceConsumerConfig(properties), keyDeserializer, valueDeserializer, Time.SYSTEM);
    }

    /** Create a consumer using deserializer classes from the supplied configuration. */
    public KafkaGlobalSequenceConsumer(Map<String, Object> configs) {
        this(new GlobalSequenceConsumerConfig(configs), null, null, Time.SYSTEM);
    }

    /** Create a consumer using the supplied configuration and deserializer instances. */
    public KafkaGlobalSequenceConsumer(
        Map<String, Object> configs,
        Deserializer<K> keyDeserializer,
        Deserializer<V> valueDeserializer
    ) {
        this(new GlobalSequenceConsumerConfig(configs), keyDeserializer, valueDeserializer, Time.SYSTEM);
    }

    KafkaGlobalSequenceConsumer(
        GlobalSequenceConsumerConfig config,
        Deserializer<K> suppliedKeyDeserializer,
        Deserializer<V> suppliedValueDeserializer,
        Time time
    ) {
        this.time = Objects.requireNonNull(time, "time");
        Deserializer<K> key = null;
        Deserializer<V> value = null;
        GlobalSequenceConsumerTransport createdTransport = null;
        GlobalSequenceFetcher<K, V> createdFetcher = null;
        try {
            key = deserializer(config, suppliedKeyDeserializer,
                GlobalSequenceConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, true);
            value = deserializer(config, suppliedValueDeserializer,
                GlobalSequenceConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, false);
            createdTransport = GlobalSequenceConsumerTransport.create(config, time);
            createdFetcher = new GlobalSequenceFetcher<>(createdTransport.client(), createdTransport::resolveTopicId,
                key, value, config.getBoolean(GlobalSequenceConsumerConfig.CHECK_CRCS_CONFIG),
                config.isolationLevel(), config.getInt(GlobalSequenceConsumerConfig.FETCH_MAX_BATCHES_CONFIG),
                config.getInt(GlobalSequenceConsumerConfig.FETCH_MAX_BYTES_CONFIG),
                config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG));
        } catch (Throwable failure) {
            closeAfterConstructionFailure(createdFetcher, createdTransport, key, value, failure);
            throw failure;
        }
        this.keyDeserializer = key;
        this.valueDeserializer = value;
        this.transport = createdTransport;
        this.fetcher = createdFetcher;
    }

    @SuppressWarnings("unchecked")
    private static <T> Deserializer<T> deserializer(
        GlobalSequenceConsumerConfig config,
        Deserializer<T> supplied,
        String configName,
        boolean isKey
    ) {
        if (supplied != null) {
            config.ignore(configName);
            return supplied;
        }
        Deserializer<T> configured = configName.equals(GlobalSequenceConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) ?
            (Deserializer<T>) config.keyDeserializer() : (Deserializer<T>) config.valueDeserializer();
        if (configured == null)
            throw new ConfigException(configName, null,
                "must be configured when no deserializer instance is supplied");
        String clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        configured.configure(config.originals(Collections.singletonMap(CommonClientConfigs.CLIENT_ID_CONFIG, clientId)),
            isKey);
        return configured;
    }

    private static void closeAfterConstructionFailure(
        AutoCloseable fetcher,
        AutoCloseable transport,
        AutoCloseable keyDeserializer,
        AutoCloseable valueDeserializer,
        Throwable failure
    ) {
        closeSuppressed(fetcher, failure);
        closeSuppressed(transport, failure);
        closeSuppressed(keyDeserializer, failure);
        closeSuppressed(valueDeserializer, failure);
    }

    private static void closeSuppressed(AutoCloseable resource, Throwable failure) {
        if (resource == null)
            return;
        try {
            resource.close();
        } catch (Throwable closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    @Override
    public GlobalSequenceConsumerRecords<K, V> fetch(
        String topic,
        long globalStartOffset,
        long globalEndOffsetExclusive,
        Duration timeout
    ) {
        return fetchInternal(topic, Optional.empty(), globalStartOffset, globalEndOffsetExclusive, timeout);
    }

    @Override
    public GlobalSequenceConsumerRecords<K, V> fetch(
        String topic,
        Uuid expectedTopicId,
        long globalStartOffset,
        long globalEndOffsetExclusive,
        Duration timeout
    ) {
        Objects.requireNonNull(expectedTopicId, "expectedTopicId");
        if (Uuid.ZERO_UUID.equals(expectedTopicId))
            throw new IllegalArgumentException("expectedTopicId must not be ZERO_UUID");
        return fetchInternal(topic, Optional.of(expectedTopicId), globalStartOffset,
            globalEndOffsetExclusive, timeout);
    }

    private GlobalSequenceConsumerRecords<K, V> fetchInternal(
        String topic,
        Optional<Uuid> expectedTopicId,
        long globalStartOffset,
        long globalEndOffsetExclusive,
        Duration timeout
    ) {
        validateFetch(topic, globalStartOffset, globalEndOffsetExclusive, timeout);
        if (closed)
            throw new IllegalStateException("This global sequence consumer is closed");
        return fetcher.fetch(topic, expectedTopicId, globalStartOffset, globalEndOffsetExclusive,
            time.timer(timeout.toMillis()));
    }

    private static void validateFetch(
        String topic,
        long globalStartOffset,
        long globalEndOffsetExclusive,
        Duration timeout
    ) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(timeout, "timeout");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic must not be empty");
        if (globalStartOffset < 0 || globalEndOffsetExclusive < globalStartOffset)
            throw new IllegalArgumentException("Invalid global offset range");
        if (timeout.isNegative())
            throw new IllegalArgumentException("timeout must not be negative");
    }

    @Override
    public void wakeup() {
        transport.client().wakeup();
    }

    @Override
    public void close() {
        close(DEFAULT_CLOSE_TIMEOUT);
    }

    @Override
    public void close(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative())
            throw new IllegalArgumentException("timeout must not be negative");
        if (closed)
            return;
        closed = true;
        Throwable failure = null;
        failure = close(fetcher, failure);
        failure = close(keyDeserializer, failure);
        failure = close(valueDeserializer, failure);
        failure = close(transport, failure);
        if (failure != null)
            throw new KafkaException("Failed to close global sequence consumer", failure);
    }

    private static Throwable close(AutoCloseable resource, Throwable firstFailure) {
        try {
            resource.close();
        } catch (Throwable failure) {
            if (firstFailure == null)
                return failure;
            firstFailure.addSuppressed(failure);
        }
        return firstFailure;
    }
}
