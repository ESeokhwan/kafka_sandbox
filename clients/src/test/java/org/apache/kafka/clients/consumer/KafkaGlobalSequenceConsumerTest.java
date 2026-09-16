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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaGlobalSequenceConsumerTest {
    @BeforeEach
    void resetTrackingDeserializer() {
        TrackingDeserializer.CONFIGURES.set(0);
        TrackingDeserializer.KEY_CONFIGURES.set(0);
        TrackingDeserializer.CLOSES.set(0);
    }

    @Test
    void testPropertiesConstructorConfiguresAndOwnsConfiguredDeserializers() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(GlobalSequenceConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, TrackingDeserializer.class);
        properties.put(GlobalSequenceConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TrackingDeserializer.class);
        try (KafkaGlobalSequenceConsumer<byte[], byte[]> ignored =
                 new KafkaGlobalSequenceConsumer<>(properties)) {
            assertEquals(2, TrackingDeserializer.CONFIGURES.get());
            assertEquals(1, TrackingDeserializer.KEY_CONFIGURES.get());
        }
        assertEquals(2, TrackingDeserializer.CLOSES.get());
    }

    @Test
    void testMapConstructorDoesNotConfigureButOwnsSuppliedDeserializers() {
        TrackingDeserializer key = new TrackingDeserializer();
        TrackingDeserializer value = new TrackingDeserializer();
        KafkaGlobalSequenceConsumer<byte[], byte[]> consumer = new KafkaGlobalSequenceConsumer<>(
            Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"), key, value);
        assertEquals(0, TrackingDeserializer.CONFIGURES.get());
        consumer.close();
        consumer.close();
        assertEquals(2, TrackingDeserializer.CLOSES.get());
    }

    @Test
    void testMissingDeserializerClosesTheOneAlreadyConstructed() {
        Map<String, Object> properties = Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            GlobalSequenceConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, TrackingDeserializer.class);
        assertThrows(ConfigException.class, () -> new KafkaGlobalSequenceConsumer<>(properties));
        assertEquals(1, TrackingDeserializer.CLOSES.get());
    }

    @Test
    void testInvalidArgumentsAndZeroTimeoutAreRejectedBeforeFetchIo() {
        try (KafkaGlobalSequenceConsumer<byte[], byte[]> consumer = new KafkaGlobalSequenceConsumer<>(
            Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"),
            new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            assertThrows(IllegalArgumentException.class,
                () -> consumer.fetch("", 0, 1, Duration.ofSeconds(1)));
            assertThrows(IllegalArgumentException.class,
                () -> consumer.fetch("events", -1, 1, Duration.ofSeconds(1)));
            assertThrows(IllegalArgumentException.class,
                () -> consumer.fetch("events", 2, 1, Duration.ofSeconds(1)));
            assertThrows(IllegalArgumentException.class,
                () -> consumer.fetch("events", Uuid.ZERO_UUID, 0, 1, Duration.ofSeconds(1)));
            assertThrows(TimeoutException.class,
                () -> consumer.fetch("events", 0, 1, Duration.ZERO));
        }
    }

    public static final class TrackingDeserializer implements Deserializer<byte[]> {
        static final AtomicInteger CONFIGURES = new AtomicInteger();
        static final AtomicInteger KEY_CONFIGURES = new AtomicInteger();
        static final AtomicInteger CLOSES = new AtomicInteger();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            CONFIGURES.incrementAndGet();
            if (isKey)
                KEY_CONFIGURES.incrementAndGet();
        }

        @Override
        public byte[] deserialize(String topic, byte[] data) {
            return data;
        }

        @Override
        public void close() {
            CLOSES.incrementAndGet();
        }
    }
}
