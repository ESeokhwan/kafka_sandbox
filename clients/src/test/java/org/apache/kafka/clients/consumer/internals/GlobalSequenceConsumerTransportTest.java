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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.GlobalSequenceConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GlobalSequenceConsumerTransportTest {
    @BeforeEach
    void resetReporter() {
        TrackingReporter.CLOSES.set(0);
    }

    @Test
    void testCreatesOnlyCommonTransportResourcesAndClosesThem() {
        var config = new GlobalSequenceConsumerConfig(Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            CommonClientConfigs.CLIENT_ID_CONFIG, "global-client"));
        GlobalSequenceConsumerTransport transport =
            GlobalSequenceConsumerTransport.create(config, new MockTime());
        assertNotNull(transport.client());
        assertNotNull(transport.apiVersions());
        assertNotNull(transport.metrics());
        transport.close();
        transport.close();
    }

    @Test
    void testSecurityChannelConfigurationIsForwarded() {
        var config = new GlobalSequenceConsumerConfig(Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL",
            SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""));
        try (GlobalSequenceConsumerTransport transport =
                 GlobalSequenceConsumerTransport.create(config, new MockTime())) {
            assertNotNull(transport.client());
        }
    }

    @Test
    void testConstructionFailureClosesAlreadyCreatedMetrics() {
        Map<String, Object> properties = Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "missing-port",
            CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, TrackingReporter.class.getName());
        var config = new GlobalSequenceConsumerConfig(properties);
        assertThrows(ConfigException.class,
            () -> GlobalSequenceConsumerTransport.create(config, new MockTime()));
        assertEquals(1, TrackingReporter.CLOSES.get());
    }

    public static final class TrackingReporter implements MetricsReporter {
        static final AtomicInteger CLOSES = new AtomicInteger();

        @Override
        public void init(List<KafkaMetric> metrics) { }

        @Override
        public void metricChange(KafkaMetric metric) { }

        @Override
        public void metricRemoval(KafkaMetric metric) { }

        @Override
        public void close() {
            CLOSES.incrementAndGet();
        }

        @Override
        public void configure(Map<String, ?> configs) { }
    }
}
