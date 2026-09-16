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
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalSequenceConsumerConfigTest {
    private static Map<String, Object> base() {
        return new HashMap<>(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
    }

    @Test
    void testDefaultsMatchGlobalWireLimitsAndDoNotRequireDeserializerClasses() {
        var config = new GlobalSequenceConsumerConfig(base());
        assertEquals(1024 * 1024, config.getInt(GlobalSequenceConsumerConfig.FETCH_MAX_BYTES_CONFIG));
        assertEquals(100, config.getInt(GlobalSequenceConsumerConfig.FETCH_MAX_BATCHES_CONFIG));
        assertTrue(config.getBoolean(GlobalSequenceConsumerConfig.CHECK_CRCS_CONFIG));
        assertEquals(IsolationLevel.READ_UNCOMMITTED, config.isolationLevel());
        assertNull(config.keyDeserializer());
        assertNull(config.valueDeserializer());
    }

    @Test
    void testPropertiesAndConfiguredDeserializers() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(GlobalSequenceConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(GlobalSequenceConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(GlobalSequenceConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        var config = new GlobalSequenceConsumerConfig(properties);
        Deserializer<?> keyDeserializer = config.keyDeserializer();
        Deserializer<?> valueDeserializer = config.valueDeserializer();
        assertTrue(keyDeserializer instanceof StringDeserializer);
        assertTrue(valueDeserializer instanceof StringDeserializer);
        assertEquals(IsolationLevel.READ_COMMITTED, config.isolationLevel());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        ConsumerConfig.GROUP_ID_CONFIG,
        ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
        ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,
        ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG
    })
    void testGroupPositionAndAutoCreationConfigsAreRejected(String name) {
        Map<String, Object> properties = base();
        properties.put(name, name.equals(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) ||
            name.equals(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG) ? true : "configured");
        ConfigException exception = assertThrows(ConfigException.class,
            () -> new GlobalSequenceConsumerConfig(properties));
        assertTrue(exception.getMessage().contains(name));
    }

    @Test
    void testPageBoundsAreValidated() {
        for (int bytes : new int[] {0, GlobalSequenceConsumerConfig.MAX_FETCH_MAX_BYTES + 1}) {
            Map<String, Object> properties = base();
            properties.put(GlobalSequenceConsumerConfig.FETCH_MAX_BYTES_CONFIG, bytes);
            assertThrows(ConfigException.class, () -> new GlobalSequenceConsumerConfig(properties));
        }
        for (int batches : new int[] {0, 1001}) {
            Map<String, Object> properties = base();
            properties.put(GlobalSequenceConsumerConfig.FETCH_MAX_BATCHES_CONFIG, batches);
            assertThrows(ConfigException.class, () -> new GlobalSequenceConsumerConfig(properties));
        }
    }

    @Test
    void testSslSaslAndNetworkSettingsRemainAvailable() {
        assertTrue(GlobalSequenceConsumerConfig.configNames().contains("ssl.truststore.location"));
        assertTrue(GlobalSequenceConsumerConfig.configNames().contains("sasl.mechanism"));
        assertTrue(GlobalSequenceConsumerConfig.configNames().contains(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG));
        assertTrue(GlobalSequenceConsumerConfig.configNames().contains(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG));
        assertTrue(GlobalSequenceConsumerConfig.configNames().contains(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG));
    }
}
