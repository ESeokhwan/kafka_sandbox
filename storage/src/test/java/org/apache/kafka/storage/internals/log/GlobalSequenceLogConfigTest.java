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

package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobalSequenceLogConfigTest {
    @Test
    void testActivationDefaultsAndParsing() {
        assertFalse(new LogConfig(Map.of()).globalSequenceEnabled());
        assertTrue(new LogConfig(Map.of(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, true)).globalSequenceEnabled());
        assertThrows(ConfigException.class, () ->
            new LogConfig(Map.of(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "invalid")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"compact", "delete,compact", ""})
    void testRejectNonDeleteCleanup(String cleanupPolicy) {
        Properties props = new Properties();
        props.setProperty(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "true");
        props.setProperty(TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy);
        assertThrows(InvalidConfigurationException.class, () -> LogConfig.validate(props));
    }

    @Test
    void testInheritedCleanupPolicy() {
        Properties props = new Properties();
        props.setProperty(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "true");
        Map<String, String> brokerDefaults = Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, "compact");
        assertThrows(InvalidConfigurationException.class, () ->
            LogConfig.validate(Map.of(), props, brokerDefaults, false));
        props.setProperty(TopicConfig.CLEANUP_POLICY_CONFIG, "delete");
        LogConfig.validate(Map.of(), props, brokerDefaults, false);
    }

    @Test
    void testOrdinaryTopicsStillSupportCompaction() {
        Properties props = new Properties();
        props.setProperty(TopicConfig.CLEANUP_POLICY_CONFIG, "compact");
        LogConfig.validate(props);
        props.setProperty(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "false");
        LogConfig.validate(props);
    }
}
