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

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * Configuration for a {@link GlobalSequenceConsumer} implementation.
 *
 * <p>This client reuses Kafka's common connection, security, metrics and
 * deserializer settings. It does not join a group, maintain a position, commit
 * offsets, auto-create topics, or run the ordinary partition fetcher. Supplying
 * group, assignment or offset-reset settings is rejected rather than ignored.</p>
 */
public class GlobalSequenceConsumerConfig extends AbstractConfig {
    /** The soft encoded batch byte limit for one global fetch page. */
    public static final String FETCH_MAX_BYTES_CONFIG = ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
    public static final int DEFAULT_FETCH_MAX_BYTES = 1024 * 1024;
    public static final int MAX_FETCH_MAX_BYTES = 16 * 1024 * 1024 - 128 * 1024;

    /** The maximum number of complete physical batch mappings in one page. */
    public static final String FETCH_MAX_BATCHES_CONFIG = "global.sequence.fetch.max.batches";
    public static final int DEFAULT_FETCH_MAX_BATCHES = 100;

    /** Whether CRCs are checked while decoding returned record batches. */
    public static final String CHECK_CRCS_CONFIG = ConsumerConfig.CHECK_CRCS_CONFIG;
    public static final boolean DEFAULT_CHECK_CRCS = true;

    /** Transaction isolation for global fetches. */
    public static final String ISOLATION_LEVEL_CONFIG = ConsumerConfig.ISOLATION_LEVEL_CONFIG;
    public static final String DEFAULT_ISOLATION_LEVEL = IsolationLevel.READ_UNCOMMITTED.toString();

    /** Key deserializer class; optional when a deserializer instance is supplied to the consumer. */
    public static final String KEY_DESERIALIZER_CLASS_CONFIG = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;

    /** Value deserializer class; optional when a deserializer instance is supplied to the consumer. */
    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

    private static final Set<String> UNSUPPORTED_CONFIGS = Set.of(
        "bootstrap.controllers",
        ConsumerConfig.GROUP_ID_CONFIG,
        ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
        ConsumerConfig.GROUP_PROTOCOL_CONFIG,
        ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG,
        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
        ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
        ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,
        ConsumerConfig.CLIENT_RACK_CONFIG,
        ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
        ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG
    );

    private static final ConfigDef CONFIG = commonConfigDef()
        .define(FETCH_MAX_BYTES_CONFIG, INT, DEFAULT_FETCH_MAX_BYTES,
            between(1, MAX_FETCH_MAX_BYTES), MEDIUM,
            "Soft limit for complete encoded record batches in one global fetch page. " +
                "The first complete batch may exceed this limit.")
        .define(FETCH_MAX_BATCHES_CONFIG, INT, DEFAULT_FETCH_MAX_BATCHES,
            between(1, 1000), MEDIUM,
            "Maximum number of complete physical batch mappings in one global fetch page.")
        .define(CHECK_CRCS_CONFIG, BOOLEAN, DEFAULT_CHECK_CRCS, LOW,
            "Whether to check CRCs while decoding global sequence record batches.")
        .define(ISOLATION_LEVEL_CONFIG, STRING, DEFAULT_ISOLATION_LEVEL,
            in(IsolationLevel.READ_UNCOMMITTED.toString(), IsolationLevel.READ_COMMITTED.toString()), MEDIUM,
            "Controls whether aborted records are returned. read_committed requires global fetch version 1.")
        .define(KEY_DESERIALIZER_CLASS_CONFIG, CLASS, null, HIGH,
            "Deserializer class for record keys. May be omitted when an instance is supplied directly.")
        .define(VALUE_DESERIALIZER_CLASS_CONFIG, CLASS, null, HIGH,
            "Deserializer class for record values. May be omitted when an instance is supplied directly.");

    private static ConfigDef commonConfigDef() {
        return new ConfigDef()
            .define(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, LIST, Collections.emptyList(),
                new ConfigDef.NonNullValidator(), HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
            .define(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG, STRING,
                ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                    ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                MEDIUM, CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
            .define(CommonClientConfigs.CLIENT_ID_CONFIG, STRING, "", LOW,
                CommonClientConfigs.CLIENT_ID_DOC)
            .define(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, LONG, 5 * 60 * 1000L,
                atLeast(0), LOW, CommonClientConfigs.METADATA_MAX_AGE_DOC)
            .define(CommonClientConfigs.SEND_BUFFER_CONFIG, INT, 128 * 1024,
                atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND), MEDIUM,
                CommonClientConfigs.SEND_BUFFER_DOC)
            .define(CommonClientConfigs.RECEIVE_BUFFER_CONFIG, INT, 64 * 1024,
                atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND), MEDIUM,
                CommonClientConfigs.RECEIVE_BUFFER_DOC)
            .define(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, LONG, 50L,
                atLeast(0L), LOW, CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
            .define(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, LONG, 1000L,
                atLeast(0L), LOW, CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
            .define(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, LONG,
                CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MS, atLeast(0L), LOW,
                CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
            .define(CommonClientConfigs.RETRY_BACKOFF_MAX_MS_CONFIG, LONG,
                CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MAX_MS, atLeast(0L), LOW,
                CommonClientConfigs.RETRY_BACKOFF_MAX_MS_DOC)
            .define(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, INT, 30000,
                atLeast(0), MEDIUM, CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
            .define(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, LONG,
                CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS, MEDIUM,
                CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
            .define(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, LONG,
                CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS, MEDIUM,
                CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
            .define(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, LONG, 9 * 60 * 1000L,
                MEDIUM, CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
            .define(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG, LONG, 30000L,
                atLeast(0), LOW, CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
            .define(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG, INT, 2,
                atLeast(1), LOW, CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
            .define(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, LIST,
                JmxReporter.class.getName(), new ConfigDef.NonNullValidator(), LOW,
                CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
            .define(CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG, STRING,
                Sensor.RecordingLevel.INFO.toString(),
                in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString(),
                    Sensor.RecordingLevel.TRACE.toString()), LOW,
                CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
            .define(SecurityConfig.SECURITY_PROVIDERS_CONFIG, STRING, null, LOW,
                SecurityConfig.SECURITY_PROVIDERS_DOC)
            .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, STRING,
                CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(SecurityProtocol.class)),
                MEDIUM, CommonClientConfigs.SECURITY_PROTOCOL_DOC)
            .withClientSslSupport()
            .withClientSaslSupport()
            .define(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, STRING,
                CommonClientConfigs.DEFAULT_METADATA_RECOVERY_STRATEGY,
                ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(MetadataRecoveryStrategy.class)),
                LOW, "Controls whether the client re-bootstraps when every known broker is unavailable.")
            .define(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG, LONG,
                CommonClientConfigs.DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS,
                atLeast(0), LOW, CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC);
    }

    public GlobalSequenceConsumerConfig(Properties props) {
        this((Map<?, ?>) props, true);
    }

    public GlobalSequenceConsumerConfig(Map<?, ?> props) {
        this(props, true);
    }

    GlobalSequenceConsumerConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
        List<String> unsupported = new ArrayList<>();
        for (String name : UNSUPPORTED_CONFIGS) {
            if (originals().containsKey(name))
                unsupported.add(name);
        }
        if (!unsupported.isEmpty()) {
            unsupported.sort(String::compareTo);
            throw new ConfigException(String.join(", ", unsupported) +
                " cannot be set for a global sequence consumer");
        }
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(Map<String, Object> parsedValues) {
        CommonClientConfigs.postValidateSaslMechanismConfig(this);
        CommonClientConfigs.warnDisablingExponentialBackoff(this);
        return CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
    }

    /** Return the configured isolation level. */
    public IsolationLevel isolationLevel() {
        return IsolationLevel.valueOf(getString(ISOLATION_LEVEL_CONFIG).toUpperCase(Locale.ROOT));
    }

    /** Return a configured key deserializer, or null when a direct instance is required. */
    @SuppressWarnings("unchecked")
    public <K> Deserializer<K> keyDeserializer() {
        return (Deserializer<K>) getConfiguredInstance(KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
    }

    /** Return a configured value deserializer, or null when a direct instance is required. */
    @SuppressWarnings("unchecked")
    public <V> Deserializer<V> valueDeserializer() {
        return (Deserializer<V>) getConfiguredInstance(VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }
}
