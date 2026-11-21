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
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/** Broker configuration for the built-in monitor logging interceptors. */
public final class MonitorLoggingConfigs {
    public static final String MONITOR_LOGGING_PREFIX = "monitor.logging.";
    public static final String JSON_BASED_MONITOR_LOGGING_PREFIX = "json.based.monitor.logging.";

    public static final String OUTPUT_PATH_SUFFIX = "output.path";
    public static final String ROLL_OUT_INTERVAL_MS_SUFFIX = "rollout.interval.ms";
    public static final String MAX_LOGS_PER_FILE_SUFFIX = "max.logs.per.file";
    public static final String FORMAT_SUFFIX = "format";
    public static final String BATCH_SIZE_SUFFIX = "batch.size";
    public static final String FLUSH_INTERVAL_MS_SUFFIX = "flush.interval.ms";
    public static final String PREPROCESSING_WORKER_COUNT_SUFFIX = "preprocessing.worker.count";

    public static final String MONITOR_LOGGING_OUTPUT_PATH_CONFIG = MONITOR_LOGGING_PREFIX + OUTPUT_PATH_SUFFIX;
    public static final String JSON_BASED_MONITOR_LOGGING_OUTPUT_PATH_CONFIG = JSON_BASED_MONITOR_LOGGING_PREFIX + OUTPUT_PATH_SUFFIX;
    public static final String MONITOR_LOGGING_ROLL_OUT_INTERVAL_MS_CONFIG = MONITOR_LOGGING_PREFIX + ROLL_OUT_INTERVAL_MS_SUFFIX;
    public static final String JSON_BASED_MONITOR_LOGGING_ROLL_OUT_INTERVAL_MS_CONFIG = JSON_BASED_MONITOR_LOGGING_PREFIX + ROLL_OUT_INTERVAL_MS_SUFFIX;
    public static final String MONITOR_LOGGING_MAX_LOGS_PER_FILE_CONFIG = MONITOR_LOGGING_PREFIX + MAX_LOGS_PER_FILE_SUFFIX;
    public static final String JSON_BASED_MONITOR_LOGGING_MAX_LOGS_PER_FILE_CONFIG = JSON_BASED_MONITOR_LOGGING_PREFIX + MAX_LOGS_PER_FILE_SUFFIX;
    public static final String MONITOR_LOGGING_FORMAT_CONFIG = MONITOR_LOGGING_PREFIX + FORMAT_SUFFIX;
    public static final String JSON_BASED_MONITOR_LOGGING_FORMAT_CONFIG = JSON_BASED_MONITOR_LOGGING_PREFIX + FORMAT_SUFFIX;
    public static final String MONITOR_LOGGING_BATCH_SIZE_CONFIG = MONITOR_LOGGING_PREFIX + BATCH_SIZE_SUFFIX;
    public static final String JSON_BASED_MONITOR_LOGGING_BATCH_SIZE_CONFIG = JSON_BASED_MONITOR_LOGGING_PREFIX + BATCH_SIZE_SUFFIX;
    public static final String MONITOR_LOGGING_FLUSH_INTERVAL_MS_CONFIG = MONITOR_LOGGING_PREFIX + FLUSH_INTERVAL_MS_SUFFIX;
    public static final String JSON_BASED_MONITOR_LOGGING_FLUSH_INTERVAL_MS_CONFIG = JSON_BASED_MONITOR_LOGGING_PREFIX + FLUSH_INTERVAL_MS_SUFFIX;
    public static final String MONITOR_LOGGING_PREPROCESSING_WORKER_COUNT_CONFIG = MONITOR_LOGGING_PREFIX + PREPROCESSING_WORKER_COUNT_SUFFIX;
    public static final String JSON_BASED_MONITOR_LOGGING_PREPROCESSING_WORKER_COUNT_CONFIG = JSON_BASED_MONITOR_LOGGING_PREFIX + PREPROCESSING_WORKER_COUNT_SUFFIX;

    private static final long MAX_DURATION_MS = Long.MAX_VALUE / 1_000_000L;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(MONITOR_LOGGING_OUTPUT_PATH_CONFIG, STRING, "output/monitor.log", new ConfigDef.NonEmptyString(), MEDIUM,
            "Base output path for MonitorLoggingBrokerInterceptor.")
        .define(JSON_BASED_MONITOR_LOGGING_OUTPUT_PATH_CONFIG, STRING, "output/json-based-monitor.log", new ConfigDef.NonEmptyString(), MEDIUM,
            "Base output path for JsonBasedMonitorLoggingBrokerInterceptor.")
        .define(MONITOR_LOGGING_ROLL_OUT_INTERVAL_MS_CONFIG, LONG, 0L, between(0L, MAX_DURATION_MS), LOW,
            "Automatic monitor log roll-out interval in milliseconds; 0 disables time-based roll-out.")
        .define(JSON_BASED_MONITOR_LOGGING_ROLL_OUT_INTERVAL_MS_CONFIG, LONG, 0L, between(0L, MAX_DURATION_MS), LOW,
            "Automatic JSON-based monitor log roll-out interval in milliseconds; 0 disables time-based roll-out.")
        .define(MONITOR_LOGGING_MAX_LOGS_PER_FILE_CONFIG, LONG, 1_000_000L, atLeast(0L), LOW,
            "Maximum monitor log records per file; 0 disables count-based roll-out.")
        .define(JSON_BASED_MONITOR_LOGGING_MAX_LOGS_PER_FILE_CONFIG, LONG, 1_000_000L, atLeast(0L), LOW,
            "Maximum JSON-based monitor log records per file; 0 disables count-based roll-out.")
        .define(MONITOR_LOGGING_FORMAT_CONFIG, STRING, "COMMA_SEPARATED", in("READ_FRIENDLY", "COMMA_SEPARATED"), LOW,
            "Monitor log file format.")
        .define(JSON_BASED_MONITOR_LOGGING_FORMAT_CONFIG, STRING, "COMMA_SEPARATED", in("READ_FRIENDLY", "COMMA_SEPARATED"), LOW,
            "JSON-based monitor log file format.")
        .define(MONITOR_LOGGING_BATCH_SIZE_CONFIG, INT, 0, atLeast(0), LOW,
            "Monitor writer batch size; 0 uses an unbounded batch policy.")
        .define(JSON_BASED_MONITOR_LOGGING_BATCH_SIZE_CONFIG, INT, 0, atLeast(0), LOW,
            "JSON-based monitor writer batch size; 0 uses an unbounded batch policy.")
        .define(MONITOR_LOGGING_FLUSH_INTERVAL_MS_CONFIG, LONG, 0L, between(0L, MAX_DURATION_MS), LOW,
            "Monitor writer automatic flush interval in milliseconds; 0 disables time-based flushing.")
        .define(JSON_BASED_MONITOR_LOGGING_FLUSH_INTERVAL_MS_CONFIG, LONG, 0L, between(0L, MAX_DURATION_MS), LOW,
            "JSON-based monitor writer automatic flush interval in milliseconds; 0 disables time-based flushing.")
        .define(MONITOR_LOGGING_PREPROCESSING_WORKER_COUNT_CONFIG, INT, 1, atLeast(1), LOW,
            "Number of monitor writer preprocessing workers.")
        .define(JSON_BASED_MONITOR_LOGGING_PREPROCESSING_WORKER_COUNT_CONFIG, INT, 1, atLeast(1), LOW,
            "Number of JSON-based monitor writer preprocessing workers.");

    private MonitorLoggingConfigs() {
    }
}
