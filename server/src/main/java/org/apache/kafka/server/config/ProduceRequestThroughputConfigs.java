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
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/** Broker configuration for produce request throughput monitoring. */
public final class ProduceRequestThroughputConfigs {
    public static final String OUTPUT_PATH_CONFIG = "produce.request.throughput.output.path";
    public static final String MEASUREMENT_INTERVAL_MS_CONFIG = "produce.request.throughput.measurement.interval.ms";
    public static final String ROLL_OUT_INTERVAL_MS_CONFIG = "produce.request.throughput.rollout.interval.ms";
    public static final String MAX_RECORDS_PER_FILE_CONFIG = "produce.request.throughput.max.records.per.file";

    private static final long MAX_DURATION_MS = Long.MAX_VALUE / 1_000_000L;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(OUTPUT_PATH_CONFIG, STRING, "output/produce-request-throughput.csv",
            new ConfigDef.NonEmptyString(), MEDIUM,
            "Base CSV output path for produce request throughput measurements.")
        .define(MEASUREMENT_INTERVAL_MS_CONFIG, LONG, 1_000L, between(1L, MAX_DURATION_MS), MEDIUM,
            "Interval in milliseconds between produce request throughput measurements.")
        .define(ROLL_OUT_INTERVAL_MS_CONFIG, LONG, 0L, between(0L, MAX_DURATION_MS), LOW,
            "Automatic throughput file roll-out interval in milliseconds; 0 disables it.")
        .define(MAX_RECORDS_PER_FILE_CONFIG, LONG, 1_000_000L, atLeast(0L), LOW,
            "Maximum throughput records per file; 0 disables count-based roll-out.");

    private ProduceRequestThroughputConfigs() {
    }
}
