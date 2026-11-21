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
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;

public class BrokerExtensionConfigs {
    public static final String BROKER_EXTENSION_REQUEST_ENABLED_CONFIG = "broker.extension.request.enabled";
    public static final String BROKER_EXTENSION_REQUEST_ALLOWED_LISTENERS_CONFIG = "broker.extension.request.allowed.listeners";
    public static final String BROKER_EXTENSION_REQUEST_MAX_PAYLOAD_BYTES_CONFIG = "broker.extension.request.max.payload.bytes";
    public static final String BROKER_EXTENSION_REQUEST_MAX_TIMEOUT_MS_CONFIG = "broker.extension.request.max.timeout.ms";
    public static final String BROKER_EXTENSION_REQUEST_MAX_IN_FLIGHT_PER_TARGET_CONFIG = "broker.extension.request.max.in.flight.per.target";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(BROKER_EXTENSION_REQUEST_ENABLED_CONFIG, BOOLEAN, false, MEDIUM,
            "Enables broker extension requests. This API is disabled by default.")
        .define(BROKER_EXTENSION_REQUEST_ALLOWED_LISTENERS_CONFIG, LIST, "", LOW,
            "Listeners allowed to accept broker extension requests when the API is enabled.")
        .define(BROKER_EXTENSION_REQUEST_MAX_PAYLOAD_BYTES_CONFIG, INT, 65536, atLeast(0), MEDIUM,
            "Maximum payload size in bytes for a broker extension request.")
        .define(BROKER_EXTENSION_REQUEST_MAX_TIMEOUT_MS_CONFIG, INT, 30000, atLeast(1), MEDIUM,
            "Maximum timeout in milliseconds for a broker extension request.")
        .define(BROKER_EXTENSION_REQUEST_MAX_IN_FLIGHT_PER_TARGET_CONFIG, INT, 1, atLeast(1), MEDIUM,
            "Maximum number of in-flight broker extension requests for each target.");

    private BrokerExtensionConfigs() {
    }
}
