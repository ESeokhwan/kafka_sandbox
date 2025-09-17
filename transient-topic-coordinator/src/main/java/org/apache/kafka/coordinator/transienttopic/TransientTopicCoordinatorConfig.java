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
package org.apache.kafka.coordinator.transienttopic;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;

public class TransientTopicCoordinatorConfig {

    public static final String COMMIT_TIMEOUT_MS_CONFIG = "transienttopic.commit.timeout.ms";
    public static final int COMMIT_TIMEOUT_MS_DEFAULT = 5000;
    public static final String COMMIT_TIMEOUT_MS_DOC = "Index of transient topic commit will be delayed until all replicas " +
        "for the index topic receive the commit or this timeout is reached. This is similar to producer request timeout.";

    public static final ConfigDef TRANSIENT_TOPIC_COORDINATOR_CONFIG_DEF = new ConfigDef()
        .define(COMMIT_TIMEOUT_MS_CONFIG, INT, COMMIT_TIMEOUT_MS_DEFAULT, atLeast(1), HIGH, COMMIT_TIMEOUT_MS_DOC);

    private final int commitTimeoutMs;

    public TransientTopicCoordinatorConfig(AbstractConfig config) {
        this.commitTimeoutMs = config.getInt(COMMIT_TIMEOUT_MS_CONFIG);
    }

    public int commitTimeoutMs() {
        return commitTimeoutMs;
    }
}
