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
package org.apache.kafka.coordinator.globalsequence;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer;
import org.apache.kafka.coordinator.globalsequence.generated.GlobalSequenceIndexLogKey;
import org.apache.kafka.coordinator.globalsequence.generated.GlobalSequenceIndexLogValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class GlobalSequenceCoordinatorShardTest {
    private static final Uuid TOPIC_ID = Uuid.randomUuid();

    private GlobalSequenceCoordinatorShard shard;

    @BeforeEach
    void setUp() {
        LogContext logContext = new LogContext();
        MockTime time = new MockTime();
        GlobalSequenceStateRegistry stateRegistry = new GlobalSequenceStateRegistry(logContext);
        stateRegistry.createNewTopicState(TOPIC_ID);

        GlobalSequenceCoordinatorConfig config = new GlobalSequenceCoordinatorConfig(
            new AbstractConfig(GlobalSequenceCoordinatorConfig.CONFIG_DEF, Map.of())
        );
        shard = new GlobalSequenceCoordinatorShard(
            logContext,
            stateRegistry,
            time,
            new MockCoordinatorTimer<>(time),
            config,
            mock(CoordinatorMetrics.class),
            mock(CoordinatorMetricsShard.class)
        );
    }

    @Test
    void testAppendIndexCreatesVersionZeroRecordAndResult() {
        GlobalSequenceAppendRequest request = new GlobalSequenceAppendRequest(
            TOPIC_ID,
            1,
            20L,
            3
        );

        CoordinatorResult<GlobalSequenceAppendResult, CoordinatorRecord> result = shard.appendIndex(request);

        CoordinatorRecord expectedRecord = CoordinatorRecord.record(
            new GlobalSequenceIndexLogKey()
                .setTopicId(TOPIC_ID)
                .setGlobalOffset(0L),
            new ApiMessageAndVersion(
                new GlobalSequenceIndexLogValue()
                    .setRecordsCount(3)
                    .setPartitionIndex(1)
                    .setPartitionOffset(20L),
                (short) 0
            )
        );

        assertEquals(new GlobalSequenceAppendResult(0L, 3, false), result.response());
        assertEquals(1, result.records().size());
        assertEquals(expectedRecord, result.records().get(0));
    }
}
