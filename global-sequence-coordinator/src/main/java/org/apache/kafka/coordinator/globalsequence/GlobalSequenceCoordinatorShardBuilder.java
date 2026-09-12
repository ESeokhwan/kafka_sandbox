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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShardBuilder;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.util.Objects;

/** Adapts the allocator to CoordinatorRuntime's shard construction contract. */
public class GlobalSequenceCoordinatorShardBuilder implements CoordinatorShardBuilder<GlobalSequenceCoordinatorShard, CoordinatorRecord> {
    private SnapshotRegistry snapshotRegistry;

    @Override
    public GlobalSequenceCoordinatorShardBuilder withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
        this.snapshotRegistry = snapshotRegistry;
        return this;
    }

    @Override
    public GlobalSequenceCoordinatorShardBuilder withLogContext(LogContext logContext) {
        return this;
    }

    @Override
    public GlobalSequenceCoordinatorShardBuilder withTime(Time time) {
        return this;
    }

    @Override
    public GlobalSequenceCoordinatorShardBuilder withTimer(CoordinatorTimer<Void, CoordinatorRecord> timer) {
        return this;
    }

    @Override
    public GlobalSequenceCoordinatorShardBuilder withExecutor(CoordinatorExecutor<CoordinatorRecord> executor) {
        return this;
    }

    @Override
    public GlobalSequenceCoordinatorShardBuilder withCoordinatorMetrics(CoordinatorMetrics metrics) {
        return this;
    }

    @Override
    public GlobalSequenceCoordinatorShardBuilder withTopicPartition(TopicPartition topicPartition) {
        return this;
    }

    @Override
    public GlobalSequenceCoordinatorShard build() {
        return new GlobalSequenceCoordinatorShard(Objects.requireNonNull(snapshotRegistry, "snapshotRegistry"));
    }
}
