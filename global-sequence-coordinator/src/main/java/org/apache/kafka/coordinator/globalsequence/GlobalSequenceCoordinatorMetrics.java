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
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.yammer.metrics.core.MetricsRegistry;

/**
 * The allocator currently has no shard-specific sensors. Runtime state, load, queue and flush
 * metrics are collected separately by CoordinatorRuntimeMetricsImpl. Indexer lag and allocation
 * metrics will be added with the indexer resource/metrics work.
 */
final class GlobalSequenceCoordinatorMetrics extends CoordinatorMetrics {
    private record Shard(TopicPartition topicPartition) implements CoordinatorMetricsShard {
        @Override
        public void record(String sensorName) { }

        @Override
        public void record(String sensorName, double value) { }

        @Override
        public void commitUpTo(long offset) { }
    }

    @Override
    public CoordinatorMetricsShard newMetricsShard(SnapshotRegistry registry, TopicPartition partition) {
        return new Shard(partition);
    }

    @Override
    public void activateMetricsShard(CoordinatorMetricsShard shard) { }

    @Override
    public void deactivateMetricsShard(CoordinatorMetricsShard shard) { }

    @Override
    public MetricsRegistry registry() {
        return KafkaYammerMetrics.defaultRegistry();
    }

    @Override
    public void onUpdateLastCommittedOffset(TopicPartition partition, long offset) { }
}
