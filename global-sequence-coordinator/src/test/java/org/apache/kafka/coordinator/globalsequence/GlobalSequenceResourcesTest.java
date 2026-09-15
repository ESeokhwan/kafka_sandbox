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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources.Scope.FETCH;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources.Scope.INDEX_ROUTE;
import static org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources.Scope.PRODUCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GlobalSequenceResourcesTest {
    private GlobalSequenceCoordinatorConfig config() {
        return new GlobalSequenceCoordinatorConfig(new AbstractConfig(GlobalSequenceCoordinatorConfig.CONFIG_DEF, Map.of(
            GlobalSequenceCoordinatorConfig.MAX_PENDING_OPERATIONS_CONFIG, 2,
            GlobalSequenceCoordinatorConfig.MAX_PENDING_PER_PARTITION_CONFIG, 1,
            GlobalSequenceCoordinatorConfig.MAX_PRODUCE_WAITERS_CONFIG, 2,
            GlobalSequenceCoordinatorConfig.FETCH_BUFFER_BYTES_CONFIG, (int) GlobalSequenceResources.FETCH_BYTES), false));
    }

    @Test
    void testHotPartitionAndFetchCannotExhaustOtherPartitionsOrWrites() {
        try (Metrics metrics = new Metrics(); GlobalSequenceResources resources = new GlobalSequenceResources(config(), metrics, new MockTime())) {
            var first = resources.acquire(PRODUCE, "a", 0);
            assertThrows(ThrottlingQuotaExceededException.class, () -> resources.acquire(PRODUCE, "a", 0));
            var other = resources.acquire(PRODUCE, "b", 0);
            assertThrows(ThrottlingQuotaExceededException.class, () -> resources.acquire(PRODUCE, "c", 0));
            var fetch = resources.acquire(FETCH, "a", GlobalSequenceResources.FETCH_BYTES);
            assertThrows(ThrottlingQuotaExceededException.class, () -> resources.acquire(FETCH, "b", GlobalSequenceResources.FETCH_BYTES));
            resources.acquire(INDEX_ROUTE, "a", 0).close();
            first.close();
            resources.acquire(PRODUCE, "c", 0).close();
            other.close();
            fetch.close();
            assertEquals(0, resources.used(PRODUCE));
            assertEquals(0, resources.bytes(FETCH));
        }
    }

    @Test
    void testConcurrentReleaseAndKeyChurnDoNotLeakAccountingOrMetrics() {
        MockTime time = new MockTime();
        try (Metrics metrics = new Metrics(); GlobalSequenceResources resources = new GlobalSequenceResources(config(), metrics, time)) {
            int metricCount = metrics.metrics().size();
            var lease = resources.acquire(FETCH, "a", GlobalSequenceResources.FETCH_BYTES);
            CompletableFuture<?>[] releases = new CompletableFuture<?>[20];
            for (int i = 0; i < releases.length; i++) releases[i] = CompletableFuture.runAsync(lease::close);
            CompletableFuture.allOf(releases).join();
            assertEquals(0, resources.used(FETCH));
            assertEquals(0, resources.bytes(FETCH));
            for (int i = 0; i < 1000; i++) {
                var wait = resources.acquire(PRODUCE, i, 0);
                time.sleep(1);
                wait.finish(new RuntimeException("timeout"));
            }
            assertEquals(metricCount, metrics.metrics().size());
            assertEquals(1000.0, metrics.metric(metrics.metricName("errors-total", GlobalSequenceResources.GROUP, Map.of("scope", "produce"))).metricValue());
            resources.close();
            assertEquals(0, metrics.metrics().keySet().stream().filter(name -> name.group().equals(GlobalSequenceResources.GROUP)).count());
        }
    }
}
