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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.utils.Time;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Broker-wide admission. Fixed scope tags; active key accounting is bounded by admitted work. */
public final class GlobalSequenceResources implements AutoCloseable {
    public static final String GROUP = "global-sequence-resources";
    public enum Scope {
        PRODUCE, INDEX_ROUTE, LOOKUP_ROUTE, DATA_ROUTE, INDEX_RPC, DATA_RPC,
        INDEX_READ, DATA_READ, FETCH, DATA_RESPONSE, INDEX_RESPONSE, COORDINATOR_READ, COORDINATOR_WRITE
    }
    public enum Event {
        RPC_RETRY, FENCED, GAP, RECOVERY, WORKER_RETRY
    }
    public enum Progress {
        INDEXING_LAG, RETENTION_HELD
    }
    public static final long BATCH_BYTES = 8L * 1024 * 1024;
    // Full response (including metadata) plus four outstanding full batches.
    public static final long FETCH_BYTES = 48L * 1024 * 1024;

    private volatile boolean closed;
    private final Metrics metrics;
    private final Time time;
    private final GlobalSequenceCoordinatorConfig config;
    private final Map<Scope, Budget> budgets = new EnumMap<>(Scope.class);
    private final Map<Event, Sensor> events = new EnumMap<>(Event.class);
    private final Map<Progress, AtomicLong> progress = new EnumMap<>(Progress.class);
    private final List<MetricName> names = new ArrayList<>();
    private final List<String> sensors = new ArrayList<>();

    public GlobalSequenceResources(GlobalSequenceCoordinatorConfig config, Metrics metrics, Time time) {
        this.config = config;
        this.metrics = metrics;
        this.time = time;
        for (Scope scope : Scope.values()) budgets.put(scope, new Budget(scope));
        for (Event event : Event.values()) {
            Sensor sensor = sensor("event-" + event.name());
            sensor.add(name("total", event.name()), new CumulativeCount());
            sensor.add(name("duration-ms-avg", event.name()), new Avg());
            sensor.add(name("duration-ms-max", event.name()), new Max());
            events.put(event, sensor);
        }
        for (Progress value : Progress.values()) {
            AtomicLong gauge = new AtomicLong();
            progress.put(value, gauge);
            metrics.addMetric(name("physical-offsets", value.name()), (Gauge<Long>) (c, now) -> gauge.get());
        }
    }

    public GlobalSequenceCoordinatorConfig config() {
        return config;
    }

    private MetricName name(String name, String scope) {
        MetricName metric = metrics.metricName(name, GROUP, "Global sequence " + scope + " " + name,
            Map.of("scope", scope.toLowerCase(java.util.Locale.ROOT)));
        names.add(metric);
        return metric;
    }

    private Sensor sensor(String name) {
        String id = GROUP + "-" + name;
        sensors.add(id);
        return metrics.sensor(id);
    }

    public Lease acquire(Scope scope, Object key, long bytes) {
        return budgets.get(scope).acquire(key, bytes);
    }

    public void event(Event event, double durationMs) {
        events.get(event).record(durationMs);
    }
    public void progress(Progress kind, long delta) {
        progress.get(kind).addAndGet(delta);
    }
    public long used(Scope scope) {
        return budgets.get(scope).count();
    }
    public long bytes(Scope scope) {
        return budgets.get(scope).bytes();
    }

    private final class Budget {
        private final int limit;
        private final long byteLimit;
        private final Map<Object, Integer> keys = new HashMap<>();
        private final Sensor rejected;
        private final Sensor latency;
        private final Sensor errors;
        private long count;
        private long bytes;

        private Budget(Scope scope) {
            limit = scope == Scope.PRODUCE ? config.maxProduceWaiters() : config.maxPendingOperations();
            byteLimit = switch (scope) {
                case FETCH -> config.fetchBufferBytes();
                case DATA_RPC -> config.dataRpcBufferBytes();
                case DATA_RESPONSE, INDEX_RESPONSE -> config.readResponseBufferBytes();
                default -> Long.MAX_VALUE;
            };
            rejected = sensor(scope + "-rejected");
            rejected.add(name("rejected-total", scope.name()), new CumulativeCount());
            errors = sensor(scope + "-errors");
            errors.add(name("errors-total", scope.name()), new CumulativeCount());
            latency = sensor(scope + "-latency");
            latency.add(name("duration-ms-avg", scope.name()), new Avg());
            latency.add(name("duration-ms-max", scope.name()), new Max());
            latency.add(name("completed-total", scope.name()), new CumulativeCount());
            metrics.addMetric(name("pending", scope.name()), (Gauge<Long>) (c, now) -> count());
            metrics.addMetric(name("reserved-bytes", scope.name()), (Gauge<Long>) (c, now) -> bytes());
        }

        private synchronized long count() {
            return count;
        }
        private synchronized long bytes() {
            return bytes;
        }

        private synchronized Lease acquire(Object key, long size) {
            if (closed) throw new org.apache.kafka.common.errors.CoordinatorNotAvailableException("Global sequence resources are closed");
            if (size < 0) throw new IllegalArgumentException("Negative byte reservation");
            int perKey = keys.getOrDefault(key, 0);
            // Reserve room for another key even when byte capacity is tighter than the count cap.
            long perKeyLimit = size == 0 ? config.maxPendingPerPartition() :
                Math.min(config.maxPendingPerPartition(), Math.max(1, byteLimit / size / 2));
            if (count >= limit || perKey >= perKeyLimit || size > byteLimit - bytes) {
                rejected.record();
                throw new ThrottlingQuotaExceededException(100, "Global sequence work budget exhausted");
            }
            count++;
            bytes += size;
            keys.put(key, perKey + 1);
            return new Lease(this, key, size);
        }
    }

    public final class Lease implements AutoCloseable {
        private final Budget budget;
        private final Object key;
        private final long size;
        private final long started = time.nanoseconds();
        private boolean released;

        private Lease(Budget budget, Object key, long size) {
            this.budget = budget;
            this.key = key;
            this.size = size;
        }

        public void finish(Throwable error) {
            synchronized (budget) {
                if (released) return;
                released = true;
                budget.count--;
                budget.bytes -= size;
                budget.keys.compute(key, (k, count) -> count == 1 ? null : count - 1);
            }
            if (error != null) budget.errors.record();
            budget.latency.record(Math.max(0, time.nanoseconds() - started) / 1000000.0);
        }

        @Override
        public void close() {
            finish(null);
        }
    }

    @Override
    public void close() {
        closed = true;
        sensors.forEach(metrics::removeSensor);
        names.forEach(metrics::removeMetric);
    }
}
