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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class CoordinatorRuntimeAdmissionTest {
    private static final TopicPartition TP = new TopicPartition("index", 0);
    private static final Duration DEFAULT_WRITE_TIMEOUT = Duration.ofMillis(5);

    @Test
    public void testAdmissionRetainedAfterTimeoutOrCancellationUntilCommit() throws Exception {
        for (boolean cancel : List.of(false, true)) {
            MockTimer timer = new MockTimer();
            MockPartitionWriter writer = new MockPartitionWriter();
            ManualEventProcessor processor = new ManualEventProcessor();
            java.util.concurrent.atomic.AtomicInteger writes = new java.util.concurrent.atomic.AtomicInteger();
            try (CoordinatorRuntime<MockCoordinatorShard, String> runtime = new CoordinatorRuntime.Builder<MockCoordinatorShard, String>()
                .withTime(timer.time()).withTimer(timer).withDefaultWriteTimeOut(DEFAULT_WRITE_TIMEOUT)
                .withLoader(new MockCoordinatorLoader()).withEventProcessor(processor).withPartitionWriter(writer)
                .withCoordinatorShardBuilderSupplier(new MockCoordinatorShardBuilderSupplier())
                .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class)).withCoordinatorMetrics(mock(CoordinatorMetrics.class))
                .withSerializer(new StringSerializer()).withExecutorService(mock(ExecutorService.class))
                .withOperationAdmission((tp, write) -> {
                    if (!write) return () -> { };
                    if (!writes.compareAndSet(0, 1)) throw new org.apache.kafka.common.errors.ThrottlingQuotaExceededException(100, "full");
                    return writes::decrementAndGet;
                }).build()) {
                runtime.scheduleLoadOperation(TP, 10);
                processor.poll();
                processor.poll();
                var first = runtime.scheduleWriteOperation("first", TP, DEFAULT_WRITE_TIMEOUT,
                    shard -> new CoordinatorResult<>(List.of("record1"), "first"));
                if (cancel) first.cancel(false);
                assertEquals(1, writes.get(), "Queued events consume admission even after cancellation");
                processor.poll();
                timer.advanceClock(DEFAULT_WRITE_TIMEOUT.toMillis() + 1);
                processor.poll();
                assertTrue(first.isCompletedExceptionally());
                assertEquals(1, writes.get(), "Timeout does not remove accepted records or their deferred events");
                assertThrows(org.apache.kafka.common.errors.ThrottlingQuotaExceededException.class, () ->
                    runtime.scheduleWriteOperation("rejected", TP, DEFAULT_WRITE_TIMEOUT,
                        shard -> new CoordinatorResult<>(List.of("record2"), "second")));
                var read = runtime.scheduleReadOperation("read", TP, (shard, offset) -> offset);
                processor.poll();
                assertEquals(0L, read.join());
                writer.commit(TP, 1);
                processor.poll();
                assertEquals(0, writes.get());
                var second = runtime.scheduleWriteOperation("second", TP, DEFAULT_WRITE_TIMEOUT,
                    shard -> new CoordinatorResult<>(List.of("record2"), "second"));
                processor.poll();
                writer.commit(TP, 2);
                processor.poll();
                assertEquals("second", second.join());
                assertEquals(0, writes.get());
                var abandoned = runtime.scheduleWriteOperation("uncommitted", TP, DEFAULT_WRITE_TIMEOUT,
                    shard -> new CoordinatorResult<>(List.of("record3"), "third"));
                processor.poll();
                assertEquals(1, writes.get());
                runtime.close();
                assertTrue(abandoned.isCompletedExceptionally());
                assertEquals(0, writes.get(), "Shutdown must release accepted writes whose HW never advanced");
            }
        }
    }

}
