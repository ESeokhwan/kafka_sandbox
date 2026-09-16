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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GlobalSequenceConsumerTest {
    /** Compiles without implementing the new overload, as an existing external implementation would. */
    private static class LegacyConsumer implements GlobalSequenceConsumer<String, String> {
        private int fetches;

        @Override
        public GlobalSequenceConsumerRecords<String, String> fetch(String topic, long start, long end, Duration timeout) {
            fetches++;
            return new GlobalSequenceConsumerRecords<>(List.of(), start);
        }

        @Override
        public void wakeup() { }

        @Override
        public void close() { }

        @Override
        public void close(Duration timeout) { }
    }

    @Test
    void testLegacyImplementationRemainsUsableWithoutAnUnverifiedUuidFallback() {
        try (LegacyConsumer consumer = new LegacyConsumer()) {
            assertEquals(12, consumer.fetch("events", 12, 20, Duration.ofSeconds(1)).nextGlobalOffset());
            assertEquals(1, consumer.fetches);
            assertThrows(UnsupportedOperationException.class, () ->
                consumer.fetch("events", new Uuid(10, 20), 12, 20, Duration.ofSeconds(1)));
            assertEquals(1, consumer.fetches, "UUID validation must never delegate to an unprotected name-only fetch");
        }
    }

    @Test
    void testDefaultOverloadRejectsUnknownExpectedIdentityWithoutFetching() {
        try (LegacyConsumer consumer = new LegacyConsumer()) {
            assertThrows(NullPointerException.class, () -> consumer.fetch("events", null, 0, 10, Duration.ofSeconds(1)));
            assertThrows(IllegalArgumentException.class, () -> consumer.fetch("events", Uuid.ZERO_UUID, 0, 10, Duration.ofSeconds(1)));
            assertEquals(0, consumer.fetches);
        }
    }
}
