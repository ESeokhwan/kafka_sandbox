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

import org.apache.kafka.common.KafkaException;

import java.time.Duration;

/**
 * A client for fetching records from a globally sequenced topic by global offset.
 *
 * <p>This client does not join a consumer group and does not maintain or commit a
 * position. Each fetch addresses an explicit half-open global offset range. If a
 * response is limited by the configured fetch size, callers can continue from
 * {@link GlobalSequenceConsumerRecords#nextGlobalOffset()}.</p>
 *
 * <p>The client is not thread-safe. The only method which may be called safely
 * from another thread is {@link #wakeup()}.</p>
 */
public interface GlobalSequenceConsumer<K, V> extends AutoCloseable {

    /**
     * Fetch one page of records from {@code [globalStartOffset, globalEndOffsetExclusive)}.
     *
     * @param topic the globally sequenced topic name
     * @param globalStartOffset the first global offset to fetch, inclusive
     * @param globalEndOffsetExclusive the end of the requested range, exclusive
     * @param timeout maximum time to complete metadata discovery, retries, and the fetch
     * @return records in global offset order and the offset from which to request the next page
     */
    GlobalSequenceConsumerRecords<K, V> fetch(
        String topic,
        long globalStartOffset,
        long globalEndOffsetExclusive,
        Duration timeout
    );

    /**
     * Interrupt an active {@link #fetch(String, long, long, Duration)} call.
     */
    void wakeup();

    /**
     * Close the consumer using its default close timeout.
     */
    @Override
    void close();

    /**
     * Close the consumer, waiting up to the supplied timeout for resources to close.
     *
     * @param timeout maximum time to wait
     * @throws KafkaException if the client cannot be closed cleanly
     */
    void close(Duration timeout);
}
