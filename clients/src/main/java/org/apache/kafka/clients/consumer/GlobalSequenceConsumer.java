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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.ConcurrentModificationException;
import java.util.Objects;

/**
 * A client for fetching records from a globally sequenced topic by global offset.
 *
 * <p>This client does not join a consumer group and does not maintain or commit a
 * position. Each fetch addresses an explicit half-open global offset range and
 * returns at most one page. Callers process that page and supply its
 * {@link GlobalSequenceConsumerRecords#nextGlobalOffset()} when requesting the next one.
 * Pages are not combined or prefetched in the background.</p>
 *
 * <p>A page may be empty while its cursor advances past aborted records. A page
 * may also contain a valid prefix followed by an error or a pending transaction.
 * Always inspect {@link GlobalSequenceConsumerRecords#error()} and
 * {@link GlobalSequenceConsumerRecords#transactionPending()}, even for an empty page.
 * The configured byte limit is a soft limit on complete encoded batches: the
 * first batch may exceed it. It does not bound deserialized object sizes.</p>
 *
 * <p>Topic names may be reused after deletion. To continue a range across calls,
 * retain both the page's {@link GlobalSequenceConsumerRecords#topicId()} and its
 * next global offset, and use {@link #fetch(String, Uuid, long, long, Duration)}.</p>
 *
 * <p>The client is not thread-safe. Concurrent calls to fetch or close are rejected
 * with {@link ConcurrentModificationException}. Only {@link #wakeup()} may be
 * called safely from another thread.</p>
 */
public interface GlobalSequenceConsumer<K, V> extends AutoCloseable {

    /**
     * Fetch one page from {@code [globalStartOffset, globalEndOffsetExclusive)} in
     * the topic currently identified by {@code topic}.
     *
     * <p>The topic UUID resolved for this call is held fixed across retries.
     * Separate calls using only a name cannot detect deletion and recreation
     * between those calls; use the UUID overload when continuing an earlier page.</p>
     *
     * <p>An empty range or the current committed end returns an empty page with
     * snapshot information. Fetch does not wait for new data at that end. With
     * {@code read_committed}, it returns at the first pending transaction instead
     * of waiting for that transaction to finish. Aborted ranges advance the cursor
     * without producing records, and control batches have no global offsets.</p>
     *
     * <p>If an error follows a processed prefix, including a prefix consisting only
     * of aborted ranges, that prefix is returned with an error at its next cursor.
     * Retriable errors without progress are retried within the original timeout;
     * terminal errors without progress are thrown. Invalid responses and
     * deserialization failures throw without returning a page or skipping offsets.</p>
     *
     * <p>One timeout covers metadata discovery, connection, version negotiation,
     * backoff, retries and fetch. It is also checked at decode boundaries. User
     * deserializers cannot be forcibly interrupted, so a blocking deserializer may
     * exceed this timeout. Zero timeout throws without starting I/O. Negative
     * timeouts and invalid arguments are rejected before I/O.</p>
     *
     * @param topic the globally sequenced topic name, not null or empty
     * @param globalStartOffset the first global offset to fetch, inclusive and nonnegative
     * @param globalEndOffsetExclusive the end of the requested range, at least the start
     * @param timeout the nonnegative timeout for the entire call
     * @return one page in global order, with its topic UUID, snapshot and resume cursor
     * @throws IllegalArgumentException if the topic, range or timeout is invalid
     * @throws NullPointerException if topic or timeout is null
     * @throws GlobalSequenceTopicIdMismatchException if the resolved topic changes identity during a retry
     * @throws TimeoutException if the deadline expires without a usable page, or the timeout is zero
     * @throws WakeupException if {@link #wakeup()} interrupts this call
     * @throws InterruptException if the calling thread is interrupted
     * @throws UnsupportedVersionException if the broker does not support the requested isolation level;
     *         read_committed requires global fetch v1 and must not be downgraded
     * @throws ConcurrentModificationException if another thread is using this client
     * @throws IllegalStateException if this client is closed
     * @throws KafkaException for authorization, unavailable source data, deserialization or other fetch failures
     */
    GlobalSequenceConsumerRecords<K, V> fetch(
        String topic,
        long globalStartOffset,
        long globalEndOffsetExclusive,
        Duration timeout
    );

    /**
     * Fetch one page only if {@code topic} still identifies {@code expectedTopicId}.
     * The range, pagination, error and deadline contracts of
     * {@link #fetch(String, long, long, Duration)} also apply to this overload.
     *
     * <p>The expected UUID is checked before fetching data and remains fixed across
     * metadata refreshes and retries. A different UUID must not cause the supplied
     * global range to be fetched from the new topic. An unresolved topic is reported
     * as a metadata failure, not as a match to an unknown UUID.</p>
     *
     * @implSpec The default implementation throws {@link UnsupportedOperationException}
     *           without invoking the name-only fetch. This preserves compatibility
     *           for existing implementations while preventing an unverified fallback.
     *           Implementations supporting UUID validation must override this method.
     * @param topic the globally sequenced topic name
     * @param expectedTopicId the expected, non-null, nonzero data topic UUID
     * @param globalStartOffset the first global offset to fetch, inclusive
     * @param globalEndOffsetExclusive the end of the requested range, exclusive
     * @param timeout the timeout for the entire call
     * @return one page belonging to the expected topic UUID
     * @throws NullPointerException if expectedTopicId is null
     * @throws IllegalArgumentException if expectedTopicId is {@link Uuid#ZERO_UUID}
     * @throws GlobalSequenceTopicIdMismatchException if metadata identifies a different topic UUID
     * @throws UnsupportedOperationException if the implementation does not support UUID validation
     */
    default GlobalSequenceConsumerRecords<K, V> fetch(
        String topic,
        Uuid expectedTopicId,
        long globalStartOffset,
        long globalEndOffsetExclusive,
        Duration timeout
    ) {
        Objects.requireNonNull(expectedTopicId, "expectedTopicId");
        if (Uuid.ZERO_UUID.equals(expectedTopicId))
            throw new IllegalArgumentException("expectedTopicId must not be ZERO_UUID");
        throw new UnsupportedOperationException("This consumer does not support fetching with an expected topic UUID");
    }

    /**
     * Interrupt an active fetch, or the next fetch if none is active, with
     * {@link WakeupException}. This is the only method safe to call from another
     * thread. Wakeup does not interrupt close or forcibly interrupt user deserializers.
     */
    void wakeup();

    /**
     * Close using a default timeout of 30 seconds. See {@link #close(Duration)}.
     */
    @Override
    void close();

    /**
     * Close the consumer, allowing up to the supplied timeout for network cleanup.
     * Zero timeout performs immediate cleanup without waiting for network work.
     * Close is idempotent and is not interrupted by {@link #wakeup()}.
     * No offsets are committed and no consumer group is contacted.
     *
     * <p>Connections, metadata, metrics and deserializers are released, including
     * deserializer instances supplied by the caller. User close callbacks cannot
     * be forcibly interrupted and may exceed the timeout.</p>
     *
     * @param timeout the nonnegative close timeout
     * @throws NullPointerException if timeout is null
     * @throws IllegalArgumentException if timeout is negative
     * @throws ConcurrentModificationException if another thread is using this client
     * @throws InterruptException if the calling thread is interrupted
     * @throws KafkaException if the client cannot be closed cleanly
     */
    void close(Duration timeout);
}
