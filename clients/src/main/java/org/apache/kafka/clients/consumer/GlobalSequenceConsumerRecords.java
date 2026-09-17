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
import org.apache.kafka.common.errors.ApiException;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * One page returned by a global sequence fetch, in global offset order.
 *
 * <p>The list is an immutable snapshot, but keys, values, headers and the exception
 * it references are not defensively copied. Gaps between records are valid when
 * aborted transactions have been filtered. A page can advance its cursor without
 * returning any records.</p>
 *
 * <p>After processing a page, resume at {@link #nextGlobalOffset()} with
 * {@link #topicId()}. Do not derive the cursor from the record count or last
 * returned record. Inspect both {@link #transactionPending()} and {@link #error()}
 * before continuing.</p>
 */
public final class GlobalSequenceConsumerRecords<K, V> implements Iterable<GlobalSequenceConsumerRecord<K, V>> {
    /** The committed end is unknown for a page created with the legacy constructor. */
    public static final long UNKNOWN_COMMITTED_GLOBAL_END_OFFSET = -1L;

    private final List<GlobalSequenceConsumerRecord<K, V>> records;
    private final long nextGlobalOffset;
    private final Uuid topicId;
    private final long committedGlobalEndOffset;
    private final boolean transactionPending;
    private final ApiException error;

    /**
     * Construct a page without snapshot information, preserving the original API.
     *
     * <p>The topic UUID is {@link Uuid#ZERO_UUID}, the committed end is
     * {@link #UNKNOWN_COMMITTED_GLOBAL_END_OFFSET}, pending is false and error is
     * empty. These defaults do not prove that the requested range has completed or
     * that the topic still has the same identity. This constructor preserves the
     * supplied order and cursor without validating their relationship.</p>
     *
     * @param records the records to copy into an immutable list, with no null elements
     * @param nextGlobalOffset the supplied next cursor
     */
    public GlobalSequenceConsumerRecords(List<GlobalSequenceConsumerRecord<K, V>> records, long nextGlobalOffset) {
        this.records = List.copyOf(Objects.requireNonNull(records, "records"));
        this.nextGlobalOffset = nextGlobalOffset;
        this.topicId = Uuid.ZERO_UUID;
        this.committedGlobalEndOffset = UNKNOWN_COMMITTED_GLOBAL_END_OFFSET;
        this.transactionPending = false;
        this.error = null;
    }

    /**
     * Construct a page with the validated snapshot returned by a consumer fetch.
     *
     * <p>Records must have strictly increasing, nonnegative global offsets below
     * both the next cursor and the committed end. They need not be contiguous.
     * Pending and error are mutually exclusive. A pending range must start below
     * the committed end. An empty non-pending page may have a cursor beyond the
     * committed end when the requested start is beyond that end.</p>
     *
     * @param records the records to copy into an immutable list, with no null elements
     * @param nextGlobalOffset the nonnegative first unprocessed global offset
     * @param topicId the non-null, nonzero data topic UUID
     * @param committedGlobalEndOffset the nonnegative exclusive committed index allocation boundary
     * @param transactionPending whether the first unprocessed range is blocked by a transaction
     * @param error the broker error at the next cursor, or an empty optional
     * @throws NullPointerException if records, a record, topicId or error is null
     * @throws IllegalArgumentException if the snapshot, ordering or cursor is inconsistent
     */
    public GlobalSequenceConsumerRecords(
        List<GlobalSequenceConsumerRecord<K, V>> records,
        long nextGlobalOffset,
        Uuid topicId,
        long committedGlobalEndOffset,
        boolean transactionPending,
        Optional<? extends ApiException> error
    ) {
        this.records = List.copyOf(Objects.requireNonNull(records, "records"));
        this.topicId = Objects.requireNonNull(topicId, "topicId");
        this.error = Objects.requireNonNull(error, "error").orElse(null);
        if (Uuid.ZERO_UUID.equals(topicId))
            throw new IllegalArgumentException("topicId must not be ZERO_UUID");
        if (nextGlobalOffset < 0 || committedGlobalEndOffset < 0)
            throw new IllegalArgumentException("Global cursor and committed end must be nonnegative");
        if (!this.records.isEmpty() && nextGlobalOffset > committedGlobalEndOffset)
            throw new IllegalArgumentException("A nonempty page cannot advance beyond the committed end");
        if (transactionPending && (this.error != null || nextGlobalOffset >= committedGlobalEndOffset))
            throw new IllegalArgumentException("A pending range must be below the committed end and have no error");
        validateOffsets(this.records, nextGlobalOffset, committedGlobalEndOffset);
        this.nextGlobalOffset = nextGlobalOffset;
        this.committedGlobalEndOffset = committedGlobalEndOffset;
        this.transactionPending = transactionPending;
    }

    private static void validateOffsets(List<? extends GlobalSequenceConsumerRecord<?, ?>> records, long next, long end) {
        long previous = -1;
        for (GlobalSequenceConsumerRecord<?, ?> record : records) {
            long offset = record.globalOffset();
            if (offset <= previous || offset >= next || offset >= end)
                throw new IllegalArgumentException("Record global offsets must increase strictly below the cursor and committed end");
            previous = offset;
        }
    }

    /**
     * An immutable list in global order. It is not grouped or sorted by physical partition.
     */
    public List<GlobalSequenceConsumerRecord<K, V>> records() {
        return records;
    }

    /**
     * The first unprocessed global offset. This may advance over aborted records
     * not present in {@link #records()}, including for an empty page.
     */
    public long nextGlobalOffset() {
        return nextGlobalOffset;
    }

    /**
     * The data topic UUID to retain with the resume cursor, or {@link Uuid#ZERO_UUID}
     * for a page constructed without snapshot information.
     */
    public Uuid topicId() {
        return topicId;
    }

    /**
     * The exclusive end of allocations committed in the index snapshot. This is
     * neither an index-log physical offset nor a transaction visibility boundary.
     * It may exceed the cursor of a read_committed page blocked by a transaction.
     * Returns {@link #UNKNOWN_COMMITTED_GLOBAL_END_OFFSET} for a legacy page.
     */
    public long committedGlobalEndOffset() {
        return committedGlobalEndOffset;
    }

    /**
     * Whether read_committed stopped at the first range not below its source LSO.
     * Any preceding records remain usable. Retry at the same next cursor after the
     * transaction has completed; do not skip to a later global range.
     */
    public boolean transactionPending() {
        return transactionPending;
    }

    /**
     * The broker error at {@link #nextGlobalOffset()}, if any. Earlier records and
     * already filtered aborted ranges form a usable prefix. Empty records do not
     * imply that no progress was made. An empty error alone does not imply the
     * requested range has completed; also check the cursor and pending status.
     */
    public Optional<ApiException> error() {
        return Optional.ofNullable(error);
    }

    /** Returns whether this page contains no records, not whether the fetch range is complete. */
    public boolean isEmpty() {
        return records.isEmpty();
    }

    /** Returns the number of records, which may differ from the number of global offsets processed. */
    public int count() {
        return records.size();
    }

    @Override
    public Iterator<GlobalSequenceConsumerRecord<K, V>> iterator() {
        return records.iterator();
    }

    @Override
    public String toString() {
        return "GlobalSequenceConsumerRecords(" +
            "records=" + records.size() +
            ", nextGlobalOffset=" + nextGlobalOffset +
            ", topicId=" + topicId +
            ", committedGlobalEndOffset=" + committedGlobalEndOffset +
            ", transactionPending=" + transactionPending +
            ", error=" + error +
            ')';
    }
}
