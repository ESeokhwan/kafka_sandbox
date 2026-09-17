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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.GlobalSequenceConsumerRecord;
import org.apache.kafka.clients.consumer.GlobalSequenceConsumerRecords;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.RecordDeserializationException.DeserializationExceptionOrigin;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.FetchGlobalSequenceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Validates and decodes one bounded {@code FetchGlobalSequence} response. */
final class GlobalSequencePageDecoder<K, V> implements AutoCloseable {
    static final int MAX_BATCH_BYTES = 8 * 1024 * 1024;
    static final int MAX_PAYLOAD_BYTES = 16 * 1024 * 1024 - 128 * 1024;

    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final boolean checkCrcs;
    private final IsolationLevel isolationLevel;
    private final int maxBatches;
    private final int maxBytes;
    private final Runnable decodeBoundary;
    private final BufferSupplier decompressionBufferSupplier;

    GlobalSequencePageDecoder(
        Deserializer<K> keyDeserializer,
        Deserializer<V> valueDeserializer,
        boolean checkCrcs,
        IsolationLevel isolationLevel,
        int maxBatches,
        int maxBytes,
        Runnable decodeBoundary
    ) {
        this(keyDeserializer, valueDeserializer, checkCrcs, isolationLevel, maxBatches, maxBytes,
            decodeBoundary, BufferSupplier.create());
    }

    GlobalSequencePageDecoder(
        Deserializer<K> keyDeserializer,
        Deserializer<V> valueDeserializer,
        boolean checkCrcs,
        IsolationLevel isolationLevel,
        int maxBatches,
        int maxBytes,
        Runnable decodeBoundary,
        BufferSupplier decompressionBufferSupplier
    ) {
        this.keyDeserializer = Objects.requireNonNull(keyDeserializer, "keyDeserializer");
        this.valueDeserializer = Objects.requireNonNull(valueDeserializer, "valueDeserializer");
        this.isolationLevel = Objects.requireNonNull(isolationLevel, "isolationLevel");
        this.decodeBoundary = Objects.requireNonNull(decodeBoundary, "decodeBoundary");
        this.decompressionBufferSupplier = Objects.requireNonNull(
            decompressionBufferSupplier, "decompressionBufferSupplier");
        if (maxBatches < 1 || maxBatches > 1000)
            throw new IllegalArgumentException("maxBatches must be between 1 and 1000");
        if (maxBytes < 1 || maxBytes > MAX_PAYLOAD_BYTES)
            throw new IllegalArgumentException("maxBytes must be between 1 and " + MAX_PAYLOAD_BYTES);
        this.checkCrcs = checkCrcs;
        this.maxBatches = maxBatches;
        this.maxBytes = maxBytes;
    }

    GlobalSequenceConsumerRecords<K, V> decode(
        String topic,
        Uuid expectedTopicId,
        long requestedStart,
        long requestedEndExclusive,
        FetchGlobalSequenceResponseData response
    ) {
        validateRequest(topic, expectedTopicId, requestedStart, requestedEndExclusive, response);
        decodeBoundary.run();
        validateTopicId(topic, expectedTopicId, response.topicId());

        Errors error = Errors.forCode(response.errorCode());
        ApiException responseError = error == Errors.NONE ? null : error.exception(response.errorMessage());
        if (!hasSnapshot(response)) {
            validateEarlyError(topic, expectedTopicId, requestedStart, response, responseError);
            throw corrupt(topic, expectedTopicId, "successful response did not contain a snapshot");
        }

        validateSnapshot(topic, expectedTopicId, requestedStart, requestedEndExclusive, response, responseError);
        List<GlobalSequenceConsumerRecord<K, V>> records = new ArrayList<>();
        BatchFlow flow = decodeBatches(topic, expectedTopicId, requestedStart, requestedEndExclusive,
            response, records);
        validatePageFlow(topic, expectedTopicId, requestedStart, requestedEndExclusive, response,
            responseError, flow);
        return new GlobalSequenceConsumerRecords<>(records, response.nextGlobalOffset(), expectedTopicId,
            response.committedGlobalEndOffset(), response.transactionPending(), Optional.ofNullable(responseError));
    }

    private static void validateRequest(
        String topic,
        Uuid expectedTopicId,
        long requestedStart,
        long requestedEndExclusive,
        FetchGlobalSequenceResponseData response
    ) {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(expectedTopicId, "expectedTopicId");
        Objects.requireNonNull(response, "response");
        if (topic.isEmpty())
            throw new IllegalArgumentException("topic must not be empty");
        if (Uuid.ZERO_UUID.equals(expectedTopicId))
            throw new IllegalArgumentException("expectedTopicId must not be ZERO_UUID");
        if (requestedStart < 0 || requestedEndExclusive < requestedStart)
            throw new IllegalArgumentException("Invalid global offset range");
    }

    private static void validateTopicId(String topic, Uuid expected, Uuid actual) {
        if (actual == null || !expected.equals(actual))
            throw corrupt(topic, expected, "response topic ID was " + actual);
    }

    private static boolean hasSnapshot(FetchGlobalSequenceResponseData response) {
        Uuid indexTopicId = response.indexTopicId();
        return indexTopicId != null && !Uuid.ZERO_UUID.equals(indexTopicId);
    }

    private static void validateEarlyError(
        String topic,
        Uuid topicId,
        long requestedStart,
        FetchGlobalSequenceResponseData response,
        ApiException responseError
    ) {
        if (response.batches() == null || !response.batches().isEmpty() || response.transactionPending() ||
            (response.nextGlobalOffset() != -1 && response.nextGlobalOffset() != requestedStart))
            throw corrupt(topic, topicId, "response without a snapshot carried page data");
        if (responseError != null)
            throw responseError;
    }

    private static void validateSnapshot(
        String topic,
        Uuid topicId,
        long requestedStart,
        long requestedEndExclusive,
        FetchGlobalSequenceResponseData response,
        ApiException responseError
    ) {
        validateSnapshotCoordinates(topic, topicId, response);
        validateSnapshotCursor(topic, topicId, requestedStart, requestedEndExclusive, response);
        if (response.transactionPending() && responseError != null)
            throw corrupt(topic, topicId, "transaction pending and an error were both set");
    }

    private static void validateSnapshotCoordinates(
        String topic,
        Uuid topicId,
        FetchGlobalSequenceResponseData response
    ) {
        if (response.indexPartition() < 0 || response.coordinatorLeaderEpoch() < 0 ||
            response.indexHighWatermark() < 0 || response.committedGlobalEndOffset() < 0)
            throw corrupt(topic, topicId, "snapshot contained a negative partition, epoch, or offset");
        if (response.batches() == null)
            throw corrupt(topic, topicId, "batch list was null");
    }

    private static void validateSnapshotCursor(
        String topic,
        Uuid topicId,
        long requestedStart,
        long requestedEndExclusive,
        FetchGlobalSequenceResponseData response
    ) {
        long next = response.nextGlobalOffset();
        if (next < requestedStart || next > requestedEndExclusive)
            throw corrupt(topic, topicId, "next global offset " + next + " was outside the requested range");
        if (next > response.committedGlobalEndOffset() && !validCursorBeyondCommittedEnd(requestedStart, response))
            throw corrupt(topic, topicId, "cursor advanced beyond the committed global end");
    }

    private static boolean validCursorBeyondCommittedEnd(
        long requestedStart,
        FetchGlobalSequenceResponseData response
    ) {
        return response.nextGlobalOffset() == requestedStart &&
            requestedStart >= response.committedGlobalEndOffset() &&
            response.batches().isEmpty() &&
            !response.transactionPending();
    }

    private BatchFlow decodeBatches(
        String topic,
        Uuid topicId,
        long requestedStart,
        long requestedEndExclusive,
        FetchGlobalSequenceResponseData response,
        List<GlobalSequenceConsumerRecord<K, V>> output
    ) {
        List<FetchGlobalSequenceResponseData.FetchedBatch> batches = response.batches();
        if (batches.size() > maxBatches)
            throw corrupt(topic, topicId, "response contained more than " + maxBatches + " batches");
        long previousGlobalEnd = -1;
        long previousSelectedEnd = -1;
        long encodedBytes = 0;
        for (FetchGlobalSequenceResponseData.FetchedBatch entry : batches) {
            decodeBoundary.run();
            if (entry == null)
                throw corrupt(topic, topicId, "batch list contained null");
            BatchBounds bounds = validateBatchMapping(topic, topicId, requestedStart,
                requestedEndExclusive, response.committedGlobalEndOffset(), entry, previousGlobalEnd);
            if (isolationLevel == IsolationLevel.READ_UNCOMMITTED && previousSelectedEnd >= 0 &&
                bounds.selectedStart != previousSelectedEnd)
                throw corrupt(topic, topicId, "uncommitted response contained a global gap");
            MemoryRecords memoryRecords = memoryRecords(topic, topicId, entry);
            encodedBytes = Math.addExact(encodedBytes, memoryRecords.sizeInBytes());
            if (memoryRecords.sizeInBytes() > MAX_BATCH_BYTES || encodedBytes > MAX_PAYLOAD_BYTES)
                throw corrupt(topic, topicId, "encoded batch data exceeded the global wire limit");
            if (encodedBytes > maxBytes && batches.size() != 1)
                throw corrupt(topic, topicId, "multi-batch response exceeded maxBytes");
            decodeBatch(topic, topicId, entry, bounds, memoryRecords, output);
            previousGlobalEnd = bounds.globalEnd;
            previousSelectedEnd = bounds.selectedEnd;
        }
        return new BatchFlow(batches.isEmpty() ? -1 : batches.get(0).selectedGlobalStartOffset(),
            previousSelectedEnd);
    }

    private static BatchBounds validateBatchMapping(
        String topic,
        Uuid topicId,
        long requestedStart,
        long requestedEndExclusive,
        long committedGlobalEnd,
        FetchGlobalSequenceResponseData.FetchedBatch entry,
        long previousGlobalEnd
    ) {
        if (entry.globalBaseOffset() < 0 || entry.physicalPartition() < 0 ||
            entry.physicalBaseOffset() < 0 || entry.physicalLastOffset() < entry.physicalBaseOffset() ||
            entry.recordCount() <= 0)
            throw corrupt(topic, topicId, "batch mapping contained a negative or empty physical range");
        if (entry.physicalLastOffset() - entry.physicalBaseOffset() != (long) entry.recordCount() - 1)
            throw corrupt(topic, topicId, "physical range did not match recordCount");
        long globalEnd;
        try {
            globalEnd = Math.addExact(entry.globalBaseOffset(), entry.recordCount());
        } catch (ArithmeticException e) {
            throw corrupt(topic, topicId, "global batch range overflowed", e);
        }
        if (globalEnd > committedGlobalEnd || previousGlobalEnd > entry.globalBaseOffset())
            throw corrupt(topic, topicId, "global batch mappings overlapped or exceeded the committed end");
        long selectedStart = Math.max(requestedStart, entry.globalBaseOffset());
        long selectedEnd = Math.min(requestedEndExclusive, globalEnd);
        if (selectedStart >= selectedEnd || entry.selectedGlobalStartOffset() != selectedStart ||
            entry.selectedGlobalEndOffset() != selectedEnd)
            throw corrupt(topic, topicId, "selected range did not match the requested batch intersection");
        return new BatchBounds(globalEnd, selectedStart, selectedEnd);
    }

    private static MemoryRecords memoryRecords(
        String topic,
        Uuid topicId,
        FetchGlobalSequenceResponseData.FetchedBatch entry
    ) {
        if (!(entry.records() instanceof MemoryRecords))
            throw corrupt(topic, topicId, "fetched batch did not contain in-memory records");
        return (MemoryRecords) entry.records();
    }

    private void decodeBatch(
        String topic,
        Uuid topicId,
        FetchGlobalSequenceResponseData.FetchedBatch entry,
        BatchBounds bounds,
        MemoryRecords records,
        List<GlobalSequenceConsumerRecord<K, V>> output
    ) {
        try {
            Iterator<? extends RecordBatch> iterator = records.batches().iterator();
            if (!iterator.hasNext())
                throw corrupt(topic, topicId, "complete physical batch was missing");
            RecordBatch batch = iterator.next();
            if (iterator.hasNext() || batch.sizeInBytes() != records.sizeInBytes())
                throw corrupt(topic, topicId, "mapping did not contain exactly one complete physical batch");
            validateRecordBatch(topic, topicId, entry, batch);
            readRecords(topic, topicId, entry, bounds, batch, output);
        } catch (CorruptRecordException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("Invalid global sequence page"))
                throw e;
            throw corrupt(topic, topicId, "physical batch was corrupt", e);
        }
    }

    private void validateRecordBatch(
        String topic,
        Uuid topicId,
        FetchGlobalSequenceResponseData.FetchedBatch entry,
        RecordBatch batch
    ) {
        if (checkCrcs)
            batch.ensureValid();
        if (batch.magic() != RecordBatch.MAGIC_VALUE_V2 || batch.isControlBatch() ||
            batch.baseOffset() != entry.physicalBaseOffset() ||
            batch.lastOffset() != entry.physicalLastOffset() ||
            batch.countOrNull() == null || batch.countOrNull() != entry.recordCount())
            throw corrupt(topic, topicId, "physical records did not match their mapping");
        if (batch.partitionLeaderEpoch() < RecordBatch.NO_PARTITION_LEADER_EPOCH)
            throw corrupt(topic, topicId, "physical batch contained an invalid source leader epoch");
    }

    private void readRecords(
        String topic,
        Uuid topicId,
        FetchGlobalSequenceResponseData.FetchedBatch entry,
        BatchBounds bounds,
        RecordBatch batch,
        List<GlobalSequenceConsumerRecord<K, V>> output
    ) {
        int count = 0;
        try (CloseableIterator<Record> iterator = batch.streamingIterator(decompressionBufferSupplier)) {
            while (iterator.hasNext()) {
                decodeBoundary.run();
                Record record = iterator.next();
                long expectedPhysicalOffset = entry.physicalBaseOffset() + count;
                if (record.offset() != expectedPhysicalOffset)
                    throw corrupt(topic, topicId, "record offsets were not contiguous inside the physical batch");
                if (checkCrcs)
                    record.ensureValid();
                long globalOffset = entry.globalBaseOffset() + count;
                if (globalOffset >= bounds.selectedStart && globalOffset < bounds.selectedEnd)
                    output.add(deserialize(topic, topicId, entry.physicalPartition(), globalOffset, batch, record));
                count++;
            }
        }
        if (count != entry.recordCount())
            throw corrupt(topic, topicId, "decoded record count did not match the mapping");
    }

    private GlobalSequenceConsumerRecord<K, V> deserialize(
        String topic,
        Uuid topicId,
        int partition,
        long globalOffset,
        RecordBatch batch,
        Record record
    ) {
        Headers headers = new RecordHeaders(record.headers());
        ByteBuffer keyBytes = duplicate(record.key());
        ByteBuffer valueBytes = duplicate(record.value());
        K key;
        V value;
        decodeBoundary.run();
        try {
            key = keyBytes == null ? null : keyDeserializer.deserialize(topic, headers, keyBytes.duplicate());
        } catch (RuntimeException e) {
            throw deserializationException(DeserializationExceptionOrigin.KEY, topic, topicId, partition,
                globalOffset, batch.timestampType(), record, keyBytes, valueBytes, headers, e);
        }
        decodeBoundary.run();
        try {
            value = valueBytes == null ? null : valueDeserializer.deserialize(topic, headers, valueBytes.duplicate());
        } catch (RuntimeException e) {
            throw deserializationException(DeserializationExceptionOrigin.VALUE, topic, topicId, partition,
                globalOffset, batch.timestampType(), record, keyBytes, valueBytes, headers, e);
        }
        decodeBoundary.run();
        Optional<Integer> leaderEpoch = batch.partitionLeaderEpoch() == RecordBatch.NO_PARTITION_LEADER_EPOCH ?
            Optional.empty() : Optional.of(batch.partitionLeaderEpoch());
        return new GlobalSequenceConsumerRecord<>(topic, globalOffset, partition, record.offset(),
            record.timestamp(), batch.timestampType(), size(keyBytes), size(valueBytes), key, value,
            headers, leaderEpoch);
    }

    private static RecordDeserializationException deserializationException(
        DeserializationExceptionOrigin origin,
        String topic,
        Uuid topicId,
        int partition,
        long globalOffset,
        TimestampType timestampType,
        Record record,
        ByteBuffer keyBytes,
        ByteBuffer valueBytes,
        Headers headers,
        RuntimeException cause
    ) {
        String message = "Error deserializing " + origin + " for global sequence topic " + topic +
            " (topicId=" + topicId + ") at global offset " + globalOffset +
            ", physical partition " + partition + " offset " + record.offset();
        return new RecordDeserializationException(origin, new TopicPartition(topic, partition), record.offset(),
            record.timestamp(), timestampType, duplicate(keyBytes), duplicate(valueBytes), headers, message, cause);
    }

    private void validatePageFlow(
        String topic,
        Uuid topicId,
        long requestedStart,
        long requestedEndExclusive,
        FetchGlobalSequenceResponseData response,
        ApiException responseError,
        BatchFlow flow
    ) {
        long next = response.nextGlobalOffset();
        if (flow.lastSelectedEnd >= 0 && flow.lastSelectedEnd > next)
            throw corrupt(topic, topicId, "next cursor preceded returned records");
        if (isolationLevel == IsolationLevel.READ_UNCOMMITTED)
            validateUncommittedFlow(topic, topicId, requestedStart, response, flow);
        validateTerminalFlow(topic, topicId, requestedStart, requestedEndExclusive, response, responseError);
    }

    private static void validateUncommittedFlow(
        String topic,
        Uuid topicId,
        long requestedStart,
        FetchGlobalSequenceResponseData response,
        BatchFlow flow
    ) {
        if (response.transactionPending())
            throw corrupt(topic, topicId, "uncommitted response reported a pending transaction");
        if (flow.firstSelectedStart >= 0 && flow.firstSelectedStart != requestedStart)
            throw corrupt(topic, topicId, "uncommitted page skipped its first global range");
        if (flow.firstSelectedStart >= 0 && flow.lastSelectedEnd != response.nextGlobalOffset())
            throw corrupt(topic, topicId, "uncommitted page cursor skipped a global range");
        if (flow.firstSelectedStart < 0 && response.nextGlobalOffset() != requestedStart)
            throw corrupt(topic, topicId, "empty uncommitted page advanced its cursor");
    }

    private static void validateTerminalFlow(
        String topic,
        Uuid topicId,
        long requestedStart,
        long requestedEndExclusive,
        FetchGlobalSequenceResponseData response,
        ApiException responseError
    ) {
        long next = response.nextGlobalOffset();
        long readableEnd = Math.min(requestedEndExclusive, response.committedGlobalEndOffset());
        if (response.transactionPending() && next >= readableEnd)
            throw corrupt(topic, topicId, "pending cursor was not before the readable end");
        if (responseError == null && !response.transactionPending() && next == requestedStart &&
            requestedStart < readableEnd)
            throw corrupt(topic, topicId, "response made no progress inside the committed range");
    }

    private static int size(ByteBuffer bytes) {
        return bytes == null ? ConsumerRecord.NULL_SIZE : bytes.remaining();
    }

    private static ByteBuffer duplicate(ByteBuffer bytes) {
        return bytes == null ? null : bytes.duplicate();
    }

    private static CorruptRecordException corrupt(String topic, Uuid topicId, String detail) {
        return new CorruptRecordException("Invalid global sequence page for topic " + topic +
            " (topicId=" + topicId + "): " + detail);
    }

    private static CorruptRecordException corrupt(String topic, Uuid topicId, String detail, Throwable cause) {
        return new CorruptRecordException("Invalid global sequence page for topic " + topic +
            " (topicId=" + topicId + "): " + detail, cause);
    }

    @Override
    public void close() {
        decompressionBufferSupplier.close();
    }

    private static final class BatchBounds {
        private final long globalEnd;
        private final long selectedStart;
        private final long selectedEnd;

        private BatchBounds(long globalEnd, long selectedStart, long selectedEnd) {
            this.globalEnd = globalEnd;
            this.selectedStart = selectedStart;
            this.selectedEnd = selectedEnd;
        }
    }

    private static final class BatchFlow {
        private final long firstSelectedStart;
        private final long lastSelectedEnd;

        private BatchFlow(long firstSelectedStart, long lastSelectedEnd) {
            this.firstSelectedStart = firstSelectedStart;
            this.lastSelectedEnd = lastSelectedEnd;
        }
    }
}
