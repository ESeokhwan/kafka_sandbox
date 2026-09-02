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
package kafka.server;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.LookupGlobalSequenceIndexRequestData;
import org.apache.kafka.common.message.LookupGlobalSequenceIndexResponseData;
import org.apache.kafka.common.message.ReadGlobalSequenceDataRequestData;
import org.apache.kafka.common.message.ReadGlobalSequenceDataResponseData;
import org.apache.kafka.common.message.WriteGlobalSequenceIndexRequestData;
import org.apache.kafka.common.message.WriteGlobalSequenceIndexResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.LookupGlobalSequenceIndexRequest;
import org.apache.kafka.common.requests.LookupGlobalSequenceIndexResponse;
import org.apache.kafka.common.requests.ReadGlobalSequenceDataRequest;
import org.apache.kafka.common.requests.ReadGlobalSequenceDataResponse;
import org.apache.kafka.common.requests.WriteGlobalSequenceIndexRequest;
import org.apache.kafka.common.requests.WriteGlobalSequenceIndexResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceAppendRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceAppendResult;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinator;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceAbortedTransaction;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceFetchBatch;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceFetchRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceFetchResult;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceIndexRecord;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookupRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookupResult;
import org.apache.kafka.coordinator.globalsequence.GlobalSequencePhysicalFetchRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequencePhysicalFetchResult;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Routes global sequence index operations to the broker leading the target index partition.
 */
public class GlobalSequenceIndexRoutingManager implements AutoCloseable {
    public static final long DEFAULT_RETRY_BACKOFF_MS = 100L;

    private static final Set<Errors> RETRIABLE_ERRORS = EnumSet.of(
        Errors.NOT_COORDINATOR,
        Errors.COORDINATOR_NOT_AVAILABLE,
        Errors.COORDINATOR_LOAD_IN_PROGRESS,
        Errors.NOT_LEADER_OR_FOLLOWER,
        Errors.LEADER_NOT_AVAILABLE,
        Errors.BROKER_NOT_AVAILABLE,
        Errors.REQUEST_TIMED_OUT,
        Errors.NETWORK_EXCEPTION
    );
    private final int brokerId;
    private final GlobalSequenceCoordinator coordinator;
    private final GlobalSequenceDataReader dataReader;
    private final MetadataCache metadataCache;
    private final AutoTopicCreationManager autoTopicCreationManager;
    private final ListenerName interBrokerListenerName;
    private final Time time;
    private final int requestTimeoutMs;
    private final long retryBackoffMs;
    private final ConcurrentLinkedQueue<PendingOperation<?>> queue = new ConcurrentLinkedQueue<>();
    private final Set<PendingOperation<?>> pendingOperations = ConcurrentHashMap.newKeySet();
    private final SendThread sender;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Object lifecycleLock = new Object();

    public GlobalSequenceIndexRoutingManager(
        int brokerId,
        GlobalSequenceCoordinator coordinator,
        GlobalSequenceDataReader dataReader,
        MetadataCache metadataCache,
        AutoTopicCreationManager autoTopicCreationManager,
        ListenerName interBrokerListenerName,
        KafkaClient networkClient,
        Time time,
        int requestTimeoutMs,
        long retryBackoffMs
    ) {
        this.brokerId = brokerId;
        this.coordinator = coordinator;
        this.dataReader = dataReader;
        this.metadataCache = metadataCache;
        this.autoTopicCreationManager = autoTopicCreationManager;
        this.interBrokerListenerName = interBrokerListenerName;
        this.time = time;
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.sender = new SendThread(networkClient);
    }

    public void start() {
        synchronized (lifecycleLock) {
            if (closed.get()) {
                throw closedException();
            }

            if (started.compareAndSet(false, true)) {
                try {
                    sender.start();
                } catch (Throwable exception) {
                    started.set(false);
                    throw exception;
                }
            }
        }
    }

    public CompletableFuture<GlobalSequenceAppendResult> appendIndex(GlobalSequenceAppendRequest request) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(closedException());
        }

        long createdTimeMs = time.milliseconds();
        PendingAppend pending = new PendingAppend(
            request,
            createdTimeMs,
            createdTimeMs + requestTimeoutMs
        );
        pendingOperations.add(pending);
        queue.add(pending);

        if (closed.get()) {
            queue.remove(pending);
            completeExceptionally(pending, closedException());
        } else {
            sender.wakeup();
        }

        return pending.result;
    }

    public CompletableFuture<GlobalSequenceLookupResult> lookup(GlobalSequenceLookupRequest request) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(closedException());
        }

        long createdTimeMs = time.milliseconds();
        PendingLookup pending = new PendingLookup(
            request,
            createdTimeMs,
            createdTimeMs + requestTimeoutMs
        );
        pendingOperations.add(pending);
        queue.add(pending);

        if (closed.get()) {
            queue.remove(pending);
            completeExceptionally(pending, closedException());
        } else {
            sender.wakeup();
        }

        return pending.result;
    }

    public CompletableFuture<GlobalSequenceFetchResult> fetch(GlobalSequenceFetchRequest request) {
        GlobalSequenceLookupRequest lookupRequest = new GlobalSequenceLookupRequest(
            request.topicId(),
            request.globalStartOffset(),
            request.globalEndOffsetExclusive()
        );
        return lookup(lookupRequest).thenCompose(lookupResult -> {
            List<CompletableFuture<GlobalSequencePhysicalFetchResult>> fetches = lookupResult.indexRecords().stream()
                .map(indexRecord -> fetchPhysical(new GlobalSequencePhysicalFetchRequest(
                    request.topicId(),
                    indexRecord.partitionIndex(),
                    indexRecord.partitionBaseOffset(),
                    indexRecord.recordCount(),
                    request.maxBytes(),
                    request.isolationLevel()
                )))
                .toList();
            return CompletableFuture.allOf(fetches.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignored -> buildFetchResult(request, lookupResult.indexRecords(), fetches));
        });
    }

    public CompletableFuture<GlobalSequencePhysicalFetchResult> readPhysicalLocally(
        GlobalSequencePhysicalFetchRequest request
    ) {
        return dataReader.fetch(request);
    }

    private CompletableFuture<GlobalSequencePhysicalFetchResult> fetchPhysical(
        GlobalSequencePhysicalFetchRequest request
    ) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(closedException());
        }

        long createdTimeMs = time.milliseconds();
        PendingPhysicalFetch pending = new PendingPhysicalFetch(
            request,
            createdTimeMs,
            createdTimeMs + requestTimeoutMs
        );
        pendingOperations.add(pending);
        queue.add(pending);

        if (closed.get()) {
            queue.remove(pending);
            completeExceptionally(pending, closedException());
        } else {
            sender.wakeup();
        }
        return pending.result;
    }

    private static GlobalSequenceFetchResult buildFetchResult(
        GlobalSequenceFetchRequest request,
        List<GlobalSequenceIndexRecord> indexRecords,
        List<CompletableFuture<GlobalSequencePhysicalFetchResult>> fetches
    ) {
        List<GlobalSequenceFetchBatch> batches = new ArrayList<>();
        int remainingBytes = request.maxBytes();
        long nextGlobalOffset = request.globalStartOffset();

        for (int index = 0; index < indexRecords.size(); index++) {
            GlobalSequenceIndexRecord indexRecord = indexRecords.get(index);
            GlobalSequencePhysicalFetchResult physical = fetches.get(index).join();
            int batchSize = physical.records().sizeInBytes();
            if (!batches.isEmpty() && batchSize > remainingBytes) {
                break;
            }

            int firstRecordIndex = Math.toIntExact(Math.max(
                0L,
                request.globalStartOffset() - indexRecord.globalBaseOffset()
            ));
            int lastRecordIndexExclusive = Math.toIntExact(Math.min(
                indexRecord.recordCount(),
                request.globalEndOffsetExclusive() - indexRecord.globalBaseOffset()
            ));
            batches.add(new GlobalSequenceFetchBatch(
                indexRecord.globalBaseOffset(),
                indexRecord.recordCount(),
                firstRecordIndex,
                lastRecordIndexExclusive,
                indexRecord.partitionIndex(),
                indexRecord.partitionBaseOffset(),
                physical.records(),
                physical.abortedTransactions()
            ));
            remainingBytes = Math.max(0, remainingBytes - batchSize);
            nextGlobalOffset = Math.addExact(indexRecord.globalBaseOffset(), lastRecordIndexExclusive);
        }
        return new GlobalSequenceFetchResult(batches, nextGlobalOffset);
    }

    private Collection<RequestAndCompletionHandler> generateRequests() {
        long now = time.milliseconds();
        int queued = queue.size();
        List<RequestAndCompletionHandler> requests = new ArrayList<>(queued);

        for (int i = 0; i < queued; i++) {
            PendingOperation<?> pending = queue.poll();
            if (pending == null) {
                break;
            }
            routePending(pending, now, requests);
        }
        return requests;
    }

    private void routePending(
        PendingOperation<?> pending,
        long now,
        List<RequestAndCompletionHandler> requests
    ) {
        if (pending.result.isDone()) {
            pendingOperations.remove(pending);
            return;
        }
        if (now >= pending.deadlineMs) {
            completeExceptionally(pending, new TimeoutException(
                "Timed out routing a global sequence index operation."
            ));
            return;
        }
        if (now < pending.nextAttemptMs) {
            requeueForBackoff(pending);
            return;
        }

        final Optional<Node> leader;
        try {
            leader = findLeader(pending);
        } catch (Throwable exception) {
            handleFailure(pending, exception);
            return;
        }
        if (leader.isEmpty() || leader.get().isEmpty()) {
            retry(pending);
        } else if (leader.get().id() == brokerId) {
            executeLocally(pending);
        } else {
            requests.add(remoteRequest(leader.get(), pending));
        }
    }

    private void requeueForBackoff(PendingOperation<?> pending) {
        queue.add(pending);
        if (pending.result.isDone()) {
            queue.remove(pending);
            pendingOperations.remove(pending);
        } else if (closed.get()) {
            queue.remove(pending);
            completeExceptionally(pending, closedException());
        }
    }

    private Optional<Node> findLeader(PendingOperation<?> pending) {
        final String topicName;
        final int partitionIndex;
        if (pending instanceof PendingPhysicalFetch physicalFetch) {
            Optional<String> targetTopicName = metadataCache.getTopicName(physicalFetch.topicId());
            if (targetTopicName.isEmpty()) {
                throw Errors.UNKNOWN_TOPIC_ID.exception("Unknown topic ID " + physicalFetch.topicId());
            }
            topicName = targetTopicName.get();
            partitionIndex = physicalFetch.request.partitionIndex();
        } else if (pending instanceof PendingAppend || pending instanceof PendingLookup) {
            if (!metadataCache.contains(Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)) {
                autoTopicCreationManager.createGlobalSequenceIndexTopic();
                return Optional.empty();
            }
            topicName = Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME;
            partitionIndex = coordinator.partitionFor(pending.topicId());
        } else {
            throw new IllegalStateException("Unknown global sequence operation " + pending);
        }

        return metadataCache.getPartitionLeaderEndpoint(topicName, partitionIndex, interBrokerListenerName);
    }

    private void executeLocally(PendingOperation<?> pending) {
        if (pending instanceof PendingAppend append) {
            appendLocally(append);
        } else if (pending instanceof PendingLookup lookup) {
            lookupLocally(lookup);
        } else if (pending instanceof PendingPhysicalFetch physicalFetch) {
            physicalFetchLocally(physicalFetch);
        } else {
            completeExceptionally(pending, new IllegalStateException(
                "Unknown global sequence index operation " + pending
            ));
        }
    }

    private void appendLocally(PendingAppend pending) {
        final CompletableFuture<GlobalSequenceAppendResult> localAppend;
        try {
            localAppend = coordinator.appendIndex(pending.request);
        } catch (Throwable exception) {
            handleFailure(pending, exception);
            return;
        }
        localAppend.whenComplete((result, exception) -> {
            if (exception == null) {
                complete(pending, result);
            } else {
                handleFailure(pending, exception);
            }
        });
    }

    private void lookupLocally(PendingLookup pending) {
        final CompletableFuture<GlobalSequenceLookupResult> localLookup;
        try {
            localLookup = coordinator.lookupIndex(pending.request);
        } catch (Throwable exception) {
            handleFailure(pending, exception);
            return;
        }
        localLookup.whenComplete((result, exception) -> {
            if (exception == null) {
                complete(pending, result);
            } else {
                handleFailure(pending, exception);
            }
        });
    }

    private void physicalFetchLocally(PendingPhysicalFetch pending) {
        final CompletableFuture<GlobalSequencePhysicalFetchResult> localFetch;
        try {
            localFetch = dataReader.fetch(pending.request);
        } catch (Throwable exception) {
            handleFailure(pending, exception);
            return;
        }
        localFetch.whenComplete((result, exception) -> {
            if (exception == null) {
                complete(pending, result);
            } else {
                handleFailure(pending, exception);
            }
        });
    }

    private RequestAndCompletionHandler remoteRequest(Node leader, PendingOperation<?> pending) {
        if (pending instanceof PendingAppend append) {
            return remoteAppendRequest(leader, append);
        } else if (pending instanceof PendingLookup lookup) {
            return remoteLookupRequest(leader, lookup);
        } else if (pending instanceof PendingPhysicalFetch physicalFetch) {
            return remotePhysicalFetchRequest(leader, physicalFetch);
        }
        throw new IllegalStateException("Unknown global sequence index operation " + pending);
    }

    private RequestAndCompletionHandler remoteAppendRequest(Node leader, PendingAppend pending) {
        GlobalSequenceAppendRequest request = pending.request;
        WriteGlobalSequenceIndexRequestData data = new WriteGlobalSequenceIndexRequestData()
            .setTopicId(request.topicId())
            .setPhysicalPartition(request.partitionIndex())
            .setPhysicalBaseOffset(request.partitionBaseOffset())
            .setRecordCount(request.recordCount());

        return new RequestAndCompletionHandler(
            pending.createdTimeMs,
            leader,
            new WriteGlobalSequenceIndexRequest.Builder(data),
            response -> handleRemoteAppendResponse(pending, response)
        );
    }

    private RequestAndCompletionHandler remoteLookupRequest(Node leader, PendingLookup pending) {
        GlobalSequenceLookupRequest request = pending.request;
        LookupGlobalSequenceIndexRequestData data = new LookupGlobalSequenceIndexRequestData()
            .setTopicId(request.topicId())
            .setGlobalStartOffset(request.globalStartOffset())
            .setGlobalEndOffsetExclusive(request.globalEndOffsetExclusive())
            .setInternalRequest(true);

        return new RequestAndCompletionHandler(
            pending.createdTimeMs,
            leader,
            new LookupGlobalSequenceIndexRequest.Builder(data),
            response -> handleRemoteLookupResponse(pending, response)
        );
    }

    private RequestAndCompletionHandler remotePhysicalFetchRequest(Node leader, PendingPhysicalFetch pending) {
        GlobalSequencePhysicalFetchRequest request = pending.request;
        ReadGlobalSequenceDataRequestData data = new ReadGlobalSequenceDataRequestData()
            .setTopicId(request.topicId())
            .setPhysicalPartition(request.partitionIndex())
            .setPhysicalBaseOffset(request.partitionBaseOffset())
            .setRecordCount(request.recordCount())
            .setMaxBytes(request.maxBytes())
            .setIsolationLevel(request.isolationLevel().id());

        return new RequestAndCompletionHandler(
            pending.createdTimeMs,
            leader,
            new ReadGlobalSequenceDataRequest.Builder(data),
            response -> handleRemotePhysicalFetchResponse(pending, response)
        );
    }

    private void handleRemoteAppendResponse(PendingAppend pending, ClientResponse clientResponse) {
        if (clientResponse.authenticationException() != null) {
            completeExceptionally(pending, clientResponse.authenticationException());
        } else if (clientResponse.versionMismatch() != null) {
            completeExceptionally(pending, clientResponse.versionMismatch());
        } else if (!clientResponse.hasResponse()) {
            if (clientResponse.wasTimedOut()) {
                handleFailure(pending, new TimeoutException("The global sequence index request timed out."));
            } else {
                handleFailure(pending, new DisconnectException(
                    "The broker leading the global sequence index partition disconnected."
                ));
            }
        } else if (!(clientResponse.responseBody() instanceof WriteGlobalSequenceIndexResponse response)) {
            completeExceptionally(pending, new IllegalStateException(
                "Unexpected response for a global sequence index append: " + clientResponse.responseBody()
            ));
        } else {
            WriteGlobalSequenceIndexResponseData data = response.data();
            Errors error = Errors.forCode(data.errorCode());
            if (error == Errors.NONE) {
                complete(pending, new GlobalSequenceAppendResult(
                    data.globalBaseOffset(),
                    data.recordCount(),
                    data.duplicateBatch()
                ));
            } else {
                handleFailure(pending, error.exception(data.errorMessage()));
            }
        }
        sender.wakeup();
    }

    private void handleRemoteLookupResponse(PendingLookup pending, ClientResponse clientResponse) {
        if (clientResponse.authenticationException() != null) {
            completeExceptionally(pending, clientResponse.authenticationException());
        } else if (clientResponse.versionMismatch() != null) {
            completeExceptionally(pending, clientResponse.versionMismatch());
        } else if (!clientResponse.hasResponse()) {
            if (clientResponse.wasTimedOut()) {
                handleFailure(pending, new TimeoutException("The global sequence index lookup timed out."));
            } else {
                handleFailure(pending, new DisconnectException(
                    "The broker leading the global sequence index partition disconnected."
                ));
            }
        } else if (!(clientResponse.responseBody() instanceof LookupGlobalSequenceIndexResponse response)) {
            completeExceptionally(pending, new IllegalStateException(
                "Unexpected response for a global sequence index lookup: " + clientResponse.responseBody()
            ));
        } else {
            LookupGlobalSequenceIndexResponseData data = response.data();
            Errors error = Errors.forCode(data.errorCode());
            if (error == Errors.NONE) {
                List<GlobalSequenceIndexRecord> records = data.indexEntries().stream()
                    .map(entry -> new GlobalSequenceIndexRecord(
                        pending.topicId(),
                        entry.globalBaseOffset(),
                        entry.recordCount(),
                        entry.physicalPartition(),
                        entry.physicalBaseOffset()
                    ))
                    .toList();
                complete(pending, new GlobalSequenceLookupResult(records));
            } else {
                handleFailure(pending, error.exception(data.errorMessage()));
            }
        }
        sender.wakeup();
    }

    private void handleRemotePhysicalFetchResponse(PendingPhysicalFetch pending, ClientResponse clientResponse) {
        if (clientResponse.authenticationException() != null) {
            completeExceptionally(pending, clientResponse.authenticationException());
        } else if (clientResponse.versionMismatch() != null) {
            completeExceptionally(pending, clientResponse.versionMismatch());
        } else if (!clientResponse.hasResponse()) {
            if (clientResponse.wasTimedOut()) {
                handleFailure(pending, new TimeoutException("The global sequence data read timed out."));
            } else {
                handleFailure(pending, new DisconnectException(
                    "The broker leading the physical data partition disconnected."
                ));
            }
        } else if (!(clientResponse.responseBody() instanceof ReadGlobalSequenceDataResponse response)) {
            completeExceptionally(pending, new IllegalStateException(
                "Unexpected response for a global sequence data read: " + clientResponse.responseBody()
            ));
        } else {
            ReadGlobalSequenceDataResponseData data = response.data();
            Errors error = Errors.forCode(data.errorCode());
            if (error == Errors.NONE) {
                if (!(data.records() instanceof MemoryRecords records)) {
                    completeExceptionally(pending, new IllegalStateException(
                        "Global sequence data read did not return memory records"
                    ));
                } else {
                    List<GlobalSequenceAbortedTransaction> abortedTransactions = data.abortedTransactions() == null ?
                        List.of() : data.abortedTransactions().stream()
                            .map(transaction -> new GlobalSequenceAbortedTransaction(
                                transaction.producerId(),
                                transaction.firstOffset()
                            ))
                            .toList();
                    complete(pending, new GlobalSequencePhysicalFetchResult(records, abortedTransactions));
                }
            } else {
                handleFailure(pending, error.exception(data.errorMessage()));
            }
        }
        sender.wakeup();
    }

    private void handleFailure(PendingOperation<?> pending, Throwable exception) {
        if (pending.result.isDone()) {
            pendingOperations.remove(pending);
            return;
        }
        Throwable cause = unwrapCompletionException(exception);
        if (isRetriable(Errors.forException(cause)) && time.milliseconds() < pending.deadlineMs) {
            retry(pending);
        } else {
            completeExceptionally(pending, cause);
        }
    }

    private void retry(PendingOperation<?> pending) {
        if (pending.result.isDone()) {
            pendingOperations.remove(pending);
            return;
        } else if (closed.get()) {
            completeExceptionally(pending, closedException());
            return;
        }

        pending.nextAttemptMs = time.milliseconds() + retryBackoffMs;
        queue.add(pending);

        if (closed.get()) {
            queue.remove(pending);
            completeExceptionally(pending, closedException());
        } else {
            sender.wakeup();
        }
    }

    private static IllegalStateException closedException() {
        return new IllegalStateException(
            "The global sequence index routing manager is closed."
        );
    }

    private static boolean isRetriable(Errors error) {
        return RETRIABLE_ERRORS.contains(error);
    }

    private static Throwable unwrapCompletionException(Throwable exception) {
        if (exception.getCause() != null &&
            (exception instanceof java.util.concurrent.CompletionException ||
                exception instanceof java.util.concurrent.ExecutionException)) {
            return exception.getCause();
        }
        return exception;
    }

    private <T> void complete(PendingOperation<T> pending, T result) {
        pendingOperations.remove(pending);
        pending.result.complete(result);
    }

    private void completeExceptionally(PendingOperation<?> pending, Throwable exception) {
        pendingOperations.remove(pending);
        pending.result.completeExceptionally(exception);
    }

    Collection<RequestAndCompletionHandler> generateRequestsForTest() {
        return generateRequests();
    }

    @Override
    public void close() throws InterruptedException {
        final boolean needShutdownSender;

        synchronized (lifecycleLock) {
            if (!closed.compareAndSet(false, true)) {
                return;
            }

            needShutdownSender = started.compareAndSet(true, false);
        }
        IllegalStateException exception = closedException();
        pendingOperations.forEach(pending -> pending.result.completeExceptionally(exception));
        pendingOperations.clear();
        queue.clear();

        if (needShutdownSender) {
            sender.shutdown();
        }
    }

    private abstract static class PendingOperation<T> {
        final long createdTimeMs;
        final long deadlineMs;
        final CompletableFuture<T> result = new CompletableFuture<>();
        volatile long nextAttemptMs;

        private PendingOperation(long createdTimeMs, long deadlineMs) {
            this.createdTimeMs = createdTimeMs;
            this.deadlineMs = deadlineMs;
            this.nextAttemptMs = 0L;
        }

        abstract Uuid topicId();
    }

    private static final class PendingAppend extends PendingOperation<GlobalSequenceAppendResult> {
        private final GlobalSequenceAppendRequest request;

        private PendingAppend(GlobalSequenceAppendRequest request, long createdTimeMs, long deadlineMs) {
            super(createdTimeMs, deadlineMs);
            this.request = request;
        }

        @Override
        Uuid topicId() {
            return request.topicId();
        }
    }

    private static final class PendingLookup extends PendingOperation<GlobalSequenceLookupResult> {
        private final GlobalSequenceLookupRequest request;

        private PendingLookup(GlobalSequenceLookupRequest request, long createdTimeMs, long deadlineMs) {
            super(createdTimeMs, deadlineMs);
            this.request = request;
        }

        @Override
        Uuid topicId() {
            return request.topicId();
        }
    }

    private static final class PendingPhysicalFetch extends PendingOperation<GlobalSequencePhysicalFetchResult> {
        private final GlobalSequencePhysicalFetchRequest request;

        private PendingPhysicalFetch(
            GlobalSequencePhysicalFetchRequest request,
            long createdTimeMs,
            long deadlineMs
        ) {
            super(createdTimeMs, deadlineMs);
            this.request = request;
        }

        @Override
        Uuid topicId() {
            return request.topicId();
        }
    }

    private final class SendThread extends InterBrokerSendThread {
        private SendThread(KafkaClient networkClient) {
            super(
                "GlobalSequenceIndexSender-" + brokerId,
                networkClient,
                requestTimeoutMs,
                time,
                true
            );
        }

        @Override
        public Collection<RequestAndCompletionHandler> generateRequests() {
            return GlobalSequenceIndexRoutingManager.this.generateRequests();
        }

        @Override
        public void doWork() {
            pollOnce(queue.isEmpty() ? Long.MAX_VALUE : Math.max(1L, retryBackoffMs));
        }
    }
}
