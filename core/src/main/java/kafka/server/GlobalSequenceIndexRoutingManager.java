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
import org.apache.kafka.common.message.WriteGlobalSequenceIndexRequestData;
import org.apache.kafka.common.message.WriteGlobalSequenceIndexResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.LookupGlobalSequenceIndexRequest;
import org.apache.kafka.common.requests.LookupGlobalSequenceIndexResponse;
import org.apache.kafka.common.requests.WriteGlobalSequenceIndexRequest;
import org.apache.kafka.common.requests.WriteGlobalSequenceIndexResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceAppendRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceAppendResult;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinator;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceIndexRecord;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookupRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookupResult;
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

    private Collection<RequestAndCompletionHandler> generateRequests() {
        long now = time.milliseconds();
        int queued = queue.size();
        List<RequestAndCompletionHandler> requests = new ArrayList<>(queued);

        for (int i = 0; i < queued; i++) {
            PendingOperation<?> pending = queue.poll();
            if (pending == null) {
                break;
            }
            if (pending.result.isDone()) {
                pendingOperations.remove(pending);
                continue;
            }
            if (now >= pending.deadlineMs) {
                completeExceptionally(pending, new TimeoutException(
                    "Timed out routing a global sequence index operation."
                ));
                continue;
            }
            if (now < pending.nextAttemptMs) {
                requeueForBackoff(pending);
                continue;
            }

            if (!metadataCache.contains(Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)) {
                autoTopicCreationManager.createGlobalSequenceIndexTopic();
                retry(pending);
                continue;
            }

            final Optional<Node> leader;
            try {
                leader = metadataCache.getPartitionLeaderEndpoint(
                    Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME,
                    coordinator.partitionFor(pending.topicId()),
                    interBrokerListenerName
                );
            } catch (Throwable exception) {
                handleFailure(pending, exception);
                continue;
            }
            if (leader.isEmpty() || leader.get().isEmpty()) {
                retry(pending);
            } else if (leader.get().id() == brokerId) {
                executeLocally(pending);
            } else {
                requests.add(remoteRequest(leader.get(), pending));
            }
        }
        return requests;
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

    private void executeLocally(PendingOperation<?> pending) {
        if (pending instanceof PendingAppend append) {
            appendLocally(append);
        } else if (pending instanceof PendingLookup lookup) {
            lookupLocally(lookup);
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

    private RequestAndCompletionHandler remoteRequest(Node leader, PendingOperation<?> pending) {
        if (pending instanceof PendingAppend append) {
            return remoteAppendRequest(leader, append);
        } else if (pending instanceof PendingLookup lookup) {
            return remoteLookupRequest(leader, lookup);
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
