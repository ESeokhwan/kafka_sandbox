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
package kafka.examples.globalsequence;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.DefaultHostResolver;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchGlobalSequenceRequestData;
import org.apache.kafka.common.message.LookupGlobalSequenceRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchGlobalSequenceRequest;
import org.apache.kafka.common.requests.FetchGlobalSequenceResponse;
import org.apache.kafka.common.requests.LookupGlobalSequenceRequest;
import org.apache.kafka.common.requests.LookupGlobalSequenceResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

/** One-shot raw-protocol example, requiring this branch's clients and brokers. Not a consumer group client. */
public final class GlobalSequenceReadDemo {
    private static final int TIMEOUT_MS = 10000;
    // Keep pages small so the example demonstrates cursor continuation even for a short topic.
    private static final int MAX_BATCHES = 2;

    private GlobalSequenceReadDemo() { }

    public static void main(String[] args) throws Exception {
        if (args.length < 5 || args.length > 6) {
            throw new IllegalArgumentException("Usage: GlobalSequenceReadDemo <bootstrap> <topic-uuid> <start> " +
                "<end-exclusive> <lookup|read_uncommitted|read_committed> [client.properties]");
        }
        Properties properties = new Properties();
        if (args.length == 6) {
            try (InputStream input = Files.newInputStream(Path.of(args[5]))) {
                properties.load(input);
            }
        }
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        read(properties, Uuid.fromString(args[1]), Long.parseLong(args[2]), Long.parseLong(args[3]), args[4], System.out);
    }

    /** Prints a finite committed prefix and its resume cursor; callers persist the cursor after processing the output. */
    public static void read(Properties properties, Uuid topicId, long start, long end, String mode, PrintStream out) throws IOException {
        if (start < 0 || end <= start || topicId.equals(Uuid.ZERO_UUID))
            throw new IllegalArgumentException("Require a nonzero topic UUID and 0 <= start < end-exclusive");
        if (!List.of("lookup", "read_uncommitted", "read_committed").contains(mode))
            throw new IllegalArgumentException("Unknown mode: " + mode);
        AdminClientConfig config = new AdminClientConfig(properties);
        List<Node> nodes = new ArrayList<>();
        for (InetSocketAddress address : ClientUtils.parseAndValidateAddresses(config))
            nodes.add(new Node(nodes.size(), address.getHostString(), address.getPort()));
        try (Metrics metrics = new Metrics();
             NetworkClient client = ClientUtils.createNetworkClient(config, "global-sequence-demo", metrics,
                 "global-sequence-demo", new LogContext(), new ApiVersions(), Time.SYSTEM, 1,
                 config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG), new ManualMetadataUpdater(nodes), new DefaultHostResolver())) {
            readPages(client, connect(client, nodes), topicId, start, end, mode, out);
        }
    }

    private record Page(long next, long committedEnd, boolean pending, Errors error) { }

    private static void readPages(NetworkClient client, Node node, Uuid topicId, long start, long end,
                                  String mode, PrintStream out) throws IOException {
        long cursor = start;
        long limit = end;
        boolean firstPage = true;
        while (cursor < limit) {
            Page page = mode.equals("lookup") ? lookup(client, node, topicId, cursor, limit, out) :
                fetch(client, node, topicId, cursor, limit, mode.equals("read_committed"), out);
            // Early request errors have no cursor; they cannot invalidate already processed earlier pages.
            long next = page.next < 0 ? cursor : page.next;
            out.printf("page\tnext=%d\tcommittedEnd=%d\tpending=%s\terror=%s%n", next, page.committedEnd, page.pending, page.error);
            if (page.error != Errors.NONE)
                throw new KafkaException("Global read failed at cursor " + next + ": " + page.error);
            if (firstPage) {
                limit = Math.min(limit, page.committedEnd);
                firstPage = false;
            }
            if (page.pending || next >= limit) return;
            if (next <= cursor) throw new KafkaException("Global read did not advance at cursor " + cursor);
            cursor = next;
        }
    }

    private static Page lookup(NetworkClient client, Node node, Uuid topicId, long start, long end, PrintStream out) throws IOException {
        LookupGlobalSequenceResponse response = (LookupGlobalSequenceResponse) send(client, node,
            new LookupGlobalSequenceRequest.Builder(new LookupGlobalSequenceRequestData().setTopicId(topicId)
                .setGlobalStartOffset(start).setGlobalEndOffsetExclusive(end).setMaxBatches(MAX_BATCHES).setTimeoutMs(TIMEOUT_MS)));
        var page = response.data();
        for (var batch : page.batches()) {
            out.printf("mapping\t%d\t%d\t%d\t%d\t%d\t%d%n", batch.globalBaseOffset(), batch.physicalPartition(),
                batch.physicalBaseOffset(), batch.physicalLastOffset(), batch.selectedGlobalStartOffset(), batch.selectedGlobalEndOffset());
        }
        return new Page(page.nextGlobalOffset(), page.committedGlobalEndOffset(), false, Errors.forCode(page.errorCode()));
    }

    private static Page fetch(NetworkClient client, Node node, Uuid topicId, long start, long end,
                               boolean committed, PrintStream out) throws IOException {
        FetchGlobalSequenceResponse response = (FetchGlobalSequenceResponse) send(client, node,
            new FetchGlobalSequenceRequest.Builder(new FetchGlobalSequenceRequestData().setTopicId(topicId)
                .setGlobalStartOffset(start).setGlobalEndOffsetExclusive(end).setMaxBatches(MAX_BATCHES).setMaxBytes(1024 * 1024)
                .setIsolationLevel(committed ? (byte) 1 : (byte) 0).setTimeoutMs(TIMEOUT_MS)));
        var page = response.data();
        for (var entry : page.batches()) {
            for (RecordBatch batch : ((MemoryRecords) entry.records()).batches()) {
                batch.ensureValid();
                for (Record record : batch) {
                    long global = entry.globalBaseOffset() + record.offset() - entry.physicalBaseOffset();
                    if (global >= entry.selectedGlobalStartOffset() && global < entry.selectedGlobalEndOffset())
                        out.printf("record\t%d\t%d\t%d\t%s%n", global, entry.physicalPartition(), record.offset(), encoded(record.value()));
                }
            }
        }
        return new Page(page.nextGlobalOffset(), page.committedGlobalEndOffset(), page.transactionPending(), Errors.forCode(page.errorCode()));
    }

    private static String encoded(ByteBuffer value) {
        return value == null ? "null" : Base64.getEncoder().encodeToString(Utils.toArray(value));
    }

    private static Node connect(NetworkClient client, List<Node> nodes) throws IOException {
        IOException failure = new IOException("No reachable bootstrap broker");
        for (Node node : nodes) {
            try {
                if (NetworkClientUtils.awaitReady(client, node, Time.SYSTEM, TIMEOUT_MS)) return node;
            } catch (IOException e) {
                failure.addSuppressed(e);
            }
        }
        throw failure;
    }

    private static AbstractResponse send(NetworkClient client, Node node, AbstractRequest.Builder<?> request) throws IOException {
        if (!NetworkClientUtils.awaitReady(client, node, Time.SYSTEM, TIMEOUT_MS))
            throw new IOException("Broker not ready: " + node);
        return NetworkClientUtils.sendAndReceive(client,
            client.newClientRequest(node.idString(), request, Time.SYSTEM.milliseconds(), true), Time.SYSTEM).responseBody();
    }
}
