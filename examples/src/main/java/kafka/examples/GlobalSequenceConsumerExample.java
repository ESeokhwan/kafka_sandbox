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
package kafka.examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GlobalSequenceConsumer;
import org.apache.kafka.clients.consumer.GlobalSequenceConsumerRecord;
import org.apache.kafka.clients.consumer.GlobalSequenceConsumerRecords;
import org.apache.kafka.clients.consumer.GlobalSequenceConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaGlobalSequenceConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Objects;
import java.util.Properties;

/** Reads a finite global offset range without joining a consumer group or committing offsets. */
public final class GlobalSequenceConsumerExample {
    private static final Duration FETCH_TIMEOUT = Duration.ofSeconds(10);
    private static final int DEFAULT_MAX_BATCHES = 2;

    private GlobalSequenceConsumerExample() { }

    public static void main(String[] args) throws IOException {
        if (args.length < 4 || args.length > 5) {
            throw new IllegalArgumentException("Usage: GlobalSequenceConsumerExample <bootstrap> <topic-name> " +
                "<start> <end-exclusive> [client.properties]");
        }
        Properties properties = new Properties();
        if (args.length == 5) {
            try (InputStream input = Files.newInputStream(Path.of(args[4]))) {
                properties.load(input);
            }
        }
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        read(properties, args[1], Long.parseLong(args[2]), Long.parseLong(args[3]), System.out);
    }

    /**
     * Print records and page cursors for a finite half-open global offset range.
     * The caller is responsible for durably coupling record processing with cursor storage.
     */
    public static void read(Properties properties, String topic, long start, long end, PrintStream out) {
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(out, "out");
        if (topic.isEmpty() || start < 0 || end < start)
            throw new IllegalArgumentException("Require a nonempty topic and 0 <= start <= end-exclusive");

        Properties consumerProperties = new Properties();
        consumerProperties.putAll(properties);
        consumerProperties.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, "global-sequence-consumer-example");
        consumerProperties.putIfAbsent(GlobalSequenceConsumerConfig.FETCH_MAX_BATCHES_CONFIG,
            Integer.toString(DEFAULT_MAX_BATCHES));
        consumerProperties.put(GlobalSequenceConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class);
        consumerProperties.put(GlobalSequenceConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class);

        try (KafkaGlobalSequenceConsumer<byte[], byte[]> consumer =
                 new KafkaGlobalSequenceConsumer<>(consumerProperties)) {
            readPages(consumer, topic, start, end, out);
        }
    }

    private static void readPages(
        GlobalSequenceConsumer<byte[], byte[]> consumer,
        String topic,
        long start,
        long requestedEnd,
        PrintStream out
    ) {
        long cursor = start;
        long limit = requestedEnd;
        Uuid topicId = null;
        boolean firstPage = true;
        do {
            GlobalSequenceConsumerRecords<byte[], byte[]> page = firstPage ?
                consumer.fetch(topic, cursor, limit, FETCH_TIMEOUT) :
                consumer.fetch(topic, topicId, cursor, limit, FETCH_TIMEOUT);
            for (GlobalSequenceConsumerRecord<byte[], byte[]> record : page) {
                out.printf("record\tglobalOffset=%d\tpartition=%d\tphysicalOffset=%d\ttimestamp=%d\t" +
                        "timestampType=%s\tleaderEpoch=%s\tkey=%s\tvalue=%s%n",
                    record.globalOffset(), record.physicalPartition(), record.physicalOffset(), record.timestamp(),
                    record.timestampType(), record.leaderEpoch().map(String::valueOf).orElse("none"),
                    encoded(record.key()), encoded(record.value()));
            }
            out.printf("page\ttopicId=%s\tnext=%d\tcommittedEnd=%d\tpending=%s\terror=%s%n",
                page.topicId(), page.nextGlobalOffset(), page.committedGlobalEndOffset(),
                page.transactionPending(), page.error().map(error -> error.getClass().getSimpleName()).orElse("NONE"));

            if (firstPage) {
                topicId = page.topicId();
                limit = Math.min(requestedEnd, page.committedGlobalEndOffset());
                firstPage = false;
            }
            if (page.error().isPresent()) {
                throw new KafkaException("Global sequence fetch stopped after the prefix ending at cursor " +
                    page.nextGlobalOffset(), page.error().get());
            }
            if (page.transactionPending() || page.nextGlobalOffset() >= limit)
                return;
            if (page.nextGlobalOffset() <= cursor)
                throw new KafkaException("Global sequence fetch did not advance at cursor " + cursor);
            cursor = page.nextGlobalOffset();
        } while (true);
    }

    private static String encoded(byte[] value) {
        return value == null ? "null" : Base64.getEncoder().encodeToString(value);
    }
}
