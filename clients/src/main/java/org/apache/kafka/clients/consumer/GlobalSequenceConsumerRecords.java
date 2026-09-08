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

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * One page returned by a global sequence fetch.
 */
public final class GlobalSequenceConsumerRecords<K, V> implements Iterable<GlobalSequenceConsumerRecord<K, V>> {
    private final List<GlobalSequenceConsumerRecord<K, V>> records;
    private final long nextGlobalOffset;

    public GlobalSequenceConsumerRecords(List<GlobalSequenceConsumerRecord<K, V>> records, long nextGlobalOffset) {
        this.records = List.copyOf(Objects.requireNonNull(records, "records"));
        this.nextGlobalOffset = nextGlobalOffset;
    }

    /**
     * Records returned by this page in global offset order.
     */
    public List<GlobalSequenceConsumerRecord<K, V>> records() {
        return records;
    }

    /**
     * The global offset from which the next page should start.
     */
    public long nextGlobalOffset() {
        return nextGlobalOffset;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

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
            ')';
    }
}
