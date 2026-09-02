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
package org.apache.kafka.coordinator.globalsequence;

import org.apache.kafka.common.record.MemoryRecords;

import java.util.List;
import java.util.Objects;

/**
 * One physical batch in global offset order.
 *
 * <p>The records retain their physical Kafka offsets. The half-open record-index range identifies
 * which records in the batch belong to the requested global range.</p>
 */
public record GlobalSequenceFetchBatch(
    long globalBaseOffset,
    int recordCount,
    int firstRecordIndex,
    int lastRecordIndexExclusive,
    int partitionIndex,
    long partitionBaseOffset,
    MemoryRecords records,
    List<GlobalSequenceAbortedTransaction> abortedTransactions
) {
    public GlobalSequenceFetchBatch {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(abortedTransactions, "abortedTransactions");
        if (firstRecordIndex < 0 || firstRecordIndex >= recordCount) {
            throw new IllegalArgumentException("firstRecordIndex must identify a record in the batch");
        }
        if (lastRecordIndexExclusive <= firstRecordIndex || lastRecordIndexExclusive > recordCount) {
            throw new IllegalArgumentException("lastRecordIndexExclusive must define a non-empty batch slice");
        }
        abortedTransactions = List.copyOf(abortedTransactions);
    }
}
