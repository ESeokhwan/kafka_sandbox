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

import org.apache.kafka.common.Uuid;

/**
 * A committed mapping from one global range to one physical record batch.
 */
public record GlobalSequenceIndexRecord(
    Uuid topicId,
    long globalBaseOffset,
    int recordCount,
    int partitionIndex,
    long partitionBaseOffset
) {
    public GlobalSequenceIndexRecord {
        GlobalSequenceAppendRequest.validate(
            topicId,
            partitionIndex,
            partitionBaseOffset,
            recordCount
        );
        if (globalBaseOffset < 0) {
            throw new IllegalArgumentException("globalBaseOffset must not be negative");
        }
        validateGlobalRange(globalBaseOffset, recordCount);
    }

    public GlobalSequenceAppendResult toAppendResult(boolean duplicate) {
        return new GlobalSequenceAppendResult(globalBaseOffset, recordCount, duplicate);
    }

    public long globalEndOffsetExclusive() {
        return endOffsetExclusive(globalBaseOffset, recordCount);
    }

    private static void validateGlobalRange(long globalBaseOffset, int recordCount) {
        endOffsetExclusive(globalBaseOffset, recordCount);
    }

    static long endOffsetExclusive(long globalBaseOffset, int recordCount) {
        if (globalBaseOffset < 0) {
            throw new IllegalArgumentException("globalBaseOffset must not be negative");
        }
        if (recordCount <= 0) {
            throw new IllegalArgumentException("recordCount must be positive");
        }
        return Math.addExact(globalBaseOffset, recordCount);
    }
}
