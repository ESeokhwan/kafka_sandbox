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
 * Describes one record batch after it has been appended to a data partition.
 * Producer idempotence is validated by the produce path before this request is created.
 *
 * @param topicId the controller-assigned ID of the data topic
 * @param partitionIndex the data partition containing the batch
 * @param partitionBaseOffset the batch's base offset in the data partition
 * @param recordCount the number of records covered by the batch
 */
public record GlobalSequenceAppendRequest(
    Uuid topicId,
    int partitionIndex,
    long partitionBaseOffset,
    int recordCount
) {
    public GlobalSequenceAppendRequest {
        validate(topicId, partitionIndex, partitionBaseOffset, recordCount);
    }

    static void validate(
        Uuid topicId,
        int partitionIndex,
        long partitionBaseOffset,
        int recordCount
    ) {
        PhysicalBatchId.validate(topicId, partitionIndex, partitionBaseOffset);
        if (recordCount <= 0) {
            throw new IllegalArgumentException("recordCount must be positive");
        }
    }

    PhysicalBatchId physicalBatchId() {
        return new PhysicalBatchId(topicId, partitionIndex, partitionBaseOffset);
    }
}
