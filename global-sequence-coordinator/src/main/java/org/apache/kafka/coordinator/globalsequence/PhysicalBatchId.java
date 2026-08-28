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

import java.util.Objects;

/**
 * Identifies one record batch at its final location in a data partition.
 *
 * <p>The record count is intentionally not part of the identity. It is validated against an
 * existing allocation so that conflicting descriptions of the same physical batch cannot receive
 * multiple global ranges.</p>
 */
record PhysicalBatchId(
    Uuid topicId,
    int partitionIndex,
    long partitionBaseOffset
) {
    PhysicalBatchId {
        validate(topicId, partitionIndex, partitionBaseOffset);
    }

    static void validate(Uuid topicId, int partitionIndex, long partitionBaseOffset) {
        Objects.requireNonNull(topicId, "topicId");
        if (Uuid.ZERO_UUID.equals(topicId)) {
            throw new IllegalArgumentException("topicId must not be ZERO_UUID");
        }
        if (partitionIndex < 0) {
            throw new IllegalArgumentException("partitionIndex must not be negative");
        }
        if (partitionBaseOffset < 0) {
            throw new IllegalArgumentException("partitionBaseOffset must not be negative");
        }
    }
}
