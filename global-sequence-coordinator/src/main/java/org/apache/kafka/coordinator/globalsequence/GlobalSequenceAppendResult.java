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

/**
 * The global range allocated to one physical record batch.
 *
 * @param globalBaseOffset the first global offset allocated to the batch
 * @param recordCount the size of the allocated global range
 * @param duplicate whether an existing index allocation was returned without writing a new record
 */
public record GlobalSequenceAppendResult(
    long globalBaseOffset,
    int recordCount,
    boolean duplicate
) {
    public GlobalSequenceAppendResult {
        if (globalBaseOffset < 0) {
            throw new IllegalArgumentException("globalBaseOffset must not be negative");
        }
        if (recordCount <= 0) {
            throw new IllegalArgumentException("recordCount must be positive");
        }
    }
}
