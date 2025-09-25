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
package org.apache.kafka.coordinator.transienttopic;

import org.apache.kafka.common.TransientTopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BasicTransientTopicPartitionPool implements TransientTopicPartitionPool {

    private final Logger log;

    private int numPartitions;

    private final Queue<TransientTopicPartition> freePartitions = new ConcurrentLinkedQueue<>();
    private final Map<Integer, TransientTopicPartition> usingPartitions = new ConcurrentHashMap<>();

    public BasicTransientTopicPartitionPool(LogContext logContext) {
        this.log = logContext.logger(BasicTransientTopicPartitionPool.class);
    }

    public void startup(int numPartitions) {
        log.info("BasicTransientTopicPartitionPool is used for transient topic partition pool(the number of partitions: {}).",
            numPartitions);
        this.numPartitions = numPartitions;
        for (int i = 0; i < numPartitions; i++) {
            // TODO-1: set offset based on real last offsets
            // TODO-1: create partitions if there are less partitions than given num partitions.
            freePartitions.add(new TransientTopicPartition(Topic.TRANSIENT_TOPIC_NAME, i, 0L));
        }
    }

    public TransientTopicPartition allocatePartition() {
        TransientTopicPartition freePartition = freePartitions.poll();
        if (freePartition == null) throw new RuntimeException(); // TODO-2: handle exception
        usingPartitions.put(freePartition.partition(), freePartition);
        return freePartition;
    }

    public void releasePartition(int partition, long usedOffset) {
        TransientTopicPartition target = usingPartitions.remove(partition);
        if (target == null) throw new RuntimeException(); // TODO-2: handle exception

        long updatedOffset = target.offset() + usedOffset;
        freePartitions.add(target.withOffset(updatedOffset));
    }
}
