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
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class BasicTransientTopicPartitionPool implements TransientTopicPartitionPool {

    private final Logger log;

    public BasicTransientTopicPartitionPool(LogContext logContext) {
        this.log = logContext.logger(BasicTransientTopicPartitionPool.class);
    }

    public void startup(int numPartitions) {
    }

    public TransientTopicPartition allocatePartition() {
        // TODO: implement here
        return null;
    }

    public void releasePartition(int partition, int usedOffset) {
        // TODO: implement here
    }
}
