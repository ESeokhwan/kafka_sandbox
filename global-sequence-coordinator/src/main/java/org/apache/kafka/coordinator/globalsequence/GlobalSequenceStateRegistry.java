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
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GlobalSequenceStateRegistry {

    private final Logger log;

    private final Map<Uuid, GlobalSequenceState> stateMap = new ConcurrentHashMap<>();

    public GlobalSequenceStateRegistry(LogContext logContext) {
        this.log = logContext.logger(GlobalSequenceStateRegistry.class);
    }

    public void startup() {}

    public GlobalSequenceState getState(Uuid topicId) {
        return stateMap.get(topicId);
    }

    public boolean contains(Uuid topicId) {
        return stateMap.containsKey(topicId);
    }

    public void createNewTopicState(Uuid topicId) {
        if (stateMap.containsKey(topicId)) {
            return;
        }
        stateMap.put(topicId, new GlobalSequenceState(
                new ConcurrentLinkedQueue<>(),
                new BasicGlobalOffsetSequencer() // TODO: implement it first
        ));
    }

    public GlobalSequenceIndexRecord addSequenceIndex(GlobalSequenceAppendRequest request) {
        GlobalSequenceState topicState = stateMap.get(request.topicId());
        if (topicState == null) return null;

        return topicState.addSequenceIndex(request);
    }

    public GlobalSequenceState evictIndexTableFromCache(Uuid topicId) {
        if (!stateMap.containsKey(topicId)) {
            // TODO: add Exception Logic
            return null;
        }

        return stateMap.remove(topicId);
    }

    public static class GlobalSequenceState {
        private final Queue<GlobalSequenceIndexRecord> sequenceIndex;
        private final GlobalOffsetSequencer offsetSequencer;

        public GlobalSequenceState(Queue<GlobalSequenceIndexRecord> sequenceIndex, GlobalOffsetSequencer offsetSequencer) {
            this.sequenceIndex = sequenceIndex;
            this.offsetSequencer = offsetSequencer;
        }

        public GlobalSequenceIndexRecord[] getSequenceIndexAsArray() {
            return sequenceIndex.toArray(new GlobalSequenceIndexRecord[0]);
        }

        public GlobalSequenceIndexRecord addSequenceIndex(GlobalSequenceAppendRequest request) {
            GlobalSequenceIndexRecord newIndexRecord = new GlobalSequenceIndexRecord(
                request.topicId(),
                offsetSequencer.nextOffset(),
                request.recordCount(),
                request.partitionIndex(),
                request.partitionBaseOffset()
            );
            sequenceIndex.add(newIndexRecord);
            return newIndexRecord;
        }
    }
}
