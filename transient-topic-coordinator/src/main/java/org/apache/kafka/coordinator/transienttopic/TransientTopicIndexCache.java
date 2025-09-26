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

import org.apache.kafka.common.TransientTopic;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class TransientTopicIndexCache {

    private final Logger log;

    private final Map<String, IndexCacheEntry> indexMap = new ConcurrentHashMap<>();

    public TransientTopicIndexCache(LogContext logContext) {
        this.log = logContext.logger(TransientTopicIndexCache.class);
    }

    public void startup() {}

    public IndexCacheEntry getIndex(String topicName) {
        return indexMap.get(topicName);
    }

    public boolean contains(String topicName) {
        return indexMap.containsKey(topicName);
    }

    public void addIndexToCache(TransientTopic transientTopic) {
        String topicName = transientTopic.name();
        if (indexMap.containsKey(topicName)) {
            return;
        }
        indexMap.put(topicName, new IndexCacheEntry(transientTopic, transientTopic.partition().offset()));
    }

    // TODO: when this method are being executed, evict Index From Cache should be blocked.
    public void updateIndexLastOffset(String topicName, long lastOffset) {
        indexMap.computeIfPresent(topicName, (k, v) -> {
            if (lastOffset <= v.curOffset()) return v;
            return v.withCurOffset(lastOffset);
        });
    }

    public IndexCacheEntry evictIndexFromCache(String topicName) {
        if (!indexMap.containsKey(topicName)) {
            // TODO: add Exception Logic
            return null;
        }

        return indexMap.remove(topicName);
    }

    public static class IndexCacheEntry {
        private final TransientTopic transientTopic;
        private final long curOffset;

        private IndexCacheEntry(TransientTopic transientTopic, long curOffset) {
            this.transientTopic = Objects.requireNonNull(transientTopic);
            this.curOffset = curOffset;
        }

        public TransientTopic transientTopic() {
            return transientTopic;
        }

        public long curOffset() {
            return curOffset;
        }

        public IndexCacheEntry withCurOffset(long curOffset) {
            return new IndexCacheEntry(this.transientTopic, curOffset);
        }
    }
}
