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

/*
import org.apache.kafka.common.TransientTopic;
import org.apache.kafka.common.TransientTopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apahce.kafka.coordinator.transienttopic.TransientTopicIndexCache;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
 */

public class TransientTopicIndexCacheTest {
/*
    @Test
    public void testAddIndexToCache() {
        String WRONG_TOPIC = "__WRONG_TOPIC";

        TransientTopicPartition dummyPartition1 = new TransientTopicPartition(
            Topic.TRANSIENT_TOPIC_INDEX_TOPIC_NAME, 0, 0
        );

        TransientTopicPartition dummyPartition2 = new TransientTopicPartition(
            Topic.TRANSIENT_TOPIC_INDEX_TOPIC_NAME, 0, 100
        );

        TransientTopicPartition dummyPartition3 = new TransientTopicPartition(
            Topic.TRANSIENT_TOPIC_INDEX_TOPIC_NAME, 1, 100
        );

        TransientTopicPartition dummyPartition4 = new TransientTopicPartition(
            Topic.TRANSIENT_TOPIC_INDEX_TOPIC_NAME, 2, 100
        );

        TransientTopicPartition wrongTopicNameDummyPartition1 = new TransientTopicPartition(
            WRONG_TOPIC, 0, 0
        );

        String dummyTransientTopicName1 = "dummy_topic1";
        String dummyTransientTopicName2 = "dummy_topic2";
        String wrongTransientTopicName = "wrong_topic";
        TransientTopic dummyTransientTopic1 = new TransientTopic(dummyTransientTopicName1, dummyPartition1);
        TransientTopic dummyTransientTopic2 = new TransientTopic(dummyTransientTopicName2, dummyPartition2);
        TransientTopic wrongTransientTopic = new TransientTopic(wrongTransientTopicName, wrongTopicNameDummyPartition1);

        Exception ex;

        TransientTopicIndexCache mockTransientTopicIndexCache = createMockTransientTopicIndexCache();
        assertNull(mockTransientTopicIndexCache.getIndex(dummyTransientTopicName1));

        mockTransientTopicIndexCache.addIndexToCache(dummyTransientTopic1);
        assertEquals(dummyTransientTopic1, mockTransientTopicIndexCache.getIndex(dummyTransientTopicName1));

        // TODO: change it to correct exception
        ex = assertThrows(Exception.class, () -> mockTransientTopicIndexCache.addIndexToCache(dummyTransientTopic1));
        assertEquals("", ex.getMessage());

        // TODO: change it to correct exception
        ex = assertThrows(Exception.class, () -> mockTransientTopicIndexCache.addIndexToCache(wrongTransientTopic));
        assertEquals("", ex.getMessage());
    }

    private TransientTopicIndexCache createMockTransientTopicIndexCache() {
        return new TransientTopicIndexCache.Builder()
            .build();
    }
 */
}
