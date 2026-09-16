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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.Node;

import java.util.concurrent.atomic.AtomicReference;

/** Tracks the single network request belonging to one stateless global fetch. */
public final class GlobalSequenceRequestScope {
    private final AtomicReference<Node> activeNode = new AtomicReference<>();

    void requestStarted(Node node) {
        if (!activeNode.compareAndSet(null, node))
            throw new IllegalStateException("A global sequence request is already active");
    }

    void requestCompleted(Node node) {
        if (!activeNode.compareAndSet(node, null))
            throw new IllegalStateException("Completed global sequence request does not match the active request");
    }

    void ensureIdle() {
        if (activeNode.get() != null)
            throw new IllegalStateException("A previous global sequence request is still active");
    }

    void abort(ConsumerNetworkClient client) {
        Node node = activeNode.getAndSet(null);
        if (node == null)
            return;
        client.disconnectAsync(node);
        client.pollNoWakeup();
    }
}
