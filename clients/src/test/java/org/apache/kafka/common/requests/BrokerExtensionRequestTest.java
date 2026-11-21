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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ApiMessageType.ListenerType;
import org.apache.kafka.common.message.BrokerExtensionRequestData;
import org.apache.kafka.common.message.BrokerExtensionResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BrokerExtensionRequestTest {

    @Test
    public void testRequestRoundTrip() {
        BrokerExtensionRequestData data = new BrokerExtensionRequestData()
            .setRequestId(Uuid.randomUuid())
            .setTarget("monitor-log")
            .setOperation("divider-and-flush")
            .setPayloadVersion((short) 1)
            .setTimeoutMs(5_000)
            .setPayload(ByteBuffer.wrap(new byte[] {1, 2, 3}));
        BrokerExtensionRequest request = new BrokerExtensionRequest(data, (short) 0);

        ByteBufferAccessor serialized = request.serialize();
        BrokerExtensionRequest parsed = (BrokerExtensionRequest) AbstractRequest.parseRequest(
            ApiKeys.BROKER_EXTENSION, (short) 0, serialized).request;

        assertEquals(data.requestId(), parsed.data().requestId());
        assertEquals(data.target(), parsed.data().target());
        assertEquals(data.operation(), parsed.data().operation());
        assertEquals(data.payloadVersion(), parsed.data().payloadVersion());
        assertEquals(data.timeoutMs(), parsed.data().timeoutMs());
        assertArrayEquals(new byte[] {1, 2, 3}, toArray(parsed.data().payload()));
    }

    @Test
    public void testResponseRoundTripAndErrorResponse() {
        BrokerExtensionResponse response = new BrokerExtensionResponse(
            new BrokerExtensionResponseData()
                .setThrottleTimeMs(12)
                .setErrorCode(Errors.INVALID_REQUEST.code())
                .setErrorMessage("unknown extension target")
                .setPayloadVersion((short) 2)
                .setPayload(new byte[] {4, 5}));

        ByteBufferAccessor serialized = response.serialize((short) 0);
        BrokerExtensionResponse parsed = (BrokerExtensionResponse) AbstractResponse.parseResponse(
            ApiKeys.BROKER_EXTENSION, serialized, (short) 0);

        assertEquals(12, parsed.throttleTimeMs());
        assertEquals(Errors.INVALID_REQUEST, parsed.error().error());
        assertEquals("unknown extension target", parsed.error().message());
        assertEquals((short) 2, parsed.data().payloadVersion());
        assertArrayEquals(new byte[] {4, 5}, parsed.data().payload());

        BrokerExtensionRequest request = new BrokerExtensionRequest(
            new BrokerExtensionRequestData(), (short) 0);
        BrokerExtensionResponse errorResponse = request.getErrorResponse(
            0, Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
        assertEquals(
            Collections.singletonMap(Errors.CLUSTER_AUTHORIZATION_FAILED, 1),
            errorResponse.errorCounts());
    }

    @Test
    public void testSparseBrokerOnlyApiKey() {
        assertEquals((short) 512, ApiKeys.BROKER_EXTENSION.id);
        assertEquals(ApiKeys.BROKER_EXTENSION, ApiKeys.forId(512));
        assertTrue(ApiKeys.apisForListener(ListenerType.BROKER).contains(ApiKeys.BROKER_EXTENSION));
        assertFalse(ApiKeys.apisForListener(ListenerType.CONTROLLER).contains(ApiKeys.BROKER_EXTENSION));
    }

    private static byte[] toArray(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
