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
package org.apache.kafka.tools;

import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.BrokerExtensionResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.BrokerExtensionRequest;
import org.apache.kafka.common.requests.BrokerExtensionResponse;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BrokerExtensionCommandTest {
    private static final String EXTENSION_ENABLED_CONFIG = "broker.extension.request.enabled";
    private static final String EXTENSION_ALLOWED_LISTENERS_CONFIG = "broker.extension.request.allowed.listeners";

    @Test
    public void testCreatesFlushRequest() throws Exception {
        Uuid requestId = Uuid.randomUuid();
        Namespace options = BrokerExtensionCommand.parseArguments(
            "--bootstrap-server", "localhost:9092",
            "--flush",
            "--request-id", requestId.toString());

        BrokerExtensionRequest request = BrokerExtensionCommand.createRequest(options);

        assertEquals("monitor-log", request.data().target());
        assertEquals("flush", request.data().operation());
        assertEquals(requestId, request.data().requestId());
        assertEquals(0, request.data().payloadVersion());
    }

    @Test
    public void testCreatesDividerAndFlushRequest() throws Exception {
        Namespace options = BrokerExtensionCommand.parseArguments(
            "--bootstrap-server", "localhost:9092",
            "--target", "monitor-log",
            "--divider-and-flush", "checkpoint");

        BrokerExtensionRequest request = BrokerExtensionCommand.createRequest(options);

        assertEquals("divider-and-flush", request.data().operation());
        byte[] payload = new byte[request.data().payload().remaining()];
        request.data().payload().duplicate().get(payload);
        assertArrayEquals("checkpoint".getBytes(StandardCharsets.UTF_8), payload);
    }

    @Test
    public void testRejectsMissingOrConflictingOperations() {
        assertThrows(ArgumentParserException.class, () -> BrokerExtensionCommand.parseArguments(
            "--bootstrap-server", "localhost:9092"));
        assertThrows(ArgumentParserException.class, () -> BrokerExtensionCommand.parseArguments(
            "--bootstrap-server", "localhost:9092", "--flush", "--operation", "flush"));
        assertThrows(ArgumentParserException.class, () -> BrokerExtensionCommand.parseArguments(
            "--bootstrap-server", "localhost:9092,localhost:9093", "--flush"));
    }

    @Test
    public void testPrintResponseHandlesSuccessAndFailure() {
        BrokerExtensionResponse success = new BrokerExtensionResponse(new BrokerExtensionResponseData()
            .setErrorCode(Errors.NONE.code())
            .setPayloadVersion((short) 1)
            .setPayload(new byte[] {1, 2}));
        assertDoesNotThrow(() -> BrokerExtensionCommand.printResponse(success));

        BrokerExtensionResponse failure = new BrokerExtensionResponse(new BrokerExtensionResponseData()
            .setErrorCode(Errors.INVALID_REQUEST.code())
            .setErrorMessage("bad request"));
        assertThrows(RuntimeException.class, () -> BrokerExtensionCommand.printResponse(failure));
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = EXTENSION_ENABLED_CONFIG, value = "true"),
        @ClusterConfigProperty(key = EXTENSION_ALLOWED_LISTENERS_CONFIG, value = "EXTERNAL")
    })
    public void testBrokerExtensionRequestEndToEnd(ClusterInstance clusterInstance) {
        String output = ToolsTestUtils.captureStandardOut(() ->
            executeUnchecked("--bootstrap-server", clusterInstance.bootstrapServers(), "--flush"));
        assertTrue(output.startsWith("request succeeded"));

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
            executeUnchecked("--bootstrap-server", clusterInstance.bootstrapServers(),
                "--target", "missing-target", "--operation", "flush"));
        assertTrue(exception.getMessage().contains("INVALID_REQUEST"));
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = EXTENSION_ENABLED_CONFIG, value = "true"),
        @ClusterConfigProperty(key = EXTENSION_ALLOWED_LISTENERS_CONFIG, value = "EXTERNAL")
    })
    public void testBrokerExtensionClientFailsAfterTargetBrokerShutdown(ClusterInstance clusterInstance) {
        int brokerId = clusterInstance.brokerIds().iterator().next();
        clusterInstance.shutdownBroker(brokerId);

        assertEquals(1, BrokerExtensionCommand.mainNoExit(
            "--bootstrap-server", clusterInstance.bootstrapServers(), "--flush", "--timeout-ms", "100"));
    }

    private static void executeUnchecked(String... args) {
        try {
            BrokerExtensionCommand.execute(args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
