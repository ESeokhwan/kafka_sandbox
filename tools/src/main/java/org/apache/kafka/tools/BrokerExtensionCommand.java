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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.BrokerExtensionRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.BrokerExtensionRequest;
import org.apache.kafka.common.requests.BrokerExtensionResponse;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeConst;

/** Sends a broker-local BROKER_EXTENSION request to a bootstrap broker. */
public class BrokerExtensionCommand {
    private static final String DEFAULT_TARGET = "monitor-log";
    private static final int DEFAULT_TIMEOUT_MS = 30_000;

    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (ArgumentParserException e) {
            e.getParser().handleError(e);
            return 1;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            return 1;
        }
    }

    static void execute(String... args) throws Exception {
        Namespace options = parseArguments(args);
        Properties properties = options.getString("command_config") == null
            ? new Properties() : Utils.loadProps(options.getString("command_config"));
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, options.getString("bootstrap_server"));
        long clientRequestTimeoutMs = Math.min(Integer.MAX_VALUE, (long) options.getInt("timeout_ms") + 1_000L);
        properties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, Long.toString(clientRequestTimeoutMs));

        BrokerExtensionRequest request = createRequest(options);
        try (BrokerApiVersionsCommand.AdminClient client = BrokerApiVersionsCommand.AdminClient.create(properties)) {
            AbstractResponse response = client.sendToBootstrapBroker(new BrokerExtensionRequest.Builder(request.data()));
            printResponse((BrokerExtensionResponse) response);
        }
    }

    static Namespace parseArguments(String... args) throws ArgumentParserException {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("kafka-broker-extension")
            .defaultHelp(true)
            .description("Send a broker-local BROKER_EXTENSION request to a bootstrap broker.");
        parser.addArgument("--bootstrap-server", "-b")
            .action(store()).required(true).help("Broker host and port to receive the request.");
        parser.addArgument("--command-config", "-c")
            .action(store()).help("Property file with client security settings.");
        parser.addArgument("--target")
            .action(store()).setDefault(DEFAULT_TARGET).help("Registered extension target.");
        parser.addArgument("--operation")
            .action(store()).help("Target-specific operation.");
        parser.addArgument("--flush")
            .action(storeConst()).setConst("flush").dest("convenience_operation")
            .help("Run the monitor-log flush operation.");
        parser.addArgument("--divider-and-flush")
            .action(store()).dest("divider")
            .help("Submit this UTF-8 divider value, then flush monitor-log.");
        parser.addArgument("--payload")
            .action(store()).help("UTF-8 payload for --operation.");
        parser.addArgument("--payload-version")
            .type(Short.class).action(store()).setDefault((short) 0).help("Payload schema version.");
        parser.addArgument("--timeout-ms")
            .type(Integer.class).action(store()).setDefault(DEFAULT_TIMEOUT_MS).help("Broker request timeout.");
        parser.addArgument("--request-id")
            .action(store()).help("UUID used for handler-side retry deduplication.");

        Namespace options = parser.parseArgs(args);
        if (options.getString("bootstrap_server").contains(",")) {
            throw new ArgumentParserException("--bootstrap-server must name exactly one target broker.", parser);
        }
        String operation = options.getString("operation");
        String convenienceOperation = options.getString("convenience_operation");
        String divider = options.getString("divider");
        if ((operation == null ? 0 : 1) + (convenienceOperation == null ? 0 : 1) + (divider == null ? 0 : 1) != 1) {
            throw new ArgumentParserException("Specify exactly one of --operation, --flush, or --divider-and-flush.", parser);
        }
        if (options.getString("payload") != null && divider != null) {
            throw new ArgumentParserException("--payload cannot be used with --divider-and-flush.", parser);
        }
        return options;
    }

    static BrokerExtensionRequest createRequest(Namespace options) {
        String divider = options.getString("divider");
        String operation = divider != null ? "divider-and-flush" : options.getString("operation");
        if (operation == null) {
            operation = options.getString("convenience_operation");
        }
        String payload = divider != null ? divider : options.getString("payload");
        String requestId = options.getString("request_id");
        BrokerExtensionRequestData data = new BrokerExtensionRequestData()
            .setRequestId(requestId == null ? Uuid.randomUuid() : Uuid.fromString(requestId))
            .setTarget(options.getString("target"))
            .setOperation(operation)
            .setPayloadVersion(options.getShort("payload_version"))
            .setTimeoutMs(options.getInt("timeout_ms"));
        if (payload != null) {
            data.setPayload(java.nio.ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8)));
        }
        return new BrokerExtensionRequest(data, (short) 0);
    }

    static void printResponse(BrokerExtensionResponse response) {
        Errors error = response.error().error();
        if (error != Errors.NONE) {
            throw new RuntimeException("Broker extension request failed: " + error + ": " + response.error().message());
        }
        byte[] payload = response.data().payload();
        System.out.println("request succeeded"
            + ", payloadVersion=" + response.data().payloadVersion()
            + ", payloadBase64=" + (payload == null ? "" : Base64.getEncoder().encodeToString(payload)));
    }
}
