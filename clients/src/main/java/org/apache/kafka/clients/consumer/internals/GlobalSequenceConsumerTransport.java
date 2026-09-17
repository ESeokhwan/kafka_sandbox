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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.DefaultHostResolver;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.GlobalSequenceConsumerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Owns the common network, ApiVersions, metadata routing and metrics resources. */
public final class GlobalSequenceConsumerTransport implements AutoCloseable {
    public static final String METRICS_GROUP_PREFIX = "global-sequence-consumer";
    private static final String JMX_PREFIX = "kafka.global.sequence.consumer";

    private final Metadata metadata;
    private final Metrics metrics;
    private final ApiVersions apiVersions;
    private final ConsumerNetworkClient client;
    private final GlobalSequenceConsumerMetadata topicMetadata;
    private final GlobalSequenceRequestScope requestScope;

    private GlobalSequenceConsumerTransport(
        Metadata metadata,
        Metrics metrics,
        ApiVersions apiVersions,
        ConsumerNetworkClient client,
        GlobalSequenceConsumerMetadata topicMetadata,
        GlobalSequenceRequestScope requestScope
    ) {
        this.metadata = metadata;
        this.metrics = metrics;
        this.apiVersions = apiVersions;
        this.client = client;
        this.topicMetadata = topicMetadata;
        this.requestScope = requestScope;
    }

    public static GlobalSequenceConsumerTransport create(GlobalSequenceConsumerConfig config, Time time) {
        LogContext logContext = new LogContext("[GlobalSequenceConsumer clientId=" +
            config.getString(CommonClientConfigs.CLIENT_ID_CONFIG) + "] ");
        Metrics metrics = null;
        Metadata metadata = null;
        ConsumerNetworkClient consumerClient = null;
        try {
            metrics = createMetrics(config, time);
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            List<Node> bootstrapNodes = new ArrayList<>();
            for (int i = 0; i < addresses.size(); i++) {
                InetSocketAddress address = addresses.get(i);
                bootstrapNodes.add(new Node(-i - 1, address.getHostString(), address.getPort()));
            }
            ManualMetadataUpdater updater = new ManualMetadataUpdater(bootstrapNodes);
            metadata = new Metadata(
                config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG),
                config.getLong(CommonClientConfigs.RETRY_BACKOFF_MAX_MS_CONFIG),
                config.getLong(CommonClientConfigs.METADATA_MAX_AGE_CONFIG),
                logContext,
                new ClusterResourceListeners());
            metadata.bootstrap(addresses);
            ApiVersions apiVersions = new ApiVersions();
            NetworkClient networkClient = ClientUtils.createNetworkClient(
                config,
                config.getString(CommonClientConfigs.CLIENT_ID_CONFIG),
                metrics,
                METRICS_GROUP_PREFIX,
                logContext,
                apiVersions,
                time,
                1,
                config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                updater,
                new DefaultHostResolver());
            consumerClient = new ConsumerNetworkClient(
                logContext,
                networkClient,
                metadata,
                time,
                config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG),
                config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                Integer.MAX_VALUE);
            GlobalSequenceRequestScope requestScope = new GlobalSequenceRequestScope();
            GlobalSequenceConsumerMetadata topicMetadata = new GlobalSequenceConsumerMetadata(
                consumerClient,
                updater::setNodes,
                config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG),
                time,
                requestScope);
            return new GlobalSequenceConsumerTransport(metadata, metrics, apiVersions, consumerClient,
                topicMetadata, requestScope);
        } catch (Throwable failure) {
            Utils.closeQuietly(consumerClient, "global sequence consumer network client");
            Utils.closeQuietly(metadata, "global sequence consumer metadata");
            Utils.closeQuietly(metrics, "global sequence consumer metrics");
            throw failure;
        }
    }

    private static Metrics createMetrics(GlobalSequenceConsumerConfig config, Time time) {
        String clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
        List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
        MetricConfig metricConfig = new MetricConfig()
            .samples(config.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG))
            .timeWindow(config.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
            .recordLevel(Sensor.RecordingLevel.forName(
                config.getString(CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG)))
            .tags(Collections.singletonMap(CommonClientConfigs.CLIENT_ID_CONFIG, clientId));
        MetricsContext context = new KafkaMetricsContext(
            JMX_PREFIX,
            config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
        return new Metrics(metricConfig, reporters, time, context);
    }

    public Uuid resolveTopicId(String topic, Optional<Uuid> expectedTopicId, Timer timer) {
        return topicMetadata.resolveTopicId(topic, expectedTopicId, timer);
    }

    public ConsumerNetworkClient client() {
        return client;
    }

    public ApiVersions apiVersions() {
        return apiVersions;
    }

    public Metrics metrics() {
        return metrics;
    }

    public GlobalSequenceRequestScope requestScope() {
        return requestScope;
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        try {
            client.close();
        } catch (IOException e) {
            failure = new org.apache.kafka.common.KafkaException("Failed to close global sequence network client", e);
        } finally {
            metadata.close();
            metrics.close();
        }
        if (failure != null)
            throw failure;
    }
}
