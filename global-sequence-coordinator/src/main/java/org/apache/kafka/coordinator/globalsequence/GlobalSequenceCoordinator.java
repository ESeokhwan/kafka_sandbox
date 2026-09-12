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
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendRequest;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.AppendResponse;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PartitionKey;
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PhysicalBatch;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntSupplier;

/** Broker-local coordinator API. Network authorization and forwarding belong to the RPC layer. */
public interface GlobalSequenceCoordinator {
    int partitionFor(Uuid topicId);

    Properties indexTopicConfigs();

    void startup(IntSupplier indexPartitionCount);

    void onElection(int partition, int leaderEpoch);

    void onResignation(int partition, OptionalInt leaderEpoch);

    void onNewMetadataImage(MetadataImage image, MetadataDelta delta);

    CompletableFuture<AppendResponse> appendIndex(AppendRequest request);

    CompletableFuture<Optional<PhysicalBatch>> committedProgress(PartitionKey partition);

    void shutdown();
}
