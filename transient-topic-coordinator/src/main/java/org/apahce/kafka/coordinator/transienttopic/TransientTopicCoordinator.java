package org.apahce.kafka.coordinator.transienttopic;

import org.apache.kafka.common.TransientTopic;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.coordinator.group.CoordinatorRecord;
import org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.slf4j.Logger;

import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntSupplier;

public class TransientTopicCoordinator {

    private final Logger log;

    private final CoordinatorRuntime<TransientTopicCoordinatorShard, CoordinatorRecord> runtime;

    private volatile int numPartitions = -1;

    TransientTopicCoordinator(Logger log, CoordinatorRuntime<TransientTopicCoordinatorShard, CoordinatorRecord> runtime) {
        this.log = log;
        this.runtime = runtime;
    }

    public CompletableFuture<TransientTopic> createNewTransientTopic(
        RequestContext context,
        Uuid topicId,
        String topicName
    ) {
        // TODO: Implement here
        return null;
    }

    public CompletableFuture<Void> freeTransientTopic() {
        // TODO: Implement here
        return null;
    }

    public void onElection(
        int groupMetadataPartitionIndex,
        int groupMetadataPartitionLeaderEpoch
    ) {
        // TODO: Implement here
    }

    public void onResignation(
        int groupMetadataPartitionIndex,
        OptionalInt groupMetadataPartitionLeaderEpoch
    ) {
        // TODO: Implement here
    }

    public void onNewMetadataImage(
        MetadataImage newImage,
        MetadataDelta delta
    ) {
        // TODO: Implement here
    }

    public void startup(IntSupplier groupMetadataTopicPartitionCount) {
        // TODO: Implement here
    }

    public void shutdown() {
        // TODO: Implement here
    }
}
