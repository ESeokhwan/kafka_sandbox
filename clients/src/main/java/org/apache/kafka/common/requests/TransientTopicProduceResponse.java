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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.TransientTopicProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * TODO: Check this comments
 * This wrapper supports both v0 and v8 of ProduceResponse.
 * <p>
 * Possible error code:
 * <p>
 * {@link Errors#CORRUPT_MESSAGE}
 * {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 * {@link Errors#NOT_LEADER_OR_FOLLOWER}
 * {@link Errors#MESSAGE_TOO_LARGE}
 * {@link Errors#INVALID_TOPIC_EXCEPTION}
 * {@link Errors#RECORD_LIST_TOO_LARGE}
 * {@link Errors#NOT_ENOUGH_REPLICAS}
 * {@link Errors#NOT_ENOUGH_REPLICAS_AFTER_APPEND}
 * {@link Errors#INVALID_REQUIRED_ACKS}
 * {@link Errors#TOPIC_AUTHORIZATION_FAILED}
 * {@link Errors#UNSUPPORTED_FOR_MESSAGE_FORMAT}
 * {@link Errors#INVALID_PRODUCER_EPOCH}
 * {@link Errors#CLUSTER_AUTHORIZATION_FAILED}
 * {@link Errors#TRANSACTIONAL_ID_AUTHORIZATION_FAILED}
 * {@link Errors#INVALID_RECORD}
 * {@link Errors#INVALID_TXN_STATE}
 * {@link Errors#INVALID_PRODUCER_ID_MAPPING}
 */
public class TransientTopicProduceResponse extends AbstractResponse {
    public static final long INVALID_OFFSET = -1L;
    private final TransientTopicProduceResponseData data;

    public TransientTopicProduceResponse(TransientTopicProduceResponseData responseData) {
        super(ApiKeys.TRANSIENT_TOPIC_PRODUCE);
        this.data = responseData;
    }

    /**
     * This is deprecated in favor of using the TransientTopicProduceResponseData constructor, KafkaApis should switch
     * to that in KAFKA-10730
     *
     * @param responses Produced data grouped by topic-partition
     */
    @Deprecated
    public TransientTopicProduceResponse(Map<String, TopicResponse> responses) {
        this(responses, DEFAULT_THROTTLE_TIME);
    }

    /**
     * This is deprecated in favor of using the TransientTopicProduceResponseData constructor, KafkaApis should switch
     * to that in KAFKA-10730
     *
     * @param responses      Produced data grouped by topic-partition
     * @param throttleTimeMs Time in milliseconds the response was throttled
     */
    @Deprecated
    public TransientTopicProduceResponse(Map<String, TopicResponse> responses, int throttleTimeMs) {
        this(responses, throttleTimeMs, Collections.emptyList());
    }

    /**
     * This is deprecated in favor of using the TransientTopicProduceResponseData constructor, KafkaApis should switch
     * to that in KAFKA-10730
     *
     * @param responses      Produced data grouped by topic-partition
     * @param throttleTimeMs Time in milliseconds the response was throttled
     * @param nodeEndpoints  List of node endpoints
     */
    @Deprecated
    public TransientTopicProduceResponse(Map<String, TopicResponse> responses, int throttleTimeMs, List<Node> nodeEndpoints) {
        this(toData(responses, throttleTimeMs, nodeEndpoints));
    }

    private static TransientTopicProduceResponseData toData(Map<String, TopicResponse> responses, int throttleTimeMs, List<Node> nodeEndpoints) {
        TransientTopicProduceResponseData data = new TransientTopicProduceResponseData().setThrottleTimeMs(throttleTimeMs);
        responses.forEach((tpName, response) -> {
            TransientTopicProduceResponseData.TopicProduceResponse tpr = data.responses().find(tpName);
            if (tpr == null) {
                tpr = new TransientTopicProduceResponseData.TopicProduceResponse()
                    .setName(tpName)
                    .setBaseOffset(response.baseOffset)
                    .setLogStartOffset(response.logStartOffset)
                    .setLogAppendTimeMs(response.logAppendTime)
                    .setErrorMessage(response.errorMessage)
                    .setErrorCode(response.error.code())
                    .setCurrentLeader(response.currentLeader != null ?
                        response.currentLeader : new TransientTopicProduceResponseData.LeaderIdAndEpoch())
                    .setRecordErrors(response.recordErrors
                        .stream()
                        .map(e -> new TransientTopicProduceResponseData.BatchIndexAndErrorMessage()
                            .setBatchIndex(e.batchIndex)
                            .setBatchIndexErrorMessage(e.message))
                        .collect(Collectors.toList())
                    );
                data.responses().add(tpr);
            }
        });
        nodeEndpoints.forEach(endpoint -> data.nodeEndpoints()
            .add(new TransientTopicProduceResponseData.NodeEndpoint()
                .setNodeId(endpoint.id())
                .setHost(endpoint.host())
                .setPort(endpoint.port())
                .setRack(endpoint.rack())));
        return data;
    }

    @Override
    public TransientTopicProduceResponseData data() {
        return this.data;
    }

    @Override
    public int throttleTimeMs() {
        return this.data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        data.responses().forEach(t -> updateErrorCounts(errorCounts, Errors.forCode(t.errorCode())));
        return errorCounts;
    }

    public static final class TopicResponse {
        public Errors error;
        public long baseOffset;
        public long lastOffset;
        public long logAppendTime;
        public long logStartOffset;
        public List<ProduceResponse.RecordError> recordErrors;
        public String errorMessage;
        public TransientTopicProduceResponseData.LeaderIdAndEpoch currentLeader;

        public TopicResponse(Errors error) {
            this(error, INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, INVALID_OFFSET);
        }

        public TopicResponse(Errors error, String errorMessage) {
            this(error, INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, INVALID_OFFSET, Collections.emptyList(), errorMessage);
        }

        public TopicResponse(Errors error, long baseOffset, long logAppendTime, long logStartOffset) {
            this(error, baseOffset, logAppendTime, logStartOffset, Collections.emptyList(), null);
        }

        public TopicResponse(Errors error, long baseOffset, long logAppendTime, long logStartOffset, List<ProduceResponse.RecordError> recordErrors) {
            this(error, baseOffset, logAppendTime, logStartOffset, recordErrors, null);
        }

        public TopicResponse(Errors error, long baseOffset, long logAppendTime, long logStartOffset, List<ProduceResponse.RecordError> recordErrors, String errorMessage) {
            this(error, baseOffset, INVALID_OFFSET, logAppendTime, logStartOffset, recordErrors, errorMessage, new TransientTopicProduceResponseData.LeaderIdAndEpoch());
        }

        public TopicResponse(Errors error, long baseOffset, long lastOffset, long logAppendTime, long logStartOffset, List<ProduceResponse.RecordError> recordErrors, String errorMessage) {
            this(error, baseOffset, lastOffset, logAppendTime, logStartOffset, recordErrors, errorMessage, new TransientTopicProduceResponseData.LeaderIdAndEpoch());
        }

        public TopicResponse(
            Errors error,
            long baseOffset,
            long lastOffset,
            long logAppendTime,
            long logStartOffset,
            List<ProduceResponse.RecordError> recordErrors,
            String errorMessage,
            TransientTopicProduceResponseData.LeaderIdAndEpoch currentLeader
        ) {
            this.error = error;
            this.baseOffset = baseOffset;
            this.lastOffset = lastOffset;
            this.logAppendTime = logAppendTime;
            this.logStartOffset = logStartOffset;
            this.recordErrors = recordErrors;
            this.errorMessage = errorMessage;
            this.currentLeader = currentLeader;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopicResponse that = (TopicResponse) o;
            return baseOffset == that.baseOffset &&
                lastOffset == that.lastOffset &&
                logAppendTime == that.logAppendTime &&
                logStartOffset == that.logStartOffset &&
                error == that.error &&
                Objects.equals(recordErrors, that.recordErrors) &&
                Objects.equals(errorMessage, that.errorMessage) &&
                Objects.equals(currentLeader, that.currentLeader);
        }

        @Override
        public int hashCode() {
            return Objects.hash(error, baseOffset, lastOffset, logAppendTime, logStartOffset, recordErrors, errorMessage, currentLeader);
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append('{');
            b.append("error: ");
            b.append(error);
            b.append(",offset: ");
            b.append(baseOffset);
            b.append(",lastOffset: ");
            b.append(lastOffset);
            b.append(",logAppendTime: ");
            b.append(logAppendTime);
            b.append(", logStartOffset: ");
            b.append(logStartOffset);
            b.append(", recordErrors: ");
            b.append(recordErrors);
            b.append(", currentLeader: ");
            b.append(currentLeader);
            b.append(", errorMessage: ");
            if (errorMessage != null) {
                b.append(errorMessage);
            } else {
                b.append("null");
            }
            b.append('}');
            return b.toString();
        }

        public static TopicResponse from(ProduceResponse.PartitionResponse tar) {
            TransientTopicProduceResponseData.LeaderIdAndEpoch currentLeader;
            if (tar.currentLeader != null) {
                currentLeader = new TransientTopicProduceResponseData.LeaderIdAndEpoch();
                currentLeader.setLeaderId(tar.currentLeader.leaderId());
                currentLeader.setLeaderEpoch(tar.currentLeader.leaderEpoch());
            } else {
                currentLeader = null;
            }
            return new TopicResponse(
                tar.error,
                tar.baseOffset,
                tar.lastOffset,
                tar.logAppendTime,
                tar.logStartOffset,
                tar.recordErrors,
                tar.errorMessage,
                currentLeader
            );
        }
    }

    public static TransientTopicProduceResponse parse(ByteBuffer buffer, short version) {
        return new TransientTopicProduceResponse(new TransientTopicProduceResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 6; // TODO-2: check this
    }
}
