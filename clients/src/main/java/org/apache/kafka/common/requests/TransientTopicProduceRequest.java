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

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.message.TransientTopicProduceRequestData;
import org.apache.kafka.common.message.TransientTopicProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.kafka.common.requests.ProduceResponse.INVALID_OFFSET;

public class TransientTopicProduceRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<TransientTopicProduceRequest> {
        private final TransientTopicProduceRequestData data;

        public Builder(TransientTopicProduceRequestData data) {
            super(ApiKeys.TRANSIENT_TOPIC_PRODUCE);
            this.data = data;
        }

        @Override
        public TransientTopicProduceRequest build(short version) {
            return build(version, true);
        }

        // Visible for testing only
        public TransientTopicProduceRequest buildUnsafe(short version) {
            return build(version, false);
        }

        private TransientTopicProduceRequest build(short version, boolean validate) {
            if (validate) {
                // Validate the given records first
                data.topicData().forEach(tpd ->
                    TransientTopicProduceRequest.validateRecords(version, tpd.records()));
            }
            return new TransientTopicProduceRequest(data, version);
        }

        @Override
        public String toString() {
            return "(type=TransientTopicProduceRequest" +
                ", acks=" + data.acks() +
                ", timeout=" + data.timeoutMs() +
                ", topicRecords=(" + new ArrayList<>(data.topicData()) + ")" +
                "'";
        }
    }

    /**
     * We have to copy acks, timeout, transactionalId and partitionSizes from data since data maybe reset to eliminate
     * the reference to ByteBuffer but those metadata are still useful.
     */
    private final short acks;
    private final int timeout;
    // This is set to null by `clearTopicRecords` to prevent unnecessary memory retention when a transient produce
    // request is put in the purgatory (due to client throttling, it can take a while before the response is sent).
    // Care should be taken in methods that use this field.
    private volatile TransientTopicProduceRequestData data;
    // the topicSizes is lazily initialized since it is used by server-side in production.
    private volatile Map<String, Integer> topicSizes;

    public TransientTopicProduceRequest(TransientTopicProduceRequestData transientTopicProduceRequestData, short version) {
        super(ApiKeys.TRANSIENT_TOPIC_PRODUCE, version);
        this.data = transientTopicProduceRequestData;
        this.acks = data.acks();
        this.timeout = data.timeoutMs();
    }

    // visible for testing
    Map<String, Integer> topicSizes() {
        if (topicSizes == null) {
            // this method may be called by different thread (see the comment on data)
            synchronized (this) {
                if (topicSizes == null) {
                    Map<String, Integer> tmpTopicSizes = new HashMap<>();
                    data.topicData().forEach(topicData ->
                        tmpTopicSizes.compute(topicData.name(), (ignored, previousValue) ->
                            topicData.records().sizeInBytes() + (previousValue == null ? 0 : previousValue)
                        )
                    );
                    topicSizes = tmpTopicSizes;
                }
            }
        }
        return topicSizes;
    }

    /**
     * @return data or IllegalStateException if the data is removed (to prevent unnecessary memory retention).
     */
    @Override
    public TransientTopicProduceRequestData data() {
        // Store it in a local variable to protect against concurrent updates
        TransientTopicProduceRequestData tmp = data;
        if (tmp == null)
            throw new IllegalStateException("The topic records are no longer available because clearTopicRecords() has been invoked.");
        return tmp;
    }

    @Override
    public String toString(boolean verbose) {
        // Use the same format as `Struct.toString()`
        StringBuilder bld = new StringBuilder();
        bld.append("{acks=").append(acks)
            .append(",timeout=").append(timeout);

        if (verbose)
            bld.append(",topicSizes=").append(Utils.mkString(topicSizes(), "[", "]", "=", ","));
        else
            bld.append(",numTopics=").append(topicSizes().size());

        bld.append("}");
        return bld.toString();
    }

    @Override
    public TransientTopicProduceResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        /* In case the producer doesn't actually want any response */
        if (acks == 0) return null;
        ApiError apiError = ApiError.fromThrowable(e);
        TransientTopicProduceResponseData data = new TransientTopicProduceResponseData().setThrottleTimeMs(throttleTimeMs);
        topicSizes().forEach((tp, ignored) -> {
            TransientTopicProduceResponseData.TopicProduceResponse tpr = data.responses().find(tp);
            if (tpr == null) {
                tpr = new TransientTopicProduceResponseData.TopicProduceResponse()
                    .setName(tp)
                    .setRecordErrors(Collections.emptyList())
                    .setBaseOffset(INVALID_OFFSET)
                    .setLogAppendTimeMs(RecordBatch.NO_TIMESTAMP)
                    .setLogStartOffset(INVALID_OFFSET)
                    .setErrorMessage(apiError.message())
                    .setErrorCode(apiError.error().code());
                data.responses().add(tpr);
            }
        });
        return new TransientTopicProduceResponse(data);
    }

    @Override
    public Map<Errors, Integer> errorCounts(Throwable e) {
        Errors error = Errors.forException(e);
        return Collections.singletonMap(error, topicSizes().size());
    }

    public short acks() {
        return acks;
    }

    public int timeout() {
        return timeout;
    }

    public void clearTopicRecords() {
        // lazily initialize topicSizes.
        topicSizes();
        data = null;
    }

    public static void validateRecords(short version, BaseRecords baseRecords) {
        if (baseRecords instanceof Records) {
            Records records = (Records) baseRecords;
            Iterator<? extends RecordBatch> iterator = records.batches().iterator();
            if (!iterator.hasNext())
                throw new InvalidRecordException("Transient Topic Produce requests with version " + version +
                    " must have at least one record batch per topic");

            RecordBatch entry = iterator.next();
            if (entry.magic() != RecordBatch.MAGIC_VALUE_V2)
                throw new InvalidRecordException("Transient Topic Produce requests with version " + version +
                    " are only allowed to contain record batches with magic version 2");
            if (iterator.hasNext())
                throw new InvalidRecordException("Transient Topic Produce requests with version " + version +
                    " are only allowed to contain exactly one record batch per partition");
        }
    }

    public static TransientTopicProduceRequest parse(ByteBuffer buffer, short version) {
        return new TransientTopicProduceRequest(new TransientTopicProduceRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
