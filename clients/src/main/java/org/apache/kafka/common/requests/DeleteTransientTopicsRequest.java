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
import org.apache.kafka.common.message.DeleteTransientTopicsRequestData;
import org.apache.kafka.common.message.DeleteTransientTopicsResponseData.DeletableTransientTopicResult;
import org.apache.kafka.common.message.DeleteTransientTopicsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;
import java.util.List;

public class DeleteTransientTopicsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<DeleteTransientTopicsRequest> {
        private final DeleteTransientTopicsRequestData data;

        public Builder(DeleteTransientTopicsRequestData data) {
            super(ApiKeys.DELETE_TRANSIENT_TOPICS);
            this.data = data;
        }

        @Override
        public DeleteTransientTopicsRequest build(short version) {
            return new DeleteTransientTopicsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DeleteTransientTopicsRequestData data;

    private DeleteTransientTopicsRequest(DeleteTransientTopicsRequestData data, short version) {
        super(ApiKeys.DELETE_TRANSIENT_TOPICS, version);
        this.data = data;
    }

    @Override
    public DeleteTransientTopicsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        DeleteTransientTopicsResponseData response = new DeleteTransientTopicsResponseData();
        if (version() >= 1) {
            response.setThrottleTimeMs(throttleTimeMs);
        }
        ApiError apiError = ApiError.fromThrowable(e);
        for (String topic : topics()) {
            response.responses().add(new DeletableTransientTopicResult()
                    .setName(topic)
                    .setTopicId(Uuid.ZERO_UUID)
                    .setErrorCode(apiError.error().code()));
        }
        return new DeleteTransientTopicsResponse(response);
    }

    public List<String> topics() {
        return data.topics();
    }

    public int numberOfTopics() {
        return data.topics().size();
    }

    public static DeleteTransientTopicsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteTransientTopicsRequest(new DeleteTransientTopicsRequestData(new ByteBufferAccessor(buffer), version), version);
    }

}
