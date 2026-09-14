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

import org.apache.kafka.common.message.ReadGlobalSequenceIndexRequestData;
import org.apache.kafka.common.message.ReadGlobalSequenceIndexResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class ReadGlobalSequenceIndexRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ReadGlobalSequenceIndexRequest> {
        private final ReadGlobalSequenceIndexRequestData data;

        public Builder(ReadGlobalSequenceIndexRequestData data) {
            super(ApiKeys.READ_GLOBAL_SEQUENCE_INDEX);
            this.data = data;
        }

        @Override
        public ReadGlobalSequenceIndexRequest build(short version) {
            return new ReadGlobalSequenceIndexRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ReadGlobalSequenceIndexRequestData data;

    public ReadGlobalSequenceIndexRequest(ReadGlobalSequenceIndexRequestData data, short version) {
        super(ApiKeys.READ_GLOBAL_SEQUENCE_INDEX, version);
        this.data = data;
    }

    @Override
    public ReadGlobalSequenceIndexRequestData data() {
        return data;
    }

    @Override
    public ReadGlobalSequenceIndexResponse getErrorResponse(int throttleTimeMs, Throwable exception) {
        Throwable cause = Errors.maybeUnwrapException(exception);
        Errors error = cause instanceof IllegalArgumentException || cause instanceof NullPointerException ?
            Errors.INVALID_REQUEST : Errors.forException(cause);
        return new ReadGlobalSequenceIndexResponse(new ReadGlobalSequenceIndexResponseData()
            .setTopicId(data.topicId())
            .setThrottleTimeMs(throttleTimeMs).setErrorCode(error.code()).setErrorMessage(error.message()));
    }

    public static ReadGlobalSequenceIndexRequest parse(Readable readable, short version) {
        return new ReadGlobalSequenceIndexRequest(new ReadGlobalSequenceIndexRequestData(readable, version), version);
    }
}
