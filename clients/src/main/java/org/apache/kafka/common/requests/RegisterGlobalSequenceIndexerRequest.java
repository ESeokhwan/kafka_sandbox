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

import org.apache.kafka.common.message.RegisterGlobalSequenceIndexerRequestData;
import org.apache.kafka.common.message.RegisterGlobalSequenceIndexerResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class RegisterGlobalSequenceIndexerRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<RegisterGlobalSequenceIndexerRequest> {
        private final RegisterGlobalSequenceIndexerRequestData data;

        public Builder(RegisterGlobalSequenceIndexerRequestData data) {
            super(ApiKeys.REGISTER_GLOBAL_SEQUENCE_INDEXER);
            this.data = data;
        }

        @Override
        public RegisterGlobalSequenceIndexerRequest build(short version) {
            return new RegisterGlobalSequenceIndexerRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final RegisterGlobalSequenceIndexerRequestData data;

    public RegisterGlobalSequenceIndexerRequest(RegisterGlobalSequenceIndexerRequestData data, short version) {
        super(ApiKeys.REGISTER_GLOBAL_SEQUENCE_INDEXER, version);
        this.data = data;
    }

    @Override
    public RegisterGlobalSequenceIndexerRequestData data() {
        return data;
    }

    @Override
    public RegisterGlobalSequenceIndexerResponse getErrorResponse(int throttleTimeMs, Throwable exception) {
        Throwable cause = Errors.maybeUnwrapException(exception);
        Errors error = cause instanceof IllegalArgumentException || cause instanceof NullPointerException ?
            Errors.INVALID_REQUEST : Errors.forException(cause);
        return new RegisterGlobalSequenceIndexerResponse(new RegisterGlobalSequenceIndexerResponseData()
            .setTopicId(data.topicId()).setPartition(data.partition())
            .setThrottleTimeMs(throttleTimeMs).setErrorCode(error.code()).setErrorMessage(error.message()));
    }

    public static RegisterGlobalSequenceIndexerRequest parse(Readable readable, short version) {
        return new RegisterGlobalSequenceIndexerRequest(new RegisterGlobalSequenceIndexerRequestData(readable, version), version);
    }
}
