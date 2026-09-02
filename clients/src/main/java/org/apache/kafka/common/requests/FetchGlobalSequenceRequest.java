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

import org.apache.kafka.common.message.FetchGlobalSequenceRequestData;
import org.apache.kafka.common.message.FetchGlobalSequenceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class FetchGlobalSequenceRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<FetchGlobalSequenceRequest> {
        private final FetchGlobalSequenceRequestData data;

        public Builder(FetchGlobalSequenceRequestData data) {
            super(ApiKeys.FETCH_GLOBAL_SEQUENCE);
            this.data = data;
        }

        @Override
        public FetchGlobalSequenceRequest build(short version) {
            return new FetchGlobalSequenceRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final FetchGlobalSequenceRequestData data;

    public FetchGlobalSequenceRequest(FetchGlobalSequenceRequestData data, short version) {
        super(ApiKeys.FETCH_GLOBAL_SEQUENCE, version);
        this.data = data;
    }

    @Override
    public FetchGlobalSequenceResponse getErrorResponse(int throttleTimeMs, Throwable exception) {
        Errors error = Errors.forException(exception);
        return new FetchGlobalSequenceResponse(new FetchGlobalSequenceResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.code())
            .setErrorMessage(error.message()));
    }

    @Override
    public FetchGlobalSequenceRequestData data() {
        return data;
    }

    public static FetchGlobalSequenceRequest parse(Readable readable, short version) {
        return new FetchGlobalSequenceRequest(new FetchGlobalSequenceRequestData(readable, version), version);
    }
}
