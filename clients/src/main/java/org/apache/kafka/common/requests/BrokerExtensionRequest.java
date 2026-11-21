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

import org.apache.kafka.common.message.BrokerExtensionRequestData;
import org.apache.kafka.common.message.BrokerExtensionResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Readable;

public class BrokerExtensionRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<BrokerExtensionRequest> {
        private final BrokerExtensionRequestData data;

        public Builder(BrokerExtensionRequestData data) {
            super(ApiKeys.BROKER_EXTENSION);
            this.data = data;
        }

        @Override
        public BrokerExtensionRequest build(short version) {
            return new BrokerExtensionRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final BrokerExtensionRequestData data;

    public BrokerExtensionRequest(BrokerExtensionRequestData data, short version) {
        super(ApiKeys.BROKER_EXTENSION, version);
        this.data = data;
    }

    @Override
    public BrokerExtensionRequestData data() {
        return data;
    }

    @Override
    public BrokerExtensionResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError error = ApiError.fromThrowable(e);
        BrokerExtensionResponseData responseData = new BrokerExtensionResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.error().code())
            .setErrorMessage(error.message());
        return new BrokerExtensionResponse(responseData);
    }

    public static BrokerExtensionRequest parse(Readable readable, short version) {
        return new BrokerExtensionRequest(new BrokerExtensionRequestData(readable, version), version);
    }
}
