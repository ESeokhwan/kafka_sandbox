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

import org.apache.kafka.common.message.ReadGlobalSequenceDataRequestData;
import org.apache.kafka.common.message.ReadGlobalSequenceDataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class ReadGlobalSequenceDataRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ReadGlobalSequenceDataRequest> {
        private final ReadGlobalSequenceDataRequestData data;

        public Builder(ReadGlobalSequenceDataRequestData data) {
            super(ApiKeys.READ_GLOBAL_SEQUENCE_DATA);
            this.data = data;
        }

        @Override
        public ReadGlobalSequenceDataRequest build(short version) {
            return new ReadGlobalSequenceDataRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ReadGlobalSequenceDataRequestData data;

    public ReadGlobalSequenceDataRequest(ReadGlobalSequenceDataRequestData data, short version) {
        super(ApiKeys.READ_GLOBAL_SEQUENCE_DATA, version);
        this.data = data;
    }

    @Override
    public ReadGlobalSequenceDataResponse getErrorResponse(int throttleTimeMs, Throwable exception) {
        Errors error = Errors.forException(exception);
        return new ReadGlobalSequenceDataResponse(new ReadGlobalSequenceDataResponseData()
            .setErrorCode(error.code())
            .setErrorMessage(error.message()));
    }

    @Override
    public ReadGlobalSequenceDataRequestData data() {
        return data;
    }

    public static ReadGlobalSequenceDataRequest parse(Readable readable, short version) {
        return new ReadGlobalSequenceDataRequest(new ReadGlobalSequenceDataRequestData(readable, version), version);
    }
}
