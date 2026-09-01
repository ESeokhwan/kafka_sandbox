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

import org.apache.kafka.common.message.LookupGlobalSequenceIndexRequestData;
import org.apache.kafka.common.message.LookupGlobalSequenceIndexResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class LookupGlobalSequenceIndexRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<LookupGlobalSequenceIndexRequest> {
        private final LookupGlobalSequenceIndexRequestData data;

        public Builder(LookupGlobalSequenceIndexRequestData data) {
            super(ApiKeys.LOOKUP_GLOBAL_SEQUENCE_INDEX);
            this.data = data;
        }

        @Override
        public LookupGlobalSequenceIndexRequest build(short version) {
            return new LookupGlobalSequenceIndexRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final LookupGlobalSequenceIndexRequestData data;

    public LookupGlobalSequenceIndexRequest(LookupGlobalSequenceIndexRequestData data, short version) {
        super(ApiKeys.LOOKUP_GLOBAL_SEQUENCE_INDEX, version);
        this.data = data;
    }

    @Override
    public LookupGlobalSequenceIndexResponse getErrorResponse(int throttleTimeMs, Throwable exception) {
        Errors error = Errors.forException(exception);
        return new LookupGlobalSequenceIndexResponse(new LookupGlobalSequenceIndexResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(error.code())
            .setErrorMessage(error.message()));
    }

    @Override
    public LookupGlobalSequenceIndexRequestData data() {
        return data;
    }

    public static LookupGlobalSequenceIndexRequest parse(Readable readable, short version) {
        return new LookupGlobalSequenceIndexRequest(
            new LookupGlobalSequenceIndexRequestData(readable, version),
            version
        );
    }
}
