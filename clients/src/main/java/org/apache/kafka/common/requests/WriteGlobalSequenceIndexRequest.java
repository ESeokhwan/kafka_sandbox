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

import org.apache.kafka.common.message.WriteGlobalSequenceIndexRequestData;
import org.apache.kafka.common.message.WriteGlobalSequenceIndexResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class WriteGlobalSequenceIndexRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<WriteGlobalSequenceIndexRequest> {
        private final WriteGlobalSequenceIndexRequestData data;

        public Builder(WriteGlobalSequenceIndexRequestData data) {
            super(ApiKeys.WRITE_GLOBAL_SEQUENCE_INDEX);
            this.data = data;
        }

        @Override
        public WriteGlobalSequenceIndexRequest build(short version) {
            return new WriteGlobalSequenceIndexRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final WriteGlobalSequenceIndexRequestData data;

    public WriteGlobalSequenceIndexRequest(WriteGlobalSequenceIndexRequestData data, short version) {
        super(ApiKeys.WRITE_GLOBAL_SEQUENCE_INDEX, version);
        this.data = data;
    }

    @Override
    public WriteGlobalSequenceIndexResponse getErrorResponse(int throttleTimeMs, Throwable exception) {
        Errors error = Errors.forException(exception);
        return new WriteGlobalSequenceIndexResponse(new WriteGlobalSequenceIndexResponseData()
            .setErrorCode(error.code())
            .setErrorMessage(error.message()));
    }

    @Override
    public WriteGlobalSequenceIndexRequestData data() {
        return data;
    }

    public static WriteGlobalSequenceIndexRequest parse(Readable readable, short version) {
        return new WriteGlobalSequenceIndexRequest(
            new WriteGlobalSequenceIndexRequestData(readable, version),
            version
        );
    }
}
