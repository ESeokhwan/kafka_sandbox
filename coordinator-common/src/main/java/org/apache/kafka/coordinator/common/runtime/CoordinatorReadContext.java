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
package org.apache.kafka.coordinator.common.runtime;

/**
 * An immutable view captured under the active coordinator lock when a read executes.
 * The high watermark is the latest committed boundary applied by the runtime, which
 * may lag the underlying partition while a notification is queued. It is not the log
 * end offset and does not include speculative replay state.
 *
 * This context is not a leadership lease. An asynchronous result derived from it must
 * be validated against the context of a subsequent operation before changing state.
 *
 * @param highWatermark The exclusive committed end offset of the coordinator log.
 * @param leaderEpoch   The coordinator partition's leader epoch, not a snapshot offset
 *                      or the epoch of a data partition managed by the coordinator.
 */
public record CoordinatorReadContext(long highWatermark, int leaderEpoch) {
}
