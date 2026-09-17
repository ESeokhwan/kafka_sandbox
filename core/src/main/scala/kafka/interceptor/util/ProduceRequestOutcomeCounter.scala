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
package kafka.interceptor.util

import java.util.concurrent.atomic.LongAdder

final case class ProduceRequestOutcomeSnapshot(
  successfulRequestCount: Long,
  failedRequestCount: Long
) {
  def isEmpty: Boolean = successfulRequestCount == 0L && failedRequestCount == 0L
}

final class ProduceRequestOutcomeCounter {
  private val successfulRequestCount = new LongAdder
  private val failedRequestCount = new LongAdder

  def record(successful: Boolean): Unit = {
    if (successful) successfulRequestCount.increment()
    else failedRequestCount.increment()
  }

  def snapshot(): ProduceRequestOutcomeSnapshot = ProduceRequestOutcomeSnapshot(
    successfulRequestCount.sum(),
    failedRequestCount.sum()
  )
}
