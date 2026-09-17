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
package kafka.interceptor

import moniq.IMonitorLog

import java.time.Instant
import java.util
import java.util.Locale

final class ProduceRequestThroughputMonitorLog(
  val measuredAtMs: Long,
  val measurementDurationMs: Long,
  val processedRequestCount: Long,
  val throughputRequestPerSec: Double
) extends IMonitorLog {
  require(measurementDurationMs > 0, "measurementDurationMs must be positive")
  require(processedRequestCount >= 0, "processedRequestCount must not be negative")
  require(java.lang.Double.isFinite(throughputRequestPerSec) && throughputRequestPerSec >= 0,
    "throughputRequestPerSec must be finite and non-negative")

  val formattedThroughput: String =
    String.format(Locale.ROOT, "%.2f", Double.box(throughputRequestPerSec))

  def kafkaLogMessage: String =
    s"Produce Request Handling Thourghput: $formattedThroughput req/s ($processedRequestCount reqs)"

  override def getHeaders: util.List[String] = ProduceRequestThroughputMonitorLog.Headers

  override def getValues: util.List[String] = util.List.of(
    Instant.ofEpochMilli(measuredAtMs).toString,
    measurementDurationMs.toString,
    processedRequestCount.toString,
    formattedThroughput
  )
}

object ProduceRequestThroughputMonitorLog {
  private val Headers: util.List[String] = util.List.of(
    "measured_at",
    "measurement_duration_ms",
    "processed_req_cnt",
    "throughput_req_per_sec"
  )
}
