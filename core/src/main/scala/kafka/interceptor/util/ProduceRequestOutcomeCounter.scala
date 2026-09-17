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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

final class ProduceRequestOutcomeCounter(
  measurementIntervalMs: Long,
  currentTimeMillis: () => Long = () => System.currentTimeMillis()
) {
  require(measurementIntervalMs > 0, "measurementIntervalMs must be positive")

  private val successfulRequestCount = new AtomicLong(0)
  private val failedRequestCount = new AtomicLong(0)
  private val isMeasuring = new AtomicBoolean(false)

  private var lastMeasurementTimeMs = 0L
  private var lastSuccessfulRequestCount = 0L
  private var lastFailedRequestCount = 0L

  def record(
    successful: Boolean,
    onMeasurement: (Long, Long, Long, Long) => Unit
  ): Unit = {
    if (successful) successfulRequestCount.incrementAndGet()
    else failedRequestCount.incrementAndGet()

    if (isMeasuring.compareAndSet(false, true)) {
      try {
        val currentTimeMs = currentTimeMillis()
        if (lastMeasurementTimeMs == 0L) {
          lastMeasurementTimeMs = currentTimeMs
        } else if (currentTimeMs - lastMeasurementTimeMs > measurementIntervalMs) {
          val currentSuccessfulRequestCount = successfulRequestCount.get()
          val currentFailedRequestCount = failedRequestCount.get()
          onMeasurement(
            currentTimeMs,
            currentTimeMs - lastMeasurementTimeMs,
            currentSuccessfulRequestCount - lastSuccessfulRequestCount,
            currentFailedRequestCount - lastFailedRequestCount
          )
          lastMeasurementTimeMs = currentTimeMs
          lastSuccessfulRequestCount = currentSuccessfulRequestCount
          lastFailedRequestCount = currentFailedRequestCount
        }
      } finally {
        isMeasuring.set(false)
      }
    }
  }
}
