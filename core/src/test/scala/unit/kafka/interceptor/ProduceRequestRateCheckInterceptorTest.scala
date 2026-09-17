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

import org.apache.kafka.common.utils.LogContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.nio.file.{Files, Path}
import java.time.Duration
import java.util

class ProduceRequestRateCheckInterceptorTest {
  @TempDir
  var tempDir: Path = _

  @Test
  def testMonitorLogColumnOrderAndKafkaMessage(): Unit = {
    val log = new ProduceRequestThroughputMonitorLog(2_001L, 1_001L, 2L, 1.998001998)

    assertEquals(util.List.of(
      "measured_at",
      "measurement_duration_ms",
      "processed_req_cnt",
      "throughput_req_per_sec"
    ), log.getHeaders)
    assertEquals(util.List.of(
      "1970-01-01T00:00:02.001Z",
      "1001",
      "2",
      "2.00"
    ), log.getValues)
    assertEquals("Produce Request Handling Thourghput: 2.00 req/s (2 reqs)", log.kafkaLogMessage)
  }

  @Test
  def testWritesCompletedMeasurementToCsv(): Unit = {
    val output = tempDir.resolve("produce-throughput.csv")
    var currentTimeMs = 1_000L
    val interceptor = new ProduceRequestRateCheckInterceptor(
      new LogContext("[produce-throughput-test] "),
      ProduceRequestThroughputSettings(output, 1_000L, Duration.ZERO, 0L),
      () => currentTimeMs
    )
    interceptor.init()

    try {
      interceptor.recordHandledProduceRequest()
      currentTimeMs = 2_001L
      interceptor.recordHandledProduceRequest()
    } finally {
      interceptor.shutdown()
    }

    assertEquals(util.List.of(
      "measured_at,measurement_duration_ms,processed_req_cnt,throughput_req_per_sec",
      "1970-01-01T00:00:02.001Z,1001,2,2.00"
    ), Files.readAllLines(output))
  }
}
