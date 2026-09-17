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
import kafka.network.RequestChannel
import kafka.utils.TestUtils
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.Mockito.mock

import java.nio.file.{Files, Path}
import java.time.Duration
import java.util

class ProduceRequestRateCheckInterceptorTest {
  @TempDir
  var tempDir: Path = _

  @Test
  def testMonitorLogColumnOrderAndKafkaMessage(): Unit = {
    val log = new ProduceRequestThroughputMonitorLog(2_001L, 1_001L, 2L, 1L)

    assertEquals(util.List.of(
      "measured_at",
      "measurement_duration_ms",
      "success_processed_req_cnt",
      "failed_processed_req_cnt",
      "throughput_req_per_sec"
    ), log.getHeaders)
    assertEquals(util.List.of(
      "2001",
      "1001",
      "2",
      "1",
      "2.00"
    ), log.getValues)
    assertEquals("Produce Request Handling Thourghput: 2.00 req/s (2 reqs)", log.kafkaLogMessage)

    val failedOnlyLog = new ProduceRequestThroughputMonitorLog(3_002L, 1_001L, 0L, 3L)
    assertEquals("0.00", failedOnlyLog.formattedThroughput)
    assertEquals("Produce Request Handling Thourghput: 0.00 req/s (0 reqs)", failedOnlyLog.kafkaLogMessage)
  }

  @Test
  def testWritesCompletedMeasurementToCsv(): Unit = {
    val output = tempDir.resolve("produce-throughput.csv")
    var currentTimeMs = 1_000L
    val interceptor = new ProduceRequestRateCheckInterceptor(
      new LogContext("[produce-throughput-test] "),
      ProduceRequestThroughputSettings(output, 60_000L, Duration.ZERO, 0L),
      () => currentTimeMs
    )
    interceptor.init()

    try {
      interceptor.recordHandledProduceRequest(successful = true)
      interceptor.recordHandledProduceRequest(successful = false)
      interceptor.recordHandledProduceRequest(successful = true)
      currentTimeMs = 2_001L
      interceptor.emitMeasurement()
    } finally {
      interceptor.shutdown()
    }

    assertEquals(util.List.of(
      "measured_at,measurement_duration_ms,success_processed_req_cnt,failed_processed_req_cnt,throughput_req_per_sec",
      "2001,1001,2,1,2.00"
    ), Files.readAllLines(output))
  }

  @Test
  def testWritesZeroMeasurementWhenNoRequestsArrive(): Unit = {
    val output = tempDir.resolve("idle-produce-throughput.csv")
    val interceptor = new ProduceRequestRateCheckInterceptor(
      new LogContext("[idle-produce-throughput-test] "),
      ProduceRequestThroughputSettings(output, 50L, Duration.ZERO, 0L)
    )
    interceptor.init()

    try {
      TestUtils.waitUntilTrue(
        () => Files.exists(output) && Files.readAllLines(output).size() >= 2,
        "Timed out waiting for an idle produce throughput measurement"
      )
    } finally {
      interceptor.shutdown()
    }

    val measurement = Files.readAllLines(output).get(1).split(",")
    assertTrue(measurement(1).toLong > 0L)
    assertEquals("0", measurement(2))
    assertEquals("0", measurement(3))
    assertEquals("0.00", measurement(4))
  }

  @Test
  def testRegistersDedicatedExtensionTargets(): Unit = {
    val interceptor = new ProduceRequestRateCheckInterceptor(
      new LogContext("[produce-throughput-extension-test] "),
      ProduceRequestThroughputSettings(tempDir.resolve("extension.csv"), 60_000L, Duration.ZERO, 0L,
        realtimeLogEnabled = false)
    )

    interceptor.init()
    try {
      assertEquals(
        Set("produce-request-throughput", "produce-request-throughput-rollout"),
        interceptor.extensionHandlers.map(_.target).toSet
      )
    } finally {
      interceptor.shutdown()
    }
  }

  @Test
  def testClassifiesOnlyTerminalProduceResponses(): Unit = {
    val interceptor = new ProduceRequestRateCheckInterceptor(
      new LogContext(),
      ProduceRequestThroughputSettings(tempDir.resolve("unused.csv"), 1_000L, Duration.ZERO, 0L)
    )
    val request = mock(classOf[RequestChannel.Request])
    val send = mock(classOf[Send])

    val successful = new RequestChannel.SendResponse(
      request, send, None, None, errorCounts = Map(Errors.NONE -> 2))
    val partiallyFailed = new RequestChannel.SendResponse(
      request, send, None, None, errorCounts = Map(Errors.NONE -> 1, Errors.NOT_LEADER_OR_FOLLOWER -> 1))

    assertTrue(interceptor.produceRequestSucceeded(successful).contains(true))
    assertTrue(interceptor.produceRequestSucceeded(partiallyFailed).contains(false))
    assertTrue(interceptor.produceRequestSucceeded(new RequestChannel.NoOpResponse(request)).contains(true))
    assertTrue(interceptor.produceRequestSucceeded(new RequestChannel.CloseConnectionResponse(request)).contains(false))
    assertFalse(interceptor.produceRequestSucceeded(new RequestChannel.StartThrottlingResponse(request)).isDefined)
    assertFalse(interceptor.produceRequestSucceeded(new RequestChannel.EndThrottlingResponse(request)).isDefined)
  }
}
