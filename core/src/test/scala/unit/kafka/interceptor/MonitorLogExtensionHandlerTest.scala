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

import moniq.MonitorLog
import moniq.writer.MonitorLogWriter
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.InvalidRequestException
import org.junit.jupiter.api.Assertions.{assertEquals, assertInstanceOf, assertThrows}
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{mock, times, verify, when}

import java.nio.charset.StandardCharsets
import java.util.concurrent.{ExecutionException, Executors, TimeUnit}

class MonitorLogExtensionHandlerTest {
  private val writer = mock(classOf[MonitorLogWriter])
  private val executor = Executors.newSingleThreadExecutor()
  private val handler = new MonitorLogExtensionHandler(writer, executor)

  @AfterEach
  def tearDown(): Unit = {
    handler.shutdown()
    executor.shutdownNow()
  }

  @Test
  def testFlushCompletesOnlyAfterWriterFlush(): Unit = {
    when(writer.flush()).thenReturn(true)

    handler.handle(command("flush")).toCompletableFuture.get(5, TimeUnit.SECONDS)

    verify(writer).flush()
  }

  @Test
  def testFlushFailureIsReturnedAsHandlerFailure(): Unit = {
    when(writer.flush()).thenReturn(false)

    val error = awaitFailure("flush")

    assertInstanceOf(classOf[IllegalStateException], error)
    assertEquals("Monitor log commit failed", error.getMessage)
  }

  @Test
  def testDividerIsSubmittedBeforeFlushAndDeduplicatedByRequestId(): Unit = {
    when(writer.flush()).thenReturn(true)
    val requestId = Uuid.randomUuid()

    handler.handle(command("divider-and-flush", requestId, "boundary-1".getBytes(StandardCharsets.UTF_8))).toCompletableFuture.get(5, TimeUnit.SECONDS)
    handler.handle(command("divider-and-flush", requestId, "boundary-1".getBytes(StandardCharsets.UTF_8))).toCompletableFuture.get(5, TimeUnit.SECONDS)

    val log = ArgumentCaptor.forClass(classOf[MonitorLog])
    verify(writer, times(1)).submit(log.capture())
    assertEquals("MONITOR_DIVIDER", log.getValue.getType)
    assertEquals("boundary-1", log.getValue.getId)
    verify(writer, times(2)).flush()
  }

  @Test
  def testRejectsUnsupportedOperationAndPayloadVersion(): Unit = {
    assertInstanceOf(classOf[InvalidRequestException], awaitFailure("unknown"))
    assertInstanceOf(classOf[InvalidRequestException], awaitFailure("flush", payloadVersion = 1))
  }

  private def awaitFailure(operation: String, payloadVersion: Short = 0): Throwable = {
    val error = assertThrows(classOf[ExecutionException], () =>
      handler.handle(command(operation, payloadVersion = payloadVersion)).toCompletableFuture.get(5, TimeUnit.SECONDS))
    error.getCause
  }

  private def command(
    operation: String,
    requestId: Uuid = Uuid.randomUuid(),
    payload: Array[Byte] = Array.emptyByteArray,
    payloadVersion: Short = 0
  ): BrokerExtensionCommand = BrokerExtensionCommand(
    requestId,
    "monitor-log",
    operation,
    payloadVersion,
    payload,
    BrokerExtensionContext(1, "connection", "User:test", "client", "BROKER", Long.MaxValue)
  )
}
