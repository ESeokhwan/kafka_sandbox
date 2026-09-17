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

import moniq.writer.MonitorLogWriter
import moniq.writer.strategy.FileMonitorLogWriteStrategy
import org.apache.kafka.common.Uuid
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, times, verify, when}

import java.nio.file.Files
import java.util.concurrent.{ExecutionException, Executors, TimeUnit}

class ProduceRequestMonitorLogRollOutExtensionHandlerTest {
  private val executor = Executors.newSingleThreadExecutor()
  private val writer = mock(classOf[MonitorLogWriter])
  private val strategy = new FileMonitorLogWriteStrategy(Files.createTempFile("produce-throughput", ".csv"))
  private val delegate = new MonitorLogRollOutExtensionHandler(writer, strategy, executor)
  private var resetCount = 0
  private val handler = new ProduceRequestMonitorLogRollOutExtensionHandler(
    delegate,
    () => resetCount += 1
  )

  @AfterEach
  def tearDown(): Unit = {
    handler.shutdown()
    executor.shutdownNow()
    strategy.close()
  }

  @Test
  def testResetsOnlyForResetOperationAndDeduplicatesRequestId(): Unit = {
    when(writer.flushAndRun(any(classOf[Runnable]))).thenReturn(true)
    val resetRequestId = Uuid.randomUuid()

    handler.handle(command("flush-and-rollout")).toCompletableFuture.get(5, TimeUnit.SECONDS)
    handler.handle(command("flush-and-rollout-with-reset", resetRequestId)).toCompletableFuture.get(5, TimeUnit.SECONDS)
    handler.handle(command("flush-and-rollout-with-reset", resetRequestId)).toCompletableFuture.get(5, TimeUnit.SECONDS)

    assertEquals(1, resetCount)
    verify(writer, times(2)).flushAndRun(any(classOf[Runnable]))
  }

  @Test
  def testDoesNotResetWhenRollOutFails(): Unit = {
    when(writer.flushAndRun(any(classOf[Runnable]))).thenReturn(false)

    val error = try {
      handler.handle(command("flush-and-rollout-with-reset")).toCompletableFuture.get(5, TimeUnit.SECONDS)
      throw new AssertionError("Expected rollout to fail")
    } catch {
      case executionError: ExecutionException => executionError.getCause
    }

    assertEquals("Monitor log commit failed", error.getMessage)
    assertEquals(0, resetCount)
  }

  private def command(operation: String, requestId: Uuid = Uuid.randomUuid()): BrokerExtensionCommand =
    BrokerExtensionCommand(
      requestId,
      handler.target,
      operation,
      0,
      Array.emptyByteArray,
      BrokerExtensionContext(1, "connection", "User:test", "client", "BROKER", Long.MaxValue)
    )
}
