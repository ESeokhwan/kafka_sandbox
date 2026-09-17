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
import moniq.writer.strategy.FileMonitorLogWriteStrategy
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.InvalidRequestException
import org.junit.jupiter.api.Assertions.{assertFalse, assertInstanceOf, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.api.io.TempDir

import java.nio.file.{Files, Path}
import java.util.concurrent.{ExecutionException, TimeUnit}

class MonitorLogRollOutExtensionHandlerTest {
  @TempDir
  var tempDir: Path = _

  private val executor = MonitorLogExtensionHandler.newBoundedExecutor("monitor-log-rollout-test")
  private var strategy: FileMonitorLogWriteStrategy = _
  private var handler: MonitorLogRollOutExtensionHandler = _

  @AfterEach
  def tearDown(): Unit = {
    if (handler != null) handler.shutdown()
    if (strategy != null) strategy.close()
  }

  @Test
  def testRollOutClosesCurrentFileAndDeduplicatesRequestId(): Unit = {
    val file = tempDir.resolve("monitor.log")
    strategy = new FileMonitorLogWriteStrategy(file)
    handler = new MonitorLogRollOutExtensionHandler(strategy, executor)
    val requestId = Uuid.randomUuid()

    strategy.write(log("before"))
    handler.handle(command(requestId)).toCompletableFuture.get(5, TimeUnit.SECONDS)
    strategy.write(log("after-first-roll-out"))
    handler.handle(command(requestId)).toCompletableFuture.get(5, TimeUnit.SECONDS)
    strategy.write(log("after-retry"))
    strategy.commit()

    assertTrue(Files.exists(file))
    assertTrue(Files.exists(tempDir.resolve("monitor.1.log")))
    assertFalse(Files.exists(tempDir.resolve("monitor.2.log")))
  }

  @Test
  def testRejectsUnsupportedOperationAndPayload(): Unit = {
    strategy = new FileMonitorLogWriteStrategy(tempDir.resolve("monitor.log"))
    handler = new MonitorLogRollOutExtensionHandler(strategy, executor)

    assertInstanceOf(classOf[InvalidRequestException], awaitFailure(command(operation = "rollup")))
    assertInstanceOf(classOf[InvalidRequestException], awaitFailure(command(payload = Array[Byte](1))))
  }

  private def awaitFailure(command: BrokerExtensionCommand): Throwable = {
    val error = assertThrows(classOf[ExecutionException], () =>
      handler.handle(command).toCompletableFuture.get(5, TimeUnit.SECONDS))
    error.getCause
  }

  private def command(
    requestId: Uuid = Uuid.randomUuid(),
    operation: String = "roll-out",
    payload: Array[Byte] = Array.emptyByteArray
  ): BrokerExtensionCommand = BrokerExtensionCommand(
    requestId,
    "monitor-log-rollout",
    operation,
    0,
    payload,
    BrokerExtensionContext(1, "connection", "User:test", "client", "BROKER", Long.MaxValue)
  )

  private def log(id: String): MonitorLog =
    new MonitorLog("TEST", id, "STATE", System.currentTimeMillis(), System.nanoTime())
}
