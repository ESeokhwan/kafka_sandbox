/**
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

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.InvalidRequestException
import org.junit.jupiter.api.Assertions.{assertEquals, assertInstanceOf, assertThrows}
import org.junit.jupiter.api.Test

import java.util.concurrent.{CompletableFuture, CompletionStage, ExecutionException}

class BrokerExtensionRegistryTest {

  @Test
  def testDispatchesToTargetedHandler(): Unit = {
    val expected = BrokerExtensionResult(payloadVersion = 2, payload = Array[Byte](1, 2))
    val handler = new TestHandler("monitor-log", _ => CompletableFuture.completedFuture(expected))
    val registry = new BrokerExtensionRegistry(Seq(handler))

    val result = registry.dispatch(command("monitor-log")).toCompletableFuture.get()

    assertEquals(expected, result)
    assertEquals(Set("monitor-log"), registry.targets)
  }

  @Test
  def testRejectsUnknownTarget(): Unit = {
    val registry = new BrokerExtensionRegistry(Seq.empty)

    val error = awaitFailure(registry.dispatch(command("missing")))

    assertInstanceOf(classOf[InvalidRequestException], error)
    assertEquals("Unknown broker extension target: missing", error.getMessage)
  }

  @Test
  def testRejectsDuplicateAndInvalidHandlerTargets(): Unit = {
    val first = new TestHandler("monitor-log", _ => CompletableFuture.completedFuture(BrokerExtensionResult()))
    val duplicate = new TestHandler("monitor-log", _ => CompletableFuture.completedFuture(BrokerExtensionResult()))
    val invalid = new TestHandler("Invalid Target", _ => CompletableFuture.completedFuture(BrokerExtensionResult()))

    assertThrows(classOf[IllegalArgumentException], () => new BrokerExtensionRegistry(Seq(first, duplicate)))
    assertThrows(classOf[IllegalArgumentException], () => new BrokerExtensionRegistry(Seq(invalid)))
  }

  @Test
  def testNormalizesSynchronousHandlerFailure(): Unit = {
    val registry = new BrokerExtensionRegistry(Seq(
      new TestHandler("failing", _ => throw new IllegalStateException("handler failed"))))

    val error = awaitFailure(registry.dispatch(command("failing")))

    assertInstanceOf(classOf[IllegalStateException], error)
    assertEquals("handler failed", error.getMessage)
  }

  @Test
  def testBuildsRegistryAfterInterceptorInitialization(): Unit = {
    val handler = new TestHandler("monitor-log", _ => CompletableFuture.completedFuture(BrokerExtensionResult()))
    val interceptor = new LifecycleInterceptor(handler)
    val interceptors = new BrokerInterceptors(Vector(interceptor))

    assertInstanceOf(classOf[InvalidRequestException], awaitFailure(interceptors.dispatchBrokerExtension(command("monitor-log"))))

    interceptors.init()
    interceptors.dispatchBrokerExtension(command("monitor-log")).toCompletableFuture.get()

    interceptors.shutdown()
    assertInstanceOf(classOf[InvalidRequestException], awaitFailure(interceptors.dispatchBrokerExtension(command("monitor-log"))))
  }

  private def command(target: String): BrokerExtensionCommand = BrokerExtensionCommand(
    Uuid.randomUuid(),
    target,
    "flush",
    0,
    Array.emptyByteArray,
    BrokerExtensionContext(1, "connection", "User:test", "client", "BROKER", Long.MaxValue)
  )

  private def awaitFailure(stage: CompletionStage[BrokerExtensionResult]): Throwable = {
    val exception = assertThrows(classOf[ExecutionException], () => stage.toCompletableFuture.get())
    exception.getCause
  }

  private final class TestHandler(
    override val target: String,
    callback: BrokerExtensionCommand => CompletionStage[BrokerExtensionResult]
  ) extends BrokerExtensionHandler {
    override def handle(command: BrokerExtensionCommand): CompletionStage[BrokerExtensionResult] = callback(command)
  }

  private final class LifecycleInterceptor(handler: BrokerExtensionHandler) extends BrokerInterceptor {
    private var initialized = false

    override def init(): Unit = initialized = true

    override def extensionHandlers: Seq[BrokerExtensionHandler] = {
      if (initialized) Seq(handler) else Seq.empty
    }
  }
}
