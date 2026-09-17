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
import org.apache.kafka.common.errors.InvalidRequestException

import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ArrayBlockingQueue, CompletableFuture, CompletionStage, ExecutorService, RejectedExecutionException, ThreadFactory, ThreadPoolExecutor, TimeUnit}

/** Handles broker extension operations for a monitor log writer. */
final class MonitorLogExtensionHandler(
  monitorLogWriter: MonitorLogWriter,
  executor: ExecutorService,
  maxRememberedDividerRequestIds: Int = 1024
) extends BrokerExtensionHandler {
  override val target: String = "monitor-log"

  private val accepting = new AtomicBoolean(true)
  private val dividerRequestIds = new util.LinkedHashMap[String, java.lang.Boolean]() {
    override def removeEldestEntry(eldest: util.Map.Entry[String, java.lang.Boolean]): Boolean =
      size() > maxRememberedDividerRequestIds
  }

  require(maxRememberedDividerRequestIds > 0, "maxRememberedDividerRequestIds must be positive")

  override def handle(command: BrokerExtensionCommand): CompletionStage[BrokerExtensionResult] = {
    if (!accepting.get()) {
      return CompletableFuture.failedFuture(new IllegalStateException("Monitor log extension handler is shutting down"))
    }
    try {
      CompletableFuture.supplyAsync(() => execute(command), executor)
    } catch {
      case _: RejectedExecutionException =>
        CompletableFuture.failedFuture(new IllegalStateException("Monitor log extension handler is shutting down"))
    }
  }

  def shutdown(): Unit = {
    if (accepting.compareAndSet(true, false)) {
      executor.shutdownNow()
    }
  }

  private def execute(command: BrokerExtensionCommand): BrokerExtensionResult = {
    command.operation match {
      case "flush" =>
        requirePayloadVersion(command)
        flush()
      case "divider-and-flush" =>
        submitDividerOnce(command.requestId.toString, decodeDivider(command))
        flush()
      case operation =>
        throw new InvalidRequestException(s"Unknown monitor-log operation: $operation")
    }
    BrokerExtensionResult()
  }

  private def requirePayloadVersion(command: BrokerExtensionCommand): Unit = {
    if (command.payloadVersion != 0) {
      throw new InvalidRequestException(s"Unsupported monitor-log payload version: ${command.payloadVersion}")
    }
  }

  private def decodeDivider(command: BrokerExtensionCommand): String = {
    requirePayloadVersion(command)
    val divider = new String(command.payload, StandardCharsets.UTF_8)
    if (divider.isEmpty) {
      throw new InvalidRequestException("Monitor-log divider payload must not be empty")
    }
    divider
  }

  private def submitDividerOnce(requestId: String, divider: String): Unit = {
    val isNew = dividerRequestIds.synchronized {
      if (dividerRequestIds.containsKey(requestId)) false
      else {
        dividerRequestIds.put(requestId, java.lang.Boolean.TRUE)
        true
      }
    }
    if (isNew) {
      monitorLogWriter.submit(new MonitorLog("MONITOR_DIVIDER", divider, "DIVIDER", System.currentTimeMillis(), System.nanoTime()))
    }
  }

  private def flush(): Unit = {
    try {
      if (!monitorLogWriter.flush()) {
        throw new IllegalStateException("Monitor log commit failed")
      }
    } catch {
      case error: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new IllegalStateException("Monitor log flush interrupted", error)
    }
  }
}

object MonitorLogExtensionHandler {
  def newBoundedExecutor(threadName: String): ExecutorService = new ThreadPoolExecutor(
    1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue[Runnable](1), new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, threadName)
        thread.setDaemon(true)
        thread
      }
    }, new ThreadPoolExecutor.AbortPolicy())
}
