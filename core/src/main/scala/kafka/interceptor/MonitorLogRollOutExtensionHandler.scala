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

import moniq.writer.strategy.FileMonitorLogWriteStrategy
import moniq.writer.MonitorLogWriter
import org.apache.kafka.common.errors.InvalidRequestException

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CompletableFuture, CompletionStage, ExecutorService, RejectedExecutionException}

/** Flushes and rolls out the active monitor log file at a writer queue boundary. */
final class MonitorLogRollOutExtensionHandler(
  monitorLogWriter: MonitorLogWriter,
  fileMonitorLogWriteStrategy: FileMonitorLogWriteStrategy,
  executor: ExecutorService,
  maxRememberedRequestIds: Int = 1024
) extends BrokerExtensionHandler {
  override val target: String = "monitor-log-rollout"

  private val accepting = new AtomicBoolean(true)
  private val requestIds = new util.LinkedHashMap[String, java.lang.Boolean]() {
    override def removeEldestEntry(eldest: util.Map.Entry[String, java.lang.Boolean]): Boolean =
      size() > maxRememberedRequestIds
  }

  require(maxRememberedRequestIds > 0, "maxRememberedRequestIds must be positive")

  override def handle(command: BrokerExtensionCommand): CompletionStage[BrokerExtensionResult] = {
    if (!accepting.get()) {
      return CompletableFuture.failedFuture(new IllegalStateException("Monitor log rollout handler is shutting down"))
    }
    try {
      CompletableFuture.supplyAsync(() => rollOut(command), executor)
    } catch {
      case _: RejectedExecutionException =>
        CompletableFuture.failedFuture(new IllegalStateException("Monitor log rollout handler is shutting down"))
    }
  }

  def shutdown(): Unit = {
    accepting.compareAndSet(true, false)
  }

  private def rollOut(command: BrokerExtensionCommand): BrokerExtensionResult = {
    if (command.operation != "flush-and-rollout") {
      throw new InvalidRequestException(s"Unknown monitor-log-rollout operation: ${command.operation}")
    }
    if (command.payloadVersion != 0) {
      throw new InvalidRequestException(s"Unsupported monitor-log-rollout payload version: ${command.payloadVersion}")
    }
    if (command.payload.nonEmpty) {
      throw new InvalidRequestException("Monitor-log-rollout does not accept a payload")
    }
    val isNew = requestIds.synchronized {
      if (requestIds.containsKey(command.requestId.toString)) false
      else {
        requestIds.put(command.requestId.toString, java.lang.Boolean.TRUE)
        true
      }
    }
    if (isNew) {
      flushAndRollOut()
    }
    BrokerExtensionResult()
  }

  private def flushAndRollOut(): Unit = {
    try {
      if (!monitorLogWriter.flushAndRun(fileMonitorLogWriteStrategy.rollOutAction())) {
        throw new IllegalStateException("Monitor log commit failed")
      }
    } catch {
      case error: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new IllegalStateException("Monitor log flush and rollout interrupted", error)
    }
  }
}
