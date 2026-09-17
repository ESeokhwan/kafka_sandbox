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

import org.apache.kafka.common.errors.InvalidRequestException

import java.util
import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.util.control.NonFatal

/** Adds Produce-specific optional counter reset behavior around the generic rollout handler. */
final class ProduceRequestMonitorLogRollOutExtensionHandler(
  delegate: MonitorLogRollOutExtensionHandler,
  resetAction: () => Unit,
  maxRememberedRequestIds: Int = 1024
) extends BrokerExtensionHandler {
  override val target: String = "produce-request-throughput-rollout"

  private val requestIds = new util.LinkedHashMap[String, java.lang.Boolean]() {
    override def removeEldestEntry(eldest: util.Map.Entry[String, java.lang.Boolean]): Boolean =
      size() > maxRememberedRequestIds
  }

  require(delegate != null, "delegate must not be null")
  require(resetAction != null, "resetAction must not be null")
  require(maxRememberedRequestIds > 0, "maxRememberedRequestIds must be positive")

  override def handle(command: BrokerExtensionCommand): CompletionStage[BrokerExtensionResult] = {
    val needReset = command.operation match {
      case "flush-and-rollout" => false
      case "flush-and-rollout-with-reset" => true
      case operation =>
        return CompletableFuture.failedFuture(
          new InvalidRequestException(s"Unknown $target operation: $operation"))
    }

    val requestId = command.requestId.toString
    val isNew = requestIds.synchronized {
      if (requestIds.containsKey(requestId)) false
      else {
        requestIds.put(requestId, java.lang.Boolean.TRUE)
        true
      }
    }
    if (!isNew) {
      return CompletableFuture.completedFuture(BrokerExtensionResult())
    }

    val delegatedCommand = command.copy(operation = "flush-and-rollout")
    try {
      delegate.handle(delegatedCommand).thenApply { result =>
        if (needReset) {
          resetAction()
        }
        result
      }
    } catch {
      case NonFatal(error) =>
        CompletableFuture.failedFuture(error)
    }
  }

  def shutdown(): Unit = delegate.shutdown()
}
