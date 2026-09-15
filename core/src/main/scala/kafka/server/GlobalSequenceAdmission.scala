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

package kafka.server

import org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources.Scope

import java.util.concurrent.CompletableFuture
import scala.util.control.NonFatal

private[server] object GlobalSequenceAdmission {
  def run[T](resources: GlobalSequenceResources, scope: Scope, key: AnyRef, bytes: Long = 0L)
            (work: => CompletableFuture[T]): CompletableFuture[T] = {
    try {
      val lease = Option(resources).map(_.acquire(scope, key, bytes))
      try {
        val result = work
        result.whenComplete((_, error) => lease.foreach(_.finish(error)))
        result
      } catch {
        case NonFatal(error) =>
          lease.foreach(_.finish(error))
          CompletableFuture.failedFuture(error)
      }
    } catch { case NonFatal(error) => CompletableFuture.failedFuture(error) }
  }
}
