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

import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.{CoordinatorNotAvailableException, DisconnectException, InvalidRequestException}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.util.{InterBrokerSendThread, RequestAndCompletionHandler}

import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ConcurrentLinkedQueue}
import scala.jdk.CollectionConverters._

private[server] trait GlobalSequenceTransport extends AutoCloseable {
  def startup(): Unit
  def send(node: Node, request: AbstractRequest.Builder[_ <: AbstractRequest]): CompletableFuture[AbstractResponse]
}

/** Uses the broker's inter-broker security configuration through NetworkUtils. No controller forwarding. */
private[server] class GlobalSequenceNetworkClient(client: KafkaClient, requestTimeoutMs: Int, time: Time)
  extends GlobalSequenceTransport {
  private case class Pending(node: Node, request: AbstractRequest.Builder[_ <: AbstractRequest],
                             createdMs: Long, future: CompletableFuture[AbstractResponse])
  private val queued = new ConcurrentLinkedQueue[Pending]()
  private val outstanding = ConcurrentHashMap.newKeySet[CompletableFuture[AbstractResponse]]()
  @volatile private var started = false
  @volatile private var closed = false

  private val destinations = scala.collection.mutable.Map.empty[Int, Node]

  private val sender = new InterBrokerSendThread("global-sequence-send-thread", client, requestTimeoutMs, time) {
    override def generateRequests(): java.util.Collection[RequestAndCompletionHandler] = {
      val requests = new java.util.ArrayList[RequestAndCompletionHandler]()
      var next = queued.poll()
      while (next != null) {
        val pending = next
        if (!closed && !pending.future.isDone) {
          destinations.get(pending.node.id()).filter(_ != pending.node).foreach(_ => client.disconnect(pending.node.idString()))
          destinations.put(pending.node.id(), pending.node)
          requests.add(new RequestAndCompletionHandler(pending.createdMs, pending.node, pending.request, response => {
            if (response.authenticationException() != null) pending.future.completeExceptionally(response.authenticationException())
            else if (response.versionMismatch() != null) pending.future.completeExceptionally(response.versionMismatch())
            else if (response.wasDisconnected() || response.wasTimedOut()) pending.future.completeExceptionally(DisconnectException.INSTANCE)
            else if (response.responseBody() == null) pending.future.completeExceptionally(new InvalidRequestException("Missing global sequence RPC response"))
            else pending.future.complete(response.responseBody())
          }))
        }
        next = queued.poll()
      }
      requests
    }
  }

  override def startup(): Unit = synchronized {
    if (closed) throw new IllegalStateException("Global sequence transport is closed")
    if (!started) {
      sender.start()
      started = true
    }
  }

  override def send(node: Node, request: AbstractRequest.Builder[_ <: AbstractRequest]): CompletableFuture[AbstractResponse] = {
    val future = new CompletableFuture[AbstractResponse]()
    outstanding.add(future)
    future.whenComplete((_, _) => outstanding.remove(future))
    if (closed || !started) future.completeExceptionally(new CoordinatorNotAvailableException("Global sequence transport is not running"))
    else {
      val pending = Pending(node, request, time.milliseconds(), future)
      queued.add(pending)
      future.whenComplete((_, _) => queued.remove(pending))
      // Recheck after publication: close may have drained the queue just before this add.
      if (closed) future.completeExceptionally(new CoordinatorNotAvailableException("Global sequence transport is closed"))
      else sender.wakeup()
    }
    future
  }

  override def close(): Unit = {
    val stop = synchronized {
      if (closed) return
      closed = true
      started
    }
    outstanding.asScala.foreach(_.completeExceptionally(new CoordinatorNotAvailableException("Global sequence transport is closed")))
    queued.clear()
    if (stop) sender.shutdown() else client.close()
  }
}
