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
import java.util.concurrent.atomic.AtomicInteger

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
private[server] class GlobalSequenceNetworkClient(client: KafkaClient, requestTimeoutMs: Int, time: Time,
                                                 resources: GlobalSequenceResources = null, data: Boolean = false)
  extends GlobalSequenceTransport {
  private case class Pending(node: Node, request: AbstractRequest.Builder[_ <: AbstractRequest],
                             createdMs: Long, future: CompletableFuture[AbstractResponse],
                             lease: Option[GlobalSequenceResources#Lease]) {
    val state = new AtomicInteger(0)
    def release(): Unit = if (state.getAndSet(2) != 2) {
      outstanding.remove(this)
      lease.foreach(_.close())
    }
  }
  private val queued = new ConcurrentLinkedQueue[Pending]()
  private val outstanding = ConcurrentHashMap.newKeySet[Pending]()
  @volatile private var started = false
  @volatile private var closed = false

  private val destinations = scala.collection.mutable.Map.empty[Int, Node]

  private val sender = new InterBrokerSendThread(if (data) "global-sequence-data-send-thread" else "global-sequence-send-thread", client, requestTimeoutMs, time) {
    override def generateRequests(): java.util.Collection[RequestAndCompletionHandler] = {
      val requests = new java.util.ArrayList[RequestAndCompletionHandler]()
      var next = queued.poll()
      while (next != null) {
        val pending = next
        if (!closed && pending.state.compareAndSet(0, 1)) {
          try {
            destinations.get(pending.node.id()).filter(_ != pending.node).foreach(_ => client.disconnect(pending.node.idString()))
            destinations.put(pending.node.id(), pending.node)
            requests.add(new RequestAndCompletionHandler(pending.createdMs, pending.node, pending.request, response => {
              try {
                if (response.authenticationException() != null) pending.future.completeExceptionally(response.authenticationException())
                else if (response.versionMismatch() != null) pending.future.completeExceptionally(response.versionMismatch())
                else if (response.wasDisconnected() || response.wasTimedOut()) pending.future.completeExceptionally(DisconnectException.INSTANCE)
                else if (response.responseBody() == null) pending.future.completeExceptionally(new InvalidRequestException("Missing global sequence RPC response"))
                else pending.future.complete(response.responseBody())
              } finally pending.release()
            }))
          } catch {
            case scala.util.control.NonFatal(error) =>
              pending.future.completeExceptionally(error)
              pending.release()
          }
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
    val lease = try Option(resources).map(_.acquire(if (data) Scope.DATA_RPC else Scope.INDEX_RPC,
      Int.box(node.id()), if (data) GlobalSequenceResources.BATCH_BYTES else 128L * 1024))
    catch { case scala.util.control.NonFatal(error) => return CompletableFuture.failedFuture(error) }
    val pending = Pending(node, request, time.milliseconds(), future, lease)
    outstanding.add(pending)
    future.whenComplete { (_, _) =>
      // Cancellation can remove queued work, but a dispatched request keeps its reservation
      // until KafkaClient calls its completion handler (or transport shutdown).
      if (pending.state.compareAndSet(0, 2)) {
        queued.remove(pending)
        outstanding.remove(pending)
        lease.foreach(_.close())
      }
    }
    if (closed || !started) future.completeExceptionally(new CoordinatorNotAvailableException("Global sequence transport is not running"))
    else {
      queued.add(pending)
      if (pending.state.get() == 2) queued.remove(pending)
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
    outstanding.asScala.foreach(_.future.completeExceptionally(new CoordinatorNotAvailableException("Global sequence transport is closed")))
    queued.clear()
    try { if (stop) sender.shutdown() else client.close() }
    finally outstanding.asScala.foreach(_.release())
  }
}
