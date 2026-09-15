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

import kafka.network.RequestChannel
import org.apache.kafka.common.acl.AclOperation.{CLUSTER_ACTION, READ}
import org.apache.kafka.common.errors.{NotCoordinatorException, TopicAuthorizationException, UnknownTopicIdException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractResponse, LookupGlobalSequenceRequest, ReadGlobalSequenceIndexRequest, LookupGlobalSequenceResponse, ReadGlobalSequenceIndexResponse}
import org.apache.kafka.common.resource.ResourceType.TOPIC
import org.apache.kafka.coordinator.globalsequence.{GlobalSequenceCoordinator, GlobalSequenceLookup}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PartitionKey
import org.apache.kafka.metadata.MetadataCache

import java.util.concurrent.CompletableFuture

/** Client lookup uses topic READ; the separate internal RPC requires CLUSTER_ACTION. */
class GlobalSequenceLookupApis(coordinator: GlobalSequenceCoordinator, router: IndexRoutingManager,
                              metadata: MetadataCache, auth: AuthHelper, helper: RequestHandlerHelper) {
  private def owned(response: AbstractResponse, lease: Option[org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources#Lease]): AbstractResponse = {
    def release(): Unit = lease.foreach(_.finish(response.errorCounts().keySet().stream()
      .filter(error => error != Errors.NONE).findFirst().orElse(Errors.NONE).exception()))
    response match {
      case page: LookupGlobalSequenceResponse => new LookupGlobalSequenceResponse(page.data()) with RequestChannel.ResourceResponse {
        override def release(): Unit = lease.foreach(_.finish(Errors.forCode(page.data().errorCode()).exception()))
      }
      case page: ReadGlobalSequenceIndexResponse => new ReadGlobalSequenceIndexResponse(page.data()) with RequestChannel.ResourceResponse {
        override def release(): Unit = lease.foreach(_.finish(Errors.forCode(page.data().errorCode()).exception()))
      }
      case _ => release(); response
    }
  }

  def lookup(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val body = request.body[LookupGlobalSequenceRequest]
    val data = body.data()
    var lease: Option[org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources#Lease] = None
    def send(response: AbstractResponse): Unit = {
      try helper.sendMaybeThrottle(request, owned(response, lease))
      catch { case error: Exception => lease.foreach(_.finish(error)); throw error }
    }
    try {
      val name = metadata.getTopicName(data.topicId())
      if (name.isEmpty) throw new UnknownTopicIdException("Unknown global sequence topic UUID")
      if (!auth.authorize(request.context, READ, TOPIC, name.get())) throw new TopicAuthorizationException(name.get())
      val lookup = new GlobalSequenceLookup.Request(data.topicId(), data.globalStartOffset(),
        data.globalEndOffsetExclusive(), data.maxBatches(), data.timeoutMs())
      lease = Option(router.reserveLookupResponse(data.topicId())).flatten
      router.lookupIndex(lookup).handle[Unit] { (routed, error) =>
        val response = if (error != null) body.getErrorResponse(error)
        else if (!router.isCurrent(new PartitionKey(data.topicId(), 0), routed.coordinator))
          body.getErrorResponse(new NotCoordinatorException("Global lookup route changed before response"))
        else GlobalSequenceProtocol.publicLookupResponse(lookup, routed.value)
        send(response)
      }
    } catch {
      case error: Exception =>
        send(body.getErrorResponse(error))
        CompletableFuture.completedFuture[Unit](())
    }
  }

  def readIndex(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val body = request.body[ReadGlobalSequenceIndexRequest]
    val data = body.data()
    var lease: Option[org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources#Lease] = None
    def send(raw: AbstractResponse): Unit = {
      val response = owned(raw, lease)
      try {
        if (response.errorCounts().containsKey(Errors.CLUSTER_AUTHORIZATION_FAILED)) helper.sendMaybeThrottle(request, response)
        else helper.sendResponseExemptThrottle(request, response, None)
      } catch { case error: Exception => lease.foreach(_.finish(error)); throw error }
    }
    try {
      auth.authorizeClusterOperation(request, CLUSTER_ACTION)
      val lookup = new GlobalSequenceLookup.Request(data.topicId(), data.globalStartOffset(),
        data.globalEndOffsetExclusive(), data.maxBatches(), data.timeoutMs())
      lease = Option(router.reserveLookupResponse(data.topicId())).flatten
      coordinator.lookupIndex(lookup, data.coordinatorLeaderEpoch()).handle[Unit] { (result, error) =>
        send(if (error != null) body.getErrorResponse(error) else GlobalSequenceProtocol.internalLookupResponse(lookup, result))
      }
    } catch {
      case error: Exception =>
        send(body.getErrorResponse(error))
        CompletableFuture.completedFuture[Unit](())
    }
  }
}
