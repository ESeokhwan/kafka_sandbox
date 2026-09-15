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
import kafka.server.QuotaFactory.QuotaManagers
import org.apache.kafka.common.acl.AclOperation.{CLUSTER_ACTION, READ}
import org.apache.kafka.common.errors.{RecordTooLargeException, TopicAuthorizationException, UnknownTopicIdException}
import org.apache.kafka.common.protocol.{Errors, ObjectSerializationCache}
import org.apache.kafka.common.requests.{AbstractResponse, FetchGlobalSequenceRequest, FetchGlobalSequenceResponse, ReadGlobalSequenceDataRequest, ReadGlobalSequenceDataResponse}
import org.apache.kafka.common.resource.ResourceType.TOPIC
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.MetadataCache

import java.util.concurrent.CompletableFuture

class GlobalSequenceFetchApis(manager: GlobalSequenceFetchManager, metadata: MetadataCache,
                              auth: AuthHelper, helper: RequestHandlerHelper, channel: RequestChannel,
                              quotas: QuotaManagers, time: Time) {
  def fetch(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val body = request.body[FetchGlobalSequenceRequest]
    def send(response: FetchGlobalSequenceResponse): Unit = try {
      val size = response.data().size(new ObjectSerializationCache(), request.header.apiVersion())
      if (size > GlobalSequenceFetch.MaxResponseBytes) {
        release(response)
        helper.sendMaybeThrottle(request, body.getErrorResponse(new RecordTooLargeException("Global fetch response exceeds 16 MiB")))
        return
      }
      val now = time.milliseconds()
      val requestThrottle = quotas.request.maybeRecordAndGetThrottleTimeMs(request, now)
      val bandwidthThrottle = quotas.fetch.maybeRecordAndGetThrottleTimeMs(request, size, now)
      val throttle = math.max(requestThrottle, bandwidthThrottle)
      request.apiThrottleTimeMs = throttle
      if (throttle > 0) helper.throttle(if (bandwidthThrottle > requestThrottle) quotas.fetch else quotas.request, request, throttle)
      // Send this bounded page and charge its bytes even when muting the channel. Its cursor remains usable.
      response.maybeSetThrottleTimeMs(throttle)
      channel.sendResponse(request, response, None)
    } catch {
      case error: Exception =>
        release(response)
        throw error
    }
    try {
      val name = metadata.getTopicName(body.data().topicId())
      if (name.isEmpty) throw new UnknownTopicIdException("Unknown global sequence topic UUID")
      if (!auth.authorize(request.context, READ, TOPIC, name.get())) throw new TopicAuthorizationException(name.get())
      val fetch = GlobalSequenceFetch.request(body.data(), body.version())
      manager.fetch(fetch).handle[Unit] { (response, error) =>
        send(if (error == null) response else body.getErrorResponse(0, error))
      }
    } catch {
      case error: Exception =>
        send(body.getErrorResponse(0, error))
        CompletableFuture.completedFuture[Unit](())
    }
  }

  private def release(response: AbstractResponse): Unit = response match {
    case resource: RequestChannel.ResourceResponse => resource.release()
    case _ =>
  }

  def readData(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val body = request.body[ReadGlobalSequenceDataRequest]
    var lease: Option[org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources#Lease] = None
    def send(original: ReadGlobalSequenceDataResponse): Unit = {
      val response = new ReadGlobalSequenceDataResponse(original.data()) with RequestChannel.ResourceResponse {
        override def release(): Unit = lease.foreach(_.finish(Errors.forCode(original.data().errorCode()).exception()))
      }
      try {
        if (response.errorCounts().containsKey(Errors.CLUSTER_AUTHORIZATION_FAILED)) helper.sendMaybeThrottle(request, response)
        else helper.sendResponseExemptThrottle(request, response, None)
      } catch { case error: Exception => release(response); throw error }
    }
    try {
      auth.authorizeClusterOperation(request, CLUSTER_ACTION)
      val isolation = GlobalSequenceFetch.isolation(body.data().isolationLevel(), body.version())
      val batch = GlobalSequenceFetch.physical(body.data())
      lease = Option(manager.reserveDataResponse(batch.partition())).flatten
      val deadline = time.nanoseconds() + body.data().timeoutMs().toLong * 1000000L
      manager.readLocal(batch, body.data().sourceLeaderEpoch(), deadline, isolation).handle[Unit] { (data, error) =>
        send(if (error == null) GlobalSequenceFetch.dataResponse(batch, data) else body.getErrorResponse(0, error))
      }
    } catch {
      case error: Exception =>
        send(body.getErrorResponse(0, error))
        CompletableFuture.completedFuture[Unit](())
    }
  }
}
