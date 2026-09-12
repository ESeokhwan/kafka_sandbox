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
import org.apache.kafka.common.acl.AclOperation.CLUSTER_ACTION
import org.apache.kafka.common.message.{AppendGlobalSequenceIndexResponseData, DescribeGlobalSequencePartitionResponseData, RegisterGlobalSequenceIndexerResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, AppendGlobalSequenceIndexRequest, AppendGlobalSequenceIndexResponse, DescribeGlobalSequencePartitionRequest, DescribeGlobalSequencePartitionResponse, RegisterGlobalSequenceIndexerRequest, RegisterGlobalSequenceIndexerResponse}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinator
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{AppendRequest, IndexerIdentity, PartitionKey, PhysicalBatch, RegistrationRequest}

import java.util.concurrent.CompletableFuture

/** Broker-only RPCs. Every path authorizes CLUSTER_ACTION before constructing or scheduling an operation. */
class GlobalSequenceApis(
  coordinator: GlobalSequenceCoordinator,
  authHelper: AuthHelper,
  requestHelper: RequestHandlerHelper
) {
  private def execute[T](request: RequestChannel.Request, body: AbstractRequest)
                        (operation: => CompletableFuture[T])
                        (response: T => AbstractResponse): CompletableFuture[Unit] = {
    try {
      authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
      operation.handle[Unit] { (result, exception) =>
        val reply = if (exception == null) response(result) else body.getErrorResponse(exception)
        send(request, reply)
      }
    } catch {
      case exception: Exception =>
        send(request, body.getErrorResponse(exception))
        CompletableFuture.completedFuture[Unit](())
    }
  }

  private def send(request: RequestChannel.Request, response: AbstractResponse): Unit = {
    if (response.errorCounts().containsKey(Errors.CLUSTER_AUTHORIZATION_FAILED))
      requestHelper.sendMaybeThrottle(request, response)
    else
      requestHelper.sendResponseExemptThrottle(request, response, None)
  }

  def registerIndexer(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val body = request.body[RegisterGlobalSequenceIndexerRequest]
    val data = body.data()
    execute(request, body) {
      coordinator.registerIndexer(new RegistrationRequest(new PartitionKey(data.topicId(), data.partition()),
        data.sourceBrokerId(), data.sourceLeaderEpoch(), data.expectedGeneration(), data.registrationId()),
        data.coordinatorLeaderEpoch())
    } { result =>
      val response = new RegisterGlobalSequenceIndexerResponseData()
        .setTopicId(data.topicId()).setPartition(data.partition()).setRegistered(result.registered())
        .setCoordinatorLeaderEpoch(result.coordinatorLeaderEpoch())
      result.indexer().ifPresent { indexer =>
        response.setSourceBrokerId(indexer.brokerId()).setSourceLeaderEpoch(indexer.leaderEpoch())
          .setIndexerGeneration(indexer.generation()).setRegistrationId(indexer.registrationId())
      }
      new RegisterGlobalSequenceIndexerResponse(response)
    }
  }

  def describePartition(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val body = request.body[DescribeGlobalSequencePartitionRequest]
    val data = body.data()
    execute(request, body) {
      coordinator.describePartition(new PartitionKey(data.topicId(), data.partition()))
    } { result =>
      val response = new DescribeGlobalSequencePartitionResponseData().setTopicId(data.topicId()).setPartition(data.partition())
      result.committedProgress().ifPresent { batch =>
        response.setPhysicalBaseOffset(batch.baseOffset()).setPhysicalLastOffset(batch.lastOffset()).setRecordCount(batch.recordCount())
      }
      result.currentIndexer().ifPresent { indexer =>
        response.setSourceBrokerId(indexer.brokerId()).setSourceLeaderEpoch(indexer.leaderEpoch())
          .setIndexerGeneration(indexer.generation()).setRegistrationId(indexer.registrationId())
      }
      new DescribeGlobalSequencePartitionResponse(response)
    }
  }

  def appendIndex(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val body = request.body[AppendGlobalSequenceIndexRequest]
    val data = body.data()
    execute(request, body) {
      val batch = new PhysicalBatch(new PartitionKey(data.topicId(), data.partition()),
        data.physicalBaseOffset(), data.physicalLastOffset(), data.recordCount())
      val indexer = new IndexerIdentity(data.sourceBrokerId(), data.sourceLeaderEpoch(), data.indexerGeneration(), data.registrationId())
      coordinator.appendIndex(new AppendRequest(batch, data.predecessorBaseOffset(), data.dataHighWatermark(), indexer),
        data.coordinatorLeaderEpoch())
    } { result =>
      val response = new AppendGlobalSequenceIndexResponseData().setTopicId(data.topicId()).setPartition(data.partition())
        .setPhysicalBaseOffset(result.batch().baseOffset()).setPhysicalLastOffset(result.batch().lastOffset()).setRecordCount(result.batch().recordCount())
        .setStatus(result.status().code()).setGlobalBaseOffset(result.globalBaseOffset().orElse(-1L))
        .setCoordinatorLeaderEpoch(result.coordinatorLeaderEpoch())
      result.indexedThrough().ifPresent { batch =>
        response.setIndexedThroughBaseOffset(batch.baseOffset()).setIndexedThroughLastOffset(batch.lastOffset())
          .setIndexedThroughRecordCount(batch.recordCount())
      }
      new AppendGlobalSequenceIndexResponse(response)
    }
  }
}
