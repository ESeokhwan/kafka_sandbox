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

import kafka.server.QuotaFactory.UNBOUNDED_QUOTA
import org.apache.kafka.common.errors.{OffsetOutOfRangeException, UnknownTopicIdException}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.{TopicIdPartition, TopicPartition}
import org.apache.kafka.coordinator.globalsequence.{GlobalSequenceAbortedTransaction, GlobalSequencePhysicalFetchRequest, GlobalSequencePhysicalFetchResult}
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}

import java.nio.ByteBuffer
import java.util.Optional
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/** Reads one indexed physical record batch from the local data leader. */
class GlobalSequenceDataReader(
  replicaManager: ReplicaManager,
  metadataCache: MetadataCache
) {

  def fetch(request: GlobalSequencePhysicalFetchRequest): CompletableFuture[GlobalSequencePhysicalFetchResult] = {
    val result = new CompletableFuture[GlobalSequencePhysicalFetchResult]()
    val topicName = metadataCache.getTopicName(request.topicId)
    if (topicName.isEmpty) {
      result.completeExceptionally(new UnknownTopicIdException(s"Unknown topic ID ${request.topicId}"))
      return result
    }

    val topicIdPartition = new TopicIdPartition(
      request.topicId,
      new TopicPartition(topicName.get, request.partitionIndex)
    )
    val partitionData = new FetchRequest.PartitionData(
      request.topicId,
      request.partitionBaseOffset,
      FetchRequest.INVALID_LOG_START_OFFSET,
      request.maxBytes,
      Optional.empty()
    )
    val fetchParams = new FetchParams(
      FetchRequest.CONSUMER_REPLICA_ID,
      -1L,
      0L,
      0,
      request.maxBytes,
      FetchIsolation.of(FetchRequest.CONSUMER_REPLICA_ID, request.isolationLevel),
      Optional.empty()
    )

    try {
      replicaManager.fetchMessages(
        params = fetchParams,
        fetchInfos = Seq(topicIdPartition -> partitionData),
        quota = UNBOUNDED_QUOTA,
        responseCallback = response => response.headOption match {
          case Some((_, data)) => complete(request, data, result)
          case None => result.completeExceptionally(missingBatch(request))
        }
      )
    } catch {
      case exception: Throwable => result.completeExceptionally(exception)
    }
    result
  }

  private def complete(
    request: GlobalSequencePhysicalFetchRequest,
    partitionData: FetchPartitionData,
    result: CompletableFuture[GlobalSequencePhysicalFetchResult]
  ): Unit = {
    if (partitionData.error != org.apache.kafka.common.protocol.Errors.NONE) {
      result.completeExceptionally(partitionData.error.exception())
      return
    }

    val batches = partitionData.records.batches().iterator()
    if (!batches.hasNext) {
      result.completeExceptionally(missingBatch(request))
      return
    }

    val batch = batches.next()
    val expectedEndOffset = Math.addExact(request.partitionBaseOffset, request.recordCount.toLong)
    if (batch.baseOffset() != request.partitionBaseOffset || batch.nextOffset() != expectedEndOffset) {
      result.completeExceptionally(missingBatch(request))
      return
    }

    val buffer = ByteBuffer.allocate(batch.sizeInBytes())
    batch.writeTo(buffer)
    buffer.flip()
    val records = MemoryRecords.readableRecords(buffer)
    val abortedTransactions = partitionData.abortedTransactions
      .orElse(java.util.List.of())
      .asScala
      .map(transaction => new GlobalSequenceAbortedTransaction(transaction.producerId, transaction.firstOffset))
      .toList
      .asJava
    result.complete(new GlobalSequencePhysicalFetchResult(records, abortedTransactions))
  }

  private def missingBatch(request: GlobalSequencePhysicalFetchRequest): OffsetOutOfRangeException =
    new OffsetOutOfRangeException(
      s"Indexed physical batch ${request.topicId}-${request.partitionIndex} at offset " +
        s"${request.partitionBaseOffset} is no longer available"
    )
}
