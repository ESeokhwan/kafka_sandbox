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

import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.{IsolationLevel, TopicIdPartition, Uuid}
import org.apache.kafka.coordinator.globalsequence.GlobalSequencePhysicalFetchRequest
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.storage.log.{FetchParams, FetchPartitionData}
import org.junit.jupiter.api.Assertions.{assertEquals, assertInstanceOf}
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}

import java.util.concurrent.CompletionException
import java.util.{Optional, OptionalInt, OptionalLong}
import scala.collection.Seq

class GlobalSequenceDataReaderTest {
  private val topicId = Uuid.fromString("AAAAAAAAAAAAAAAAAAAAAQ")
  private val replicaManager = mock(classOf[ReplicaManager])
  private val metadataCache = mock(classOf[MetadataCache])
  private val reader = new GlobalSequenceDataReader(replicaManager, metadataCache)
  private val request = new GlobalSequencePhysicalFetchRequest(
    topicId,
    1,
    42L,
    2,
    1024,
    IsolationLevel.READ_COMMITTED
  )

  @Test
  def testReadsExactIndexedBatchAndAbortedTransactions(): Unit = {
    val records = MemoryRecords.withRecords(
      42L,
      Compression.NONE,
      new SimpleRecord("one".getBytes),
      new SimpleRecord("two".getBytes)
    )
    val abortedTransaction = new FetchResponseData.AbortedTransaction()
      .setProducerId(7L)
      .setFirstOffset(42L)
    respondWith(new FetchPartitionData(
      Errors.NONE,
      44L,
      0L,
      records,
      Optional.empty(),
      OptionalLong.of(44L),
      Optional.of(java.util.List.of(abortedTransaction)),
      OptionalInt.empty(),
      false
    ))

    val result = reader.fetch(request).join()

    val batch = result.records.batches.iterator().next()
    assertEquals(42L, batch.baseOffset())
    assertEquals(44L, batch.nextOffset())
    assertEquals(1, result.abortedTransactions.size())
    assertEquals(7L, result.abortedTransactions.get(0).producerId())
  }

  @Test
  def testMissingIndexedBatchReturnsOffsetOutOfRange(): Unit = {
    respondWith(new FetchPartitionData(
      Errors.NONE,
      44L,
      44L,
      MemoryRecords.EMPTY,
      Optional.empty(),
      OptionalLong.empty(),
      Optional.empty(),
      OptionalInt.empty(),
      false
    ))

    val exception = org.junit.jupiter.api.Assertions.assertThrows(
      classOf[CompletionException],
      () => reader.fetch(request).join()
    )

    assertInstanceOf(classOf[OffsetOutOfRangeException], exception.getCause)
  }

  private def respondWith(responseData: FetchPartitionData): Unit = {
    when(metadataCache.getTopicName(topicId)).thenReturn(Optional.of("data-topic"))
    when(replicaManager.fetchMessages(
      any[FetchParams](),
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]](),
      any[ReplicaQuota](),
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer(invocation => {
      val callback = invocation.getArgument(3)
        .asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      val topicIdPartition = invocation.getArgument(1)
        .asInstanceOf[Seq[(TopicIdPartition, FetchRequest.PartitionData)]]
        .head
        ._1
      callback(Seq(topicIdPartition -> responseData))
    })
  }
}
