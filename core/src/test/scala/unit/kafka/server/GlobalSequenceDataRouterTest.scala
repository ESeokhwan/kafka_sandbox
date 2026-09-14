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

import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.common.IsolationLevel.{READ_COMMITTED, READ_UNCOMMITTED}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{FencedLeaderEpochException, InvalidRequestException, OffsetOutOfRangeException, TimeoutException, UnknownTopicIdException}
import org.apache.kafka.common.metadata.{ConfigRecord, PartitionRecord, RegisterBrokerRecord, TopicRecord}
import org.apache.kafka.common.metadata.RegisterBrokerRecord.{BrokerEndpoint, BrokerEndpointCollection}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests._
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{PartitionKey, PhysicalBatch}
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito.{mock, when}

import java.util.{List => JList}
import java.util.concurrent.CompletableFuture
import scala.collection.mutable.ArrayBuffer

class GlobalSequenceDataRouterTest {
  private val topicId = new Uuid(1, 2)
  private val batch = new PhysicalBatch(new PartitionKey(topicId, 0), 10, 11, 2)
  private val records = MemoryRecords.withRecords(10L, Compression.NONE, new SimpleRecord(Array[Byte](1)), new SimpleRecord(Array[Byte](2)))
  private def data(epoch: Int = 3): GlobalSequenceFetch.Data = GlobalSequenceFetch.Data(records, epoch, 12)
  private def image(leader: Int = 2, epoch: Int = 3, sourceId: Uuid = topicId,
                    enabled: Boolean = true, endpoint: Boolean = true): MetadataImage = {
    val delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build()
    delta.replay(new TopicRecord().setName("ordered").setTopicId(sourceId))
    delta.replay(new PartitionRecord().setTopicId(sourceId).setPartitionId(0).setLeader(leader).setLeaderEpoch(epoch)
      .setReplicas(JList.of(1, 2)).setIsr(JList.of(1, 2)))
    delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName("ordered")
      .setName(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG).setValue(enabled.toString))
    for (broker <- 1 to 2) {
      val endpoints = new BrokerEndpointCollection()
      if (endpoint) endpoints.add(new BrokerEndpoint().setName("PLAINTEXT").setHost("localhost").setPort(9000 + broker).setSecurityProtocol(0.toShort))
      delta.replay(new RegisterBrokerRecord().setBrokerId(broker).setBrokerEpoch(7).setIncarnationId(new Uuid(9, broker))
        .setFenced(false).setEndPoints(endpoints))
    }
    delta.apply(MetadataProvenance.EMPTY)
  }

  private case class Sent(node: Node, request: AbstractRequest, future: CompletableFuture[AbstractResponse])
  private class Transport extends GlobalSequenceTransport {
    val sent = ArrayBuffer.empty[Sent]
    var closed = false
    override def startup(): Unit = ()
    override def send(node: Node, request: AbstractRequest.Builder[_ <: AbstractRequest]): CompletableFuture[AbstractResponse] = {
      val future = new CompletableFuture[AbstractResponse]()
      sent += Sent(node, request.build(), future)
      future
    }
    override def close(): Unit = { closed = true }
  }

  private class Context(initial: MetadataImage = image()) extends AutoCloseable {
    val time = new MockTime(0, 0)
    var metadata = initial
    val reader: GlobalSequenceDataReader = mock(classOf[GlobalSequenceDataReader])
    val transport = new Transport
    val router = new GlobalSequenceDataRouter(1, new ListenerName("PLAINTEXT"), () => metadata, reader, transport, time.scheduler, time)
    def read(): CompletableFuture[GlobalSequenceFetch.Data] = router.read(batch, 1000000000L)
    override def close(): Unit = { router.close(); time.scheduler.clear() }
  }

  @Test
  def testLocalAndRemoteReadValidateTheMappedBatchAndEpoch(): Unit = {
    val local = new Context(image(leader = 1))
    try {
      when(local.reader.read(batch, 3, 1000000000L, READ_UNCOMMITTED)).thenReturn(CompletableFuture.completedFuture(data()))
      assertEquals(data(), local.read().join())
      assertTrue(local.transport.sent.isEmpty)
    } finally local.close()
    val remote = new Context
    try {
      val result = remote.read()
      val request = remote.transport.sent.head.request.asInstanceOf[ReadGlobalSequenceDataRequest].data()
      assertEquals(batch, GlobalSequenceFetch.physical(request))
      assertEquals(3, request.sourceLeaderEpoch())
      remote.transport.sent.head.future.complete(GlobalSequenceFetch.dataResponse(batch, data()))
      assertEquals(records.buffer(), result.join().records.buffer())
    } finally remote.close()
    assertFalse(remote.transport.closed, "Data routing must not close the shared index transport")
  }

  @Test
  def testSourceLeaderMoveRetriesWithRemainingDeadline(): Unit = {
    val c = new Context
    try {
      val result = c.read()
      c.metadata = image(leader = 1, epoch = 4)
      when(c.reader.read(any(), anyInt(), anyLong(), any())).thenReturn(CompletableFuture.completedFuture(data(4)))
      c.transport.sent.head.future.complete(GlobalSequenceFetch.dataResponse(batch, data()))
      assertFalse(result.isDone)
      c.time.sleep(100)
      assertEquals(4, result.join().sourceLeaderEpoch)
    } finally c.close()
    val fenced = new Context
    try {
      val result = fenced.read()
      fenced.transport.sent.head.future.completeExceptionally(new FencedLeaderEpochException("moved"))
      fenced.metadata = image(epoch = 4)
      fenced.time.sleep(100)
      val retry = fenced.transport.sent.last.request.asInstanceOf[ReadGlobalSequenceDataRequest].data()
      assertEquals(4, retry.sourceLeaderEpoch())
      assertEquals(900, retry.timeoutMs())
      fenced.transport.sent.last.future.complete(GlobalSequenceFetch.dataResponse(batch, data(4)))
      assertEquals(4, result.join().sourceLeaderEpoch)
    } finally fenced.close()
  }

  @Test
  def testRetentionFailureAndTopicRecreationAreTerminal(): Unit = {
    val c = new Context
    try {
      val result = c.read()
      c.transport.sent.head.future.completeExceptionally(new OffsetOutOfRangeException("expired"))
      assertFutureThrows(classOf[OffsetOutOfRangeException], result)
      c.time.sleep(100)
      assertEquals(1, c.transport.sent.size)
    } finally c.close()
    val recreated = new Context(image(sourceId = new Uuid(5, 6)))
    try {
      assertFutureThrows(classOf[UnknownTopicIdException], recreated.read())
      assertTrue(recreated.transport.sent.isEmpty)
    } finally recreated.close()
  }

  @Test
  def testTimeoutAndCancellationDetachRemoteWork(): Unit = {
    val c = new Context
    try {
      val result = c.read()
      c.time.sleep(1000)
      assertFutureThrows(classOf[TimeoutException], result)
      assertTrue(c.transport.sent.head.future.isCancelled)
    } finally c.close()
    val cancelled = new Context
    try {
      val result = cancelled.read()
      result.cancel(false)
      assertTrue(cancelled.transport.sent.head.future.isCancelled)
    } finally cancelled.close()
  }

  @Test
  def testProtocolRejectsDifferentUuidEpochHwAndRecords(): Unit = {
    for (field <- Seq("uuid", "epoch", "hw", "records")) {
      val reply = GlobalSequenceFetch.dataResponse(batch, data())
      field match {
        case "uuid" => reply.data().setTopicId(Uuid.ZERO_UUID)
        case "epoch" => reply.data().setSourceLeaderEpoch(4)
        case "hw" => reply.data().setDataHighWatermark(11)
        case "records" => reply.data().setRecords(MemoryRecords.EMPTY)
      }
      assertThrows(classOf[org.apache.kafka.common.KafkaException], () => GlobalSequenceFetch.decode(batch, 3, reply))
    }
    val disabled = new Context(image(enabled = false))
    try assertFutureThrows(classOf[InvalidRequestException], disabled.read()) finally disabled.close()
  }

  @Test
  def testCommittedRemoteReadsRequireVersionOneAndPreservePendingAndAbortDecisions(): Unit = {
    for (status <- Seq(GlobalSequenceFetch.Pending, GlobalSequenceFetch.Aborted, GlobalSequenceFetch.Visible)) {
      val c = new Context
      try {
        val future = c.router.read(batch, 1000000000L, READ_COMMITTED)
        val request = c.transport.sent.head.request.asInstanceOf[ReadGlobalSequenceDataRequest]
        assertEquals(1.toShort, request.version())
        assertEquals(1.toByte, request.data().isolationLevel())
        val answer = GlobalSequenceFetch.Data(if (status == GlobalSequenceFetch.Visible) records else MemoryRecords.EMPTY,
          3, 12, if (status == GlobalSequenceFetch.Pending) 10 else 12, status)
        c.transport.sent.head.future.complete(GlobalSequenceFetch.dataResponse(batch, answer))
        assertEquals(answer, future.join())
        assertEquals(1, c.transport.sent.size, "Pending is a normal page boundary, not a routing error to retry")
      } finally c.close()
    }
  }

  @Test
  def testCommittedResponsesCannotOmitOrContradictIsolationEvidence(): Unit = {
    for (field <- Seq("lso", "status", "payload", "base", "last", "count", "pendingBelowLso", "visibleAtLso")) {
      val response = GlobalSequenceFetch.dataResponse(batch, GlobalSequenceFetch.Data(MemoryRecords.EMPTY, 3, 12, 12, GlobalSequenceFetch.Aborted))
      field match {
        case "lso" => response.data().setLastStableOffset(-1)
        case "status" => response.data().setReadStatus(3.toByte)
        case "payload" => response.data().setRecords(records)
        case "base" => response.data().setPhysicalBaseOffset(9)
        case "last" => response.data().setPhysicalLastOffset(12)
        case "count" => response.data().setRecordCount(1)
        case "pendingBelowLso" => response.data().setReadStatus(GlobalSequenceFetch.Pending)
        case "visibleAtLso" => response.data().setReadStatus(GlobalSequenceFetch.Visible).setRecords(records).setLastStableOffset(10)
      }
      assertThrows(classOf[InvalidRequestException], () => { GlobalSequenceFetch.decode(batch, 3, response, READ_COMMITTED); () }, field)
    }
    val old = GlobalSequenceFetch.dataResponse(batch, data())
    assertThrows(classOf[InvalidRequestException], () => GlobalSequenceFetch.decode(batch, 3, old, READ_COMMITTED))
    val builder = GlobalSequenceFetch.dataRequest(batch, 3, 1000, READ_COMMITTED)
    assertEquals(1.toShort, builder.oldestAllowedVersion())
    assertThrows(classOf[org.apache.kafka.common.errors.UnsupportedVersionException], () => builder.build(0.toShort))
  }

  @Test
  def testVersionZeroKeepsOriginalWirePayloadAndVersionOneCarriesIsolationDecisions(): Unit = {
    val ru = GlobalSequenceFetch.dataResponse(batch, data().copy(lastStableOffset = 12))
    val oldData = new org.apache.kafka.common.message.ReadGlobalSequenceDataResponseData(
      org.apache.kafka.common.protocol.MessageUtil.toByteBufferAccessor(ru.data(), 0.toShort), 0.toShort)
    assertEquals(-1L, oldData.lastStableOffset())
    assertEquals(-1L, oldData.physicalBaseOffset())
    assertEquals(records, GlobalSequenceFetch.decode(batch, 3, new ReadGlobalSequenceDataResponse(oldData)).records)
    val pending = GlobalSequenceFetch.dataResponse(batch,
      GlobalSequenceFetch.Data(MemoryRecords.EMPTY, 3, 12, 10, GlobalSequenceFetch.Pending))
    val current = new org.apache.kafka.common.message.ReadGlobalSequenceDataResponseData(
      org.apache.kafka.common.protocol.MessageUtil.toByteBufferAccessor(pending.data(), 1.toShort), 1.toShort)
    assertEquals(10L, current.physicalBaseOffset())
    assertEquals(GlobalSequenceFetch.Pending,
      GlobalSequenceFetch.decode(batch, 3, new ReadGlobalSequenceDataResponse(current), READ_COMMITTED).status)
    val publicResponse = new org.apache.kafka.common.message.FetchGlobalSequenceResponseData().setTopicId(topicId)
      .setTransactionPending(true).setNextGlobalOffset(7)
    val parsed = new org.apache.kafka.common.message.FetchGlobalSequenceResponseData(
      org.apache.kafka.common.protocol.MessageUtil.toByteBufferAccessor(publicResponse, 1.toShort), 1.toShort)
    assertTrue(parsed.transactionPending())
    assertEquals(7L, parsed.nextGlobalOffset())
  }
}
