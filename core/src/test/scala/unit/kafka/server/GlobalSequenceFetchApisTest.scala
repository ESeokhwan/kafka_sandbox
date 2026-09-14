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
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.IsolationLevel.{READ_COMMITTED, READ_UNCOMMITTED}
import org.apache.kafka.common.acl.AclOperation.{CLUSTER_ACTION, READ}
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.message.{FetchGlobalSequenceRequestData, FetchGlobalSequenceResponseData, ReadGlobalSequenceDataRequestData}
import org.apache.kafka.common.protocol.{ApiKeys, Errors, ObjectSerializationCache}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.ResourceType.TOPIC
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.util.MockTime
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyDouble, anyInt, anyLong, eq => eqTo}
import org.mockito.Mockito.{doThrow, mock, never, verify, verifyNoInteractions, when}

import java.util.Optional
import java.util.concurrent.CompletableFuture

class GlobalSequenceFetchApisTest {
  private val topic = new Uuid(1, 2)
  private class Context {
    val manager: GlobalSequenceFetchManager = mock(classOf[GlobalSequenceFetchManager])
    val metadata: MetadataCache = mock(classOf[MetadataCache])
    val auth: AuthHelper = mock(classOf[AuthHelper])
    val helper: RequestHandlerHelper = mock(classOf[RequestHandlerHelper])
    val channel: RequestChannel = mock(classOf[RequestChannel])
    val request: RequestChannel.Request = mock(classOf[RequestChannel.Request])
    val bandwidth: ClientQuotaManager = mock(classOf[ClientQuotaManager])
    val requestQuota: ClientRequestQuotaManager = mock(classOf[ClientRequestQuotaManager])
    val quotas = new QuotaManagers(bandwidth, bandwidth, requestQuota, mock(classOf[ControllerMutationQuotaManager]),
      mock(classOf[ReplicationQuotaManager]), mock(classOf[ReplicationQuotaManager]), mock(classOf[ReplicationQuotaManager]), Optional.empty())
    val time = new MockTime(0, 0)
    val data = new FetchGlobalSequenceRequestData().setTopicId(topic).setGlobalEndOffsetExclusive(10)
    when(metadata.getTopicName(topic)).thenReturn(Optional.of("ordered"))
    when(auth.authorize(any(), eqTo(READ), eqTo(TOPIC), eqTo("ordered"), anyBoolean(), anyBoolean(), anyInt())).thenReturn(true)
    when(request.header).thenReturn(new RequestHeader(ApiKeys.FETCH_GLOBAL_SEQUENCE, 0.toShort, "client", 1))
    when(request.body[FetchGlobalSequenceRequest]).thenReturn(new FetchGlobalSequenceRequest.Builder(data).build())
    val response = new FetchGlobalSequenceResponse(new FetchGlobalSequenceResponseData().setTopicId(topic).setNextGlobalOffset(0))
    when(manager.fetch(any())).thenReturn(CompletableFuture.completedFuture(response))
    val apis = new GlobalSequenceFetchApis(manager, metadata, auth, helper, channel, quotas, time)
    def sent(): FetchGlobalSequenceResponse = {
      val captured = ArgumentCaptor.forClass(classOf[AbstractResponse])
      verify(channel).sendResponse(eqTo(request), captured.capture(), eqTo(None))
      captured.getValue.asInstanceOf[FetchGlobalSequenceResponse]
    }
  }

  @Test
  def testTopicReadAclPrecedesLookupAndPhysicalWork(): Unit = {
    val c = new Context
    when(c.auth.authorize(any(), eqTo(READ), eqTo(TOPIC), eqTo("ordered"), anyBoolean(), anyBoolean(), anyInt())).thenReturn(false)
    c.apis.fetch(c.request).join()
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), c.sent().data().errorCode())
    verifyNoInteractions(c.manager)
  }

  @Test
  def testUnknownUuidAndInvalidInputsNeverFetch(): Unit = {
    for (field <- Seq("uuid", "bytes", "range", "isolation", "timeout", "page")) {
      val c = new Context
      field match {
        case "uuid" => when(c.metadata.getTopicName(topic)).thenReturn(Optional.empty())
        case "bytes" => c.data.setMaxBytes(0)
        case "range" => c.data.setGlobalStartOffset(11)
        case "isolation" => c.data.setIsolationLevel(2.toByte)
        case "timeout" => c.data.setTimeoutMs(30001)
        case "page" => c.data.setMaxBatches(1001)
      }
      c.apis.fetch(c.request).join()
      val response = c.sent().data()
      assertEquals((if (field == "uuid") Errors.UNKNOWN_TOPIC_ID else Errors.INVALID_REQUEST).code(), response.errorCode())
      assertEquals(-1L, response.nextGlobalOffset())
      assertTrue(response.batches().isEmpty)
      verifyNoInteractions(c.manager)
    }
  }

  @Test
  def testBothQuotasAreChargedAndTheLargerThrottleMutesTheChannelWithoutLosingThePage(): Unit = {
    for ((requestMs, bytesMs) <- Seq((20, 50), (60, 10), (0, 0))) {
      val c = new Context
      when(c.requestQuota.maybeRecordAndGetThrottleTimeMs(any(), anyLong())).thenReturn(requestMs)
      when(c.bandwidth.maybeRecordAndGetThrottleTimeMs(any(), anyDouble(), anyLong())).thenReturn(bytesMs)
      c.apis.fetch(c.request).join()
      val response = c.sent()
      assertSame(c.response, response)
      assertEquals(math.max(requestMs, bytesMs), response.throttleTimeMs())
      val size = response.data().size(new ObjectSerializationCache(), 0.toShort)
      verify(c.bandwidth).maybeRecordAndGetThrottleTimeMs(c.request, size.toDouble, 0L)
      verify(c.requestQuota).maybeRecordAndGetThrottleTimeMs(c.request, 0L)
      verify(c.bandwidth, never()).unrecordQuotaSensor(any(), anyDouble(), anyLong())
      if (math.max(requestMs, bytesMs) > 0)
        verify(c.helper).throttle(if (bytesMs > requestMs) c.bandwidth else c.requestQuota, c.request, math.max(requestMs, bytesMs))
    }
  }

  @Test
  def testInternalPhysicalReadRequiresClusterActionBeforeValidationOrDiskWork(): Unit = {
    val c = new Context
    when(c.request.body[ReadGlobalSequenceDataRequest]).thenReturn(new ReadGlobalSequenceDataRequest.Builder(
      new ReadGlobalSequenceDataRequestData().setTopicId(topic)).build())
    doThrow(new ClusterAuthorizationException("denied")).when(c.auth).authorizeClusterOperation(c.request, CLUSTER_ACTION)
    c.apis.readData(c.request).join()
    val captured = ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(c.helper).sendMaybeThrottle(eqTo(c.request), captured.capture())
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), captured.getValue.asInstanceOf[ReadGlobalSequenceDataResponse].data().errorCode())
    verifyNoInteractions(c.manager)
  }

  @Test
  def testInternalReadUsesRemainingDeadlineAndExemptQuotaAfterAuthorization(): Unit = {
    val c = new Context
    val data = new ReadGlobalSequenceDataRequestData().setTopicId(topic).setPartition(1)
      .setPhysicalBaseOffset(10).setPhysicalLastOffset(10).setRecordCount(1).setSourceLeaderEpoch(3).setTimeoutMs(400)
    val batch = GlobalSequenceFetch.physical(data)
    val records = org.apache.kafka.common.record.MemoryRecords.withRecords(10L, org.apache.kafka.common.compress.Compression.NONE,
      new org.apache.kafka.common.record.SimpleRecord(Array[Byte](1)))
    when(c.request.body[ReadGlobalSequenceDataRequest]).thenReturn(new ReadGlobalSequenceDataRequest.Builder(data).build())
    when(c.manager.readLocal(batch, 3, 400000000L, READ_UNCOMMITTED)).thenReturn(CompletableFuture.completedFuture(GlobalSequenceFetch.Data(records, 3, 11)))
    c.apis.readData(c.request).join()
    val captured = ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(c.helper).sendResponseExemptThrottle(eqTo(c.request), captured.capture(), eqTo(None))
    assertEquals(records, captured.getValue.asInstanceOf[ReadGlobalSequenceDataResponse].data().records())
    verifyNoInteractions(c.bandwidth, c.requestQuota)
  }

  @Test
  def testVersionOneForwardsReadCommittedAndReturnsThePendingCursor(): Unit = {
    val c = new Context
    c.data.setIsolationLevel(1.toByte)
    when(c.request.header).thenReturn(new RequestHeader(ApiKeys.FETCH_GLOBAL_SEQUENCE, 1.toShort, "client", 1))
    when(c.request.body[FetchGlobalSequenceRequest]).thenReturn(new FetchGlobalSequenceRequest.Builder(c.data).build(1.toShort))
    c.response.data().setTransactionPending(true).setNextGlobalOffset(2)
    c.apis.fetch(c.request).join()
    val captured = ArgumentCaptor.forClass(classOf[GlobalSequenceFetch.Request])
    verify(c.manager).fetch(captured.capture())
    assertEquals(READ_COMMITTED, captured.getValue.isolation)
    assertTrue(c.sent().data().transactionPending())
    assertEquals(2L, c.response.data().nextGlobalOffset())
  }

  @Test
  def testCommittedVersionCannotDowngradeAndVersionZeroStillRejectsIt(): Unit = {
    val c = new Context
    c.data.setIsolationLevel(1.toByte)
    val builder = new FetchGlobalSequenceRequest.Builder(c.data)
    assertEquals(1.toShort, builder.oldestAllowedVersion())
    assertThrows(classOf[org.apache.kafka.common.errors.UnsupportedVersionException], () => builder.build(0.toShort))
    // A malformed v0 request may still arrive on the wire, bypassing the builder.
    when(c.request.body[FetchGlobalSequenceRequest]).thenReturn(new FetchGlobalSequenceRequest(c.data, 0.toShort))
    c.apis.fetch(c.request).join()
    assertEquals(Errors.INVALID_REQUEST.code(), c.sent().data().errorCode())
    verifyNoInteractions(c.manager)
  }

  @Test
  def testInternalReadCommittedPassesIsolationAndReturnsNoAbortedPayload(): Unit = {
    val c = new Context
    val data = new ReadGlobalSequenceDataRequestData().setTopicId(topic).setPartition(1)
      .setPhysicalBaseOffset(10).setPhysicalLastOffset(10).setRecordCount(1).setSourceLeaderEpoch(3)
      .setTimeoutMs(400).setIsolationLevel(1.toByte)
    val batch = GlobalSequenceFetch.physical(data)
    when(c.request.body[ReadGlobalSequenceDataRequest]).thenReturn(new ReadGlobalSequenceDataRequest.Builder(data).build(1.toShort))
    when(c.manager.readLocal(batch, 3, 400000000L, READ_COMMITTED)).thenReturn(CompletableFuture.completedFuture(
      GlobalSequenceFetch.Data(org.apache.kafka.common.record.MemoryRecords.EMPTY, 3, 12, 12, GlobalSequenceFetch.Aborted)))
    c.apis.readData(c.request).join()
    val captured = ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(c.helper).sendResponseExemptThrottle(eqTo(c.request), captured.capture(), eqTo(None))
    val response = captured.getValue.asInstanceOf[ReadGlobalSequenceDataResponse].data()
    assertEquals(GlobalSequenceFetch.Aborted, response.readStatus())
    assertEquals(12L, response.lastStableOffset())
    assertEquals(10L, response.physicalBaseOffset())
    assertEquals(0, response.records().sizeInBytes())
  }
}
