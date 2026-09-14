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
import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.common.acl.AclOperation.{CLUSTER_ACTION, READ}
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.message.{LookupGlobalSequenceRequestData, ReadGlobalSequenceIndexRequestData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.ResourceType.TOPIC
import org.apache.kafka.coordinator.globalsequence.{GlobalSequenceCoordinator, GlobalSequenceLookup}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{PartitionKey, PhysicalBatch}
import org.apache.kafka.metadata.MetadataCache
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, eq => eqTo}
import org.mockito.Mockito.{doThrow, mock, verify, verifyNoInteractions, when}

import java.util.{List => JList, Optional}
import java.util.concurrent.CompletableFuture

class GlobalSequenceLookupApisTest {
  private val topic = new Uuid(1, 2)
  private val index = new Uuid(3, 4)
  private val route = IndexRoutingManager.CoordinatorLocation(index, 1, new Node(1, "localhost", 9001), 10, 7)
  private val lookup = new GlobalSequenceLookup.Request(topic, 1, 2, 10, 1000)
  private val result = new GlobalSequenceLookup.Result(new GlobalSequenceLookup.Snapshot(index, 1, 10, 4, 3),
    JList.of(new GlobalSequenceLookup.Mapping(0, new PhysicalBatch(new PartitionKey(topic, 0), 10, 12, 3))), 2)

  private class Context {
    val coordinator: GlobalSequenceCoordinator = mock(classOf[GlobalSequenceCoordinator])
    val router: IndexRoutingManager = mock(classOf[IndexRoutingManager])
    val metadata: MetadataCache = mock(classOf[MetadataCache])
    val auth: AuthHelper = mock(classOf[AuthHelper])
    val helper: RequestHandlerHelper = mock(classOf[RequestHandlerHelper])
    val request: RequestChannel.Request = mock(classOf[RequestChannel.Request])
    val data = new LookupGlobalSequenceRequestData().setTopicId(topic).setGlobalStartOffset(1)
      .setGlobalEndOffsetExclusive(2).setMaxBatches(10).setTimeoutMs(1000)
    when(metadata.getTopicName(topic)).thenReturn(Optional.of("ordered"))
    when(auth.authorize(any(), eqTo(READ), eqTo(TOPIC), eqTo("ordered"), anyBoolean(), anyBoolean(), anyInt())).thenReturn(true)
    when(request.body[LookupGlobalSequenceRequest]).thenReturn(new LookupGlobalSequenceRequest.Builder(data).build())
    when(router.isCurrent(any(), any())).thenReturn(true)
    val apis = new GlobalSequenceLookupApis(coordinator, router, metadata, auth, helper)
    def response(): AbstractResponse = {
      val captured = ArgumentCaptor.forClass(classOf[AbstractResponse])
      verify(helper).sendMaybeThrottle(eqTo(request), captured.capture())
      captured.getValue
    }
  }

  @Test
  def testTopicReadAuthorizationPrecedesRouting(): Unit = {
    val c = new Context
    when(c.auth.authorize(any(), eqTo(READ), eqTo(TOPIC), eqTo("ordered"), anyBoolean(), anyBoolean(), anyInt())).thenReturn(false)
    c.apis.lookup(c.request).join()
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, c.response().asInstanceOf[LookupGlobalSequenceResponse].data().errorCode() match {
      case code => Errors.forCode(code)
    })
    verifyNoInteractions(c.router, c.coordinator)
  }

  @Test
  def testPublicLookupWaitsAndPreservesPartialBatchMapping(): Unit = {
    val c = new Context
    val future = new CompletableFuture[IndexRoutingManager.RoutedResult[GlobalSequenceLookup.Result]]()
    when(c.router.lookupIndex(lookup)).thenReturn(future)
    val done = c.apis.lookup(c.request)
    assertFalse(done.isDone)
    verifyNoInteractions(c.helper)
    future.complete(IndexRoutingManager.RoutedResult(result, route))
    assertTrue(done.isDone)
    val response = c.response().asInstanceOf[LookupGlobalSequenceResponse].data()
    assertEquals(Errors.NONE.code(), response.errorCode())
    assertEquals(3L, response.committedGlobalEndOffset())
    assertEquals(2L, response.nextGlobalOffset())
    val mapping = response.batches().get(0)
    assertEquals(10L, mapping.physicalBaseOffset())
    assertEquals(12L, mapping.physicalLastOffset())
    assertEquals(1L, mapping.selectedGlobalStartOffset())
    assertEquals(2L, mapping.selectedGlobalEndOffset())
  }

  @Test
  def testInvalidRangeAndPageLimitsNeverRoute(): Unit = {
    for (field <- Seq("range", "page", "timeout")) {
      val c = new Context
      field match {
        case "range" => c.data.setGlobalStartOffset(-1)
        case "page" => c.data.setMaxBatches(1001)
        case "timeout" => c.data.setTimeoutMs(0)
      }
      c.apis.lookup(c.request).join()
      assertEquals(Errors.INVALID_REQUEST.code(), c.response().asInstanceOf[LookupGlobalSequenceResponse].data().errorCode())
      verifyNoInteractions(c.router)
    }
  }

  @Test
  def testUnknownUuidAndStaleRouteDoNotReturnMappings(): Unit = {
    val unknown = new Context
    when(unknown.metadata.getTopicName(topic)).thenReturn(Optional.empty())
    unknown.apis.lookup(unknown.request).join()
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code(), unknown.response().asInstanceOf[LookupGlobalSequenceResponse].data().errorCode())
    verifyNoInteractions(unknown.router)
    val stale = new Context
    when(stale.router.lookupIndex(lookup)).thenReturn(CompletableFuture.completedFuture(IndexRoutingManager.RoutedResult(result, route)))
    when(stale.router.isCurrent(any(), any())).thenReturn(false)
    stale.apis.lookup(stale.request).join()
    val response = stale.response().asInstanceOf[LookupGlobalSequenceResponse].data()
    assertEquals(Errors.NOT_COORDINATOR.code(), response.errorCode())
    assertTrue(response.batches().isEmpty)
  }

  @Test
  def testInternalLookupRequiresClusterActionBeforeScheduling(): Unit = {
    val c = new Context
    when(c.request.body[ReadGlobalSequenceIndexRequest]).thenReturn(new ReadGlobalSequenceIndexRequest.Builder(
      new ReadGlobalSequenceIndexRequestData().setTopicId(topic)).build())
    doThrow(new ClusterAuthorizationException("denied")).when(c.auth).authorizeClusterOperation(c.request, CLUSTER_ACTION)
    c.apis.readIndex(c.request).join()
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), c.response().asInstanceOf[ReadGlobalSequenceIndexResponse].data().errorCode())
    verifyNoInteractions(c.coordinator, c.router)
  }

  @Test
  def testInternalProtocolRejectsGapsAndInvalidContinuation(): Unit = {
    val reply = GlobalSequenceProtocol.internalLookupResponse(lookup, result)
    assertEquals(result, GlobalSequenceProtocol.lookupResponse(lookup, reply))
    reply.data().setNextGlobalOffset(1)
    assertThrows(classOf[org.apache.kafka.common.errors.InvalidRequestException], () => GlobalSequenceProtocol.lookupResponse(lookup, reply))
    reply.data().setNextGlobalOffset(2)
    reply.data().batches().get(0).setSelectedGlobalStartOffset(0)
    assertThrows(classOf[org.apache.kafka.common.errors.InvalidRequestException], () => GlobalSequenceProtocol.lookupResponse(lookup, reply))
  }
}
