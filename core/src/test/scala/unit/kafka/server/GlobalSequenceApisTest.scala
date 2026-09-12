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
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.acl.AclOperation.CLUSTER_ACTION
import org.apache.kafka.common.errors.{ClusterAuthorizationException, TimeoutException}
import org.apache.kafka.common.message.{AppendGlobalSequenceIndexRequestData, DescribeGlobalSequencePartitionRequestData, RegisterGlobalSequenceIndexerRequestData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinator
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{AppendRequest, AppendResponse, AppendStatus, IndexerIdentity, PartitionDescription, PartitionKey, PhysicalBatch, RegistrationRequest, RegistrationResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyInt, eq => eqTo}
import org.mockito.Mockito.{doThrow, mock, verify, verifyNoInteractions, when}

import java.util.{Optional, OptionalLong}
import java.util.concurrent.CompletableFuture

class GlobalSequenceApisTest {
  private val topicId = new Uuid(1, 2)
  private val partition = new PartitionKey(topicId, 0)
  private val owner = new IndexerIdentity(1, 3, 0, new Uuid(4, 5))
  private val batch = new PhysicalBatch(partition, 0, 2, 3)

  private class Context {
    val coordinator: GlobalSequenceCoordinator = mock(classOf[GlobalSequenceCoordinator])
    val auth: AuthHelper = mock(classOf[AuthHelper])
    val helper: RequestHandlerHelper = mock(classOf[RequestHandlerHelper])
    val request: RequestChannel.Request = mock(classOf[RequestChannel.Request])
    val apis = new GlobalSequenceApis(coordinator, auth, helper)

    def response(unauthorized: Boolean = false): AbstractResponse = {
      val captor = ArgumentCaptor.forClass(classOf[AbstractResponse])
      if (unauthorized) verify(helper).sendMaybeThrottle(eqTo(request), captor.capture())
      else verify(helper).sendResponseExemptThrottle(eqTo(request), captor.capture(), eqTo(None))
      captor.getValue
    }
  }

  private def registerData(): RegisterGlobalSequenceIndexerRequestData =
    new RegisterGlobalSequenceIndexerRequestData().setTopicId(topicId).setPartition(0).setSourceBrokerId(1)
      .setSourceLeaderEpoch(3).setExpectedGeneration(-1).setRegistrationId(owner.registrationId()).setCoordinatorLeaderEpoch(10)

  private def appendData(): AppendGlobalSequenceIndexRequestData =
    new AppendGlobalSequenceIndexRequestData().setTopicId(topicId).setPartition(0).setPhysicalBaseOffset(0).setPhysicalLastOffset(2)
      .setRecordCount(3).setPredecessorBaseOffset(-1).setDataHighWatermark(3).setSourceBrokerId(1).setSourceLeaderEpoch(3)
      .setIndexerGeneration(0).setRegistrationId(owner.registrationId()).setCoordinatorLeaderEpoch(10)

  @ParameterizedTest
  @ValueSource(strings = Array("register", "describe", "append"))
  def testClusterAuthorizationPrecedesOperation(kind: String): Unit = {
    val ctx = new Context
    doThrow(new ClusterAuthorizationException("denied")).when(ctx.auth).authorizeClusterOperation(ctx.request, CLUSTER_ACTION)
    kind match {
      case "register" =>
        when(ctx.request.body[RegisterGlobalSequenceIndexerRequest]).thenReturn(new RegisterGlobalSequenceIndexerRequest.Builder(registerData()).build())
        ctx.apis.registerIndexer(ctx.request).join()
      case "describe" =>
        when(ctx.request.body[DescribeGlobalSequencePartitionRequest]).thenReturn(new DescribeGlobalSequencePartitionRequest.Builder(
          new DescribeGlobalSequencePartitionRequestData().setTopicId(topicId).setPartition(0)).build())
        ctx.apis.describePartition(ctx.request).join()
      case "append" =>
        when(ctx.request.body[AppendGlobalSequenceIndexRequest]).thenReturn(new AppendGlobalSequenceIndexRequest.Builder(appendData()).build())
        ctx.apis.appendIndex(ctx.request).join()
    }
    verifyNoInteractions(ctx.coordinator)
    assertEquals(java.util.Map.of(Errors.CLUSTER_AUTHORIZATION_FAILED, Int.box(1)), ctx.response(unauthorized = true).errorCounts())
  }

  @Test
  def testRegistrationResponseWaitsForCoordinatorFuture(): Unit = {
    val ctx = new Context
    when(ctx.request.body[RegisterGlobalSequenceIndexerRequest]).thenReturn(new RegisterGlobalSequenceIndexerRequest.Builder(registerData()).build())
    val future = new CompletableFuture[RegistrationResponse]()
    when(ctx.coordinator.registerIndexer(any[RegistrationRequest], anyInt())).thenReturn(future)
    val completed = ctx.apis.registerIndexer(ctx.request)
    assertFalse(completed.isDone)
    verifyNoInteractions(ctx.helper)
    verify(ctx.coordinator).registerIndexer(new RegistrationRequest(partition, 1, 3, -1, owner.registrationId()), 10)
    future.complete(new RegistrationResponse(true, Optional.of(owner), 10))
    completed.join()
    val response = ctx.response().asInstanceOf[RegisterGlobalSequenceIndexerResponse].data()
    assertTrue(response.registered())
    assertEquals(Errors.NONE.code, response.errorCode())
    assertEquals(0, response.indexerGeneration())
    assertEquals(owner.registrationId(), response.registrationId())
    assertEquals(10, response.coordinatorLeaderEpoch())
  }

  @Test
  def testDescribeSeparatesCommittedProgressAndPendingOwnership(): Unit = {
    val ctx = new Context
    when(ctx.request.body[DescribeGlobalSequencePartitionRequest]).thenReturn(new DescribeGlobalSequencePartitionRequest.Builder(
      new DescribeGlobalSequencePartitionRequestData().setTopicId(topicId).setPartition(0)).build())
    when(ctx.coordinator.describePartition(partition)).thenReturn(CompletableFuture.completedFuture(
      new PartitionDescription(Optional.empty[PhysicalBatch](), Optional.of(owner))))
    ctx.apis.describePartition(ctx.request).join()
    val response = ctx.response().asInstanceOf[DescribeGlobalSequencePartitionResponse].data()
    assertEquals(-1L, response.physicalBaseOffset())
    assertEquals(-1L, response.physicalLastOffset())
    assertEquals(0, response.recordCount())
    assertEquals(0L, response.indexerGeneration())
    assertEquals(owner.registrationId(), response.registrationId())
  }

  @ParameterizedTest
  @ValueSource(strings = Array("INDEXED", "ALREADY_INDEXED", "FENCED", "OWNER_NOT_COMMITTED", "OUT_OF_ORDER"))
  def testAppendDomainResultIsPreserved(statusName: String): Unit = {
    val ctx = new Context
    val status = AppendStatus.valueOf(statusName)
    when(ctx.request.body[AppendGlobalSequenceIndexRequest]).thenReturn(new AppendGlobalSequenceIndexRequest.Builder(appendData()).build())
    val future = new CompletableFuture[AppendResponse]()
    when(ctx.coordinator.appendIndex(any[AppendRequest], anyInt())).thenReturn(future)
    val completed = ctx.apis.appendIndex(ctx.request)
    assertFalse(completed.isDone)
    verifyNoInteractions(ctx.helper)
    val global = if (status == AppendStatus.INDEXED) OptionalLong.of(7) else OptionalLong.empty()
    future.complete(new AppendResponse(status, batch, global, Optional.of(batch), 10))
    completed.join()
    val response = ctx.response().asInstanceOf[AppendGlobalSequenceIndexResponse].data()
    assertEquals(status.code(), response.status())
    assertEquals(global.orElse(-1), response.globalBaseOffset())
    assertEquals(0L, response.physicalBaseOffset())
    assertEquals(2L, response.indexedThroughLastOffset())
    assertEquals(10, response.coordinatorLeaderEpoch())
  }

  @Test
  def testMalformedBatchDoesNotReachCoordinator(): Unit = {
    val ctx = new Context
    when(ctx.request.body[AppendGlobalSequenceIndexRequest]).thenReturn(new AppendGlobalSequenceIndexRequest.Builder(appendData().setRecordCount(0)).build())
    ctx.apis.appendIndex(ctx.request).join()
    assertEquals(Errors.INVALID_REQUEST.code(), ctx.response().asInstanceOf[AppendGlobalSequenceIndexResponse].data().errorCode())
    verifyNoInteractions(ctx.coordinator)
  }

  @Test
  def testRegistrationTimeoutIsReturnedWithoutCancelingFuture(): Unit = {
    val ctx = new Context
    when(ctx.request.body[RegisterGlobalSequenceIndexerRequest]).thenReturn(new RegisterGlobalSequenceIndexerRequest.Builder(registerData()).build())
    when(ctx.coordinator.registerIndexer(any[RegistrationRequest], anyInt())).thenReturn(CompletableFuture.failedFuture(new TimeoutException("late commit")))
    ctx.apis.registerIndexer(ctx.request).join()
    assertEquals(Errors.REQUEST_TIMED_OUT.code(), ctx.response().asInstanceOf[RegisterGlobalSequenceIndexerResponse].data().errorCode())
  }
}
