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
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, CoordinatorNotAvailableException, DisconnectException, FencedLeaderEpochException, InvalidRequestException, TimeoutException, UnsupportedVersionException}
import org.apache.kafka.common.internals.Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME
import org.apache.kafka.common.message.{AppendGlobalSequenceIndexResponseData, DescribeGlobalSequencePartitionResponseData, RegisterGlobalSequenceIndexerResponseData}
import org.apache.kafka.common.metadata.{ConfigRecord, PartitionRecord, RegisterBrokerRecord, TopicRecord}
import org.apache.kafka.common.metadata.RegisterBrokerRecord.{BrokerEndpoint, BrokerEndpointCollection}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinator
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard._
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, never, verify, when}

import java.util.{List => JList, Optional, OptionalLong}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookup
import java.util.concurrent.CompletableFuture
import scala.collection.mutable.ArrayBuffer

class IndexRoutingManagerTest {
  private val topicId = new Uuid(1, 2)
  private val indexId = new Uuid(3, 4)
  private val partition = new PartitionKey(topicId, 0)
  private val owner = new IndexerIdentity(1, 3, 0, new Uuid(5, 6))
  private val batch = new PhysicalBatch(partition, 0, 2, 3)
  private val append = new AppendRequest(batch, -1, 3, owner)
  private val registration = new RegistrationRequest(partition, 1, 3, -1, owner.registrationId())
  private val empty = new PartitionDescription(Optional.empty[PhysicalBatch](), Optional.empty[IndexerIdentity]())

  private def image(leader: Int = 2, epoch: Int = 10, index: Uuid = indexId, sourceEpoch: Int = 3,
                    enabled: Boolean = true, endpoint: Boolean = true): MetadataImage = {
    val delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build()
    delta.replay(new TopicRecord().setName("ordered").setTopicId(topicId))
    delta.replay(new PartitionRecord().setTopicId(topicId).setPartitionId(0).setLeader(1).setLeaderEpoch(sourceEpoch)
      .setReplicas(JList.of(1, 2)).setIsr(JList.of(1, 2)))
    delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName("ordered")
      .setName(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG).setValue(enabled.toString))
    if (index != null) {
      delta.replay(new TopicRecord().setName(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME).setTopicId(index))
      for (p <- 0 until 2) delta.replay(new PartitionRecord().setTopicId(index).setPartitionId(p).setLeader(leader).setLeaderEpoch(epoch)
        .setReplicas(JList.of(1, 2)).setIsr(JList.of(1, 2)))
    }
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
    var metadata: MetadataImage = initial
    val coordinator: GlobalSequenceCoordinator = mock(classOf[GlobalSequenceCoordinator])
    when(coordinator.partitionFor(topicId)).thenReturn(1)
    val transport = new Transport
    val router = new IndexRoutingManager(1, 2, new ListenerName("PLAINTEXT"), () => metadata, coordinator, transport, time.scheduler, time)
    router.startup()
    override def close(): Unit = { router.close(); time.scheduler.clear() }
  }

  @Test
  def testGlobalLookupUsesLocalAndRemoteRoutesAndKeepsOneDeadline(): Unit = {
    val lookup = new GlobalSequenceLookup.Request(topicId, 0, 3, 1, 1000)
    def result(epoch: Int) = new GlobalSequenceLookup.Result(new GlobalSequenceLookup.Snapshot(indexId, 1, epoch, 4, 3),
      JList.of(new GlobalSequenceLookup.Mapping(0, batch)), 3)
    val local = new Context(image(leader = 1))
    try {
      when(local.coordinator.lookupIndex(lookup, 10)).thenReturn(CompletableFuture.completedFuture(result(10)))
      assertEquals(result(10), local.router.lookupIndex(lookup).join().value)
      assertTrue(local.transport.sent.isEmpty)
    } finally local.close()
    val remote = new Context
    try {
      val future = remote.router.lookupIndex(lookup)
      assertTrue(remote.transport.sent.head.request.isInstanceOf[ReadGlobalSequenceIndexRequest])
      remote.metadata = image(epoch = 11)
      remote.transport.sent.head.future.complete(GlobalSequenceProtocol.internalLookupResponse(lookup, result(10)))
      assertFalse(future.isDone)
      remote.time.sleep(100)
      val retry = remote.transport.sent.last.request.asInstanceOf[ReadGlobalSequenceIndexRequest].data()
      assertEquals(11, retry.coordinatorLeaderEpoch())
      assertEquals(900, retry.timeoutMs())
      remote.transport.sent.last.future.complete(GlobalSequenceProtocol.internalLookupResponse(lookup, result(11)))
      assertEquals(result(11), future.join().value)
    } finally remote.close()
  }

  private def describeReply(base: Long = -1, last: Long = -1, count: Int = 0): DescribeGlobalSequencePartitionResponse =
    new DescribeGlobalSequencePartitionResponse(new DescribeGlobalSequencePartitionResponseData().setTopicId(topicId).setPartition(0)
      .setPhysicalBaseOffset(base).setPhysicalLastOffset(last).setRecordCount(count))

  private def registerReply(registered: Boolean = true, epoch: Int = 10): RegisterGlobalSequenceIndexerResponse =
    new RegisterGlobalSequenceIndexerResponse(new RegisterGlobalSequenceIndexerResponseData().setTopicId(topicId).setPartition(0)
      .setRegistered(registered).setSourceBrokerId(1).setSourceLeaderEpoch(3).setIndexerGeneration(0)
      .setRegistrationId(owner.registrationId()).setCoordinatorLeaderEpoch(epoch))

  private def appendReply(status: AppendStatus = AppendStatus.INDEXED, epoch: Int = 10): AppendGlobalSequenceIndexResponse =
    new AppendGlobalSequenceIndexResponse(new AppendGlobalSequenceIndexResponseData().setTopicId(topicId).setPartition(0)
      .setPhysicalBaseOffset(0).setPhysicalLastOffset(2).setRecordCount(3).setStatus(status.code())
      .setGlobalBaseOffset(if (status == AppendStatus.INDEXED) 0 else -1).setIndexedThroughBaseOffset(0).setIndexedThroughLastOffset(2)
      .setIndexedThroughRecordCount(3).setCoordinatorLeaderEpoch(epoch))

  @Test
  def testLocalCallsUseCoordinatorAndCapturedEpoch(): Unit = {
    val ctx = new Context(image(leader = 1))
    try {
      when(ctx.coordinator.describePartition(partition)).thenReturn(CompletableFuture.completedFuture(empty))
      val registrationFuture = new CompletableFuture[RegistrationResponse]()
      when(ctx.coordinator.registerIndexer(registration, 10)).thenReturn(registrationFuture)
      val response = new AppendResponse(AppendStatus.INDEXED, batch, OptionalLong.of(0), Optional.of(batch), 10)
      when(ctx.coordinator.appendIndex(append, 10)).thenReturn(CompletableFuture.completedFuture(response))
      val read = ctx.router.describePartition(partition, 1000).join()
      assertEquals(empty, read.value)
      assertTrue(ctx.router.isCurrent(partition, read.coordinator))
      val registered = ctx.router.registerIndexer(registration, 1000)
      assertFalse(registered.isDone)
      registrationFuture.complete(new RegistrationResponse(true, Optional.of(owner), 10))
      assertTrue(registered.join().value.registered())
      assertEquals(response, ctx.router.appendIndex(append, 1000).join().value)
      assertTrue(ctx.transport.sent.isEmpty)
      ctx.metadata = image(leader = 1, epoch = 11)
      assertFalse(ctx.router.isCurrent(partition, read.coordinator))
    } finally ctx.close()
  }

  @Test
  def testRemoteResponseAfterLeaderChangeIsDiscardedAndReadLocally(): Unit = {
    val ctx = new Context
    try {
      val result = ctx.router.describePartition(partition, 1000)
      assertEquals(2, ctx.transport.sent.head.node.id())
      ctx.metadata = image(leader = 1, epoch = 11)
      when(ctx.coordinator.describePartition(partition)).thenReturn(CompletableFuture.completedFuture(empty))
      ctx.transport.sent.head.future.complete(describeReply(0, 2, 3))
      assertFalse(result.isDone)
      ctx.time.sleep(100)
      assertEquals(empty, result.join().value)
      assertEquals(11, result.join().coordinator.leaderEpoch)
    } finally ctx.close()
  }

  @Test
  def testOldLocalReadIsRetriedOnRemoteLeader(): Unit = {
    val ctx = new Context(image(leader = 1))
    try {
      val local = new CompletableFuture[PartitionDescription]()
      when(ctx.coordinator.describePartition(partition)).thenReturn(local)
      val result = ctx.router.describePartition(partition, 1000)
      ctx.metadata = image(leader = 2, epoch = 11)
      local.complete(empty)
      ctx.time.sleep(100)
      ctx.transport.sent.head.future.complete(describeReply(0, 2, 3))
      assertEquals(Optional.of(batch), result.join().value.committedProgress())
      assertEquals(2, result.join().coordinator.node.id())
    } finally ctx.close()
  }

  @Test
  def testRegistrationRetryPreservesCasAndUuidAcrossCoordinatorEpochChange(): Unit = {
    val ctx = new Context
    try {
      val result = ctx.router.registerIndexer(registration, 2000)
      val first = ctx.transport.sent.head.request.asInstanceOf[RegisterGlobalSequenceIndexerRequest].data()
      ctx.transport.sent.head.future.completeExceptionally(DisconnectException.INSTANCE)
      ctx.metadata = image(epoch = 11)
      ctx.time.sleep(100)
      val retry = ctx.transport.sent(1).request.asInstanceOf[RegisterGlobalSequenceIndexerRequest].data()
      assertEquals(first.duplicate().setCoordinatorLeaderEpoch(11), retry)
      ctx.transport.sent(1).future.complete(registerReply(epoch = 11))
      assertTrue(result.join().value.registered())
      assertEquals(owner, result.join().value.indexer().get())
    } finally ctx.close()
  }

  @Test
  def testAppendTimeoutRetryPreservesEntireBatchAndOwnership(): Unit = {
    val ctx = new Context
    try {
      val result = ctx.router.appendIndex(append, 2000)
      val first = ctx.transport.sent.head.request.asInstanceOf[AppendGlobalSequenceIndexRequest].data()
      ctx.transport.sent.head.future.complete(new AppendGlobalSequenceIndexResponse(new AppendGlobalSequenceIndexResponseData()
        .setErrorCode(Errors.REQUEST_TIMED_OUT.code())))
      ctx.time.sleep(100)
      assertEquals(first, ctx.transport.sent(1).request.asInstanceOf[AppendGlobalSequenceIndexRequest].data())
      ctx.transport.sent(1).future.complete(appendReply(AppendStatus.ALREADY_INDEXED))
      assertEquals(AppendStatus.ALREADY_INDEXED, result.join().value.status())
      assertTrue(result.join().value.globalBaseOffset().isEmpty)
    } finally ctx.close()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("FENCED", "OWNER_NOT_COMMITTED", "OUT_OF_ORDER"))
  def testDomainResultsDoNotTriggerAutomaticOwnershipOrOrderingChanges(statusName: String): Unit = {
    val ctx = new Context
    try {
      val status = AppendStatus.valueOf(statusName)
      val result = ctx.router.appendIndex(append, 1000)
      ctx.metadata = image(epoch = 11)
      ctx.transport.sent.head.future.complete(appendReply(status))
      assertEquals(status, result.join().value.status())
      ctx.time.sleep(1000)
      assertEquals(1, ctx.transport.sent.size)
    } finally ctx.close()
  }

  @Test
  def testFailedRegistrationCasIsTerminal(): Unit = {
    val ctx = new Context
    try {
      val result = ctx.router.registerIndexer(registration, 1000)
      ctx.metadata = image(epoch = 11)
      ctx.transport.sent.head.future.complete(registerReply(registered = false))
      assertFalse(result.join().value.registered())
      ctx.time.sleep(1000)
      assertEquals(1, ctx.transport.sent.size)
    } finally ctx.close()
  }

  @Test
  def testAuthenticationVersionAndSourceFencingFailuresAreTerminal(): Unit = {
    for (error <- Seq(new ClusterAuthorizationException("denied"), new UnsupportedVersionException("old broker"), new FencedLeaderEpochException("old source"))) {
      val ctx = new Context
      try {
        val result = ctx.router.registerIndexer(registration, 1000)
        ctx.transport.sent.head.future.completeExceptionally(error)
        assertFutureThrows(error.getClass, result)
        ctx.time.sleep(1000)
        assertEquals(1, ctx.transport.sent.size)
      } finally ctx.close()
    }
  }

  @Test
  def testDeadlineDoesNotResetAfterFailuresAndIgnoresLateCompletion(): Unit = {
    val ctx = new Context
    try {
      val result = ctx.router.appendIndex(append, 500)
      ctx.transport.sent.head.future.completeExceptionally(DisconnectException.INSTANCE)
      ctx.time.sleep(100)
      val retry = ctx.transport.sent(1)
      ctx.time.sleep(400)
      assertFutureThrows(classOf[TimeoutException], result)
      assertTrue(retry.future.isCancelled)
      assertFalse(retry.future.complete(appendReply()))
      ctx.time.sleep(1000)
      assertEquals(2, ctx.transport.sent.size)
    } finally ctx.close()
  }

  @Test
  def testMetadataNotAvailableThenArrivesAndMissingEndpointRetries(): Unit = {
    val ctx = new Context(image(index = null))
    try {
      val result = ctx.router.describePartition(partition, 2000)
      assertTrue(ctx.transport.sent.isEmpty)
      ctx.metadata = image(endpoint = false)
      ctx.time.sleep(100)
      assertTrue(ctx.transport.sent.isEmpty)
      ctx.metadata = image()
      ctx.time.sleep(200)
      ctx.transport.sent.head.future.complete(describeReply())
      assertTrue(result.isDone)
      assertEquals(empty, result.join().value)
    } finally ctx.close()
  }

  @Test
  def testSourceEpochIsRecheckedBeforeRetryAndAfterSuccess(): Unit = {
    for (succeed <- Seq(true, false)) {
      val ctx = new Context
      try {
        val result = ctx.router.appendIndex(append, 1000)
        ctx.metadata = image(sourceEpoch = 4)
        if (succeed) ctx.transport.sent.head.future.complete(appendReply())
        else { ctx.transport.sent.head.future.completeExceptionally(DisconnectException.INSTANCE); ctx.time.sleep(100) }
        assertFutureThrows(classOf[FencedLeaderEpochException], result)
        assertEquals(1, ctx.transport.sent.size)
      } finally ctx.close()
    }
  }

  @Test
  def testIndexLossOrReplacementStopsRoutingPermanently(): Unit = {
    for (replacement <- Seq(null, new Uuid(8, 9))) {
      val ctx = new Context
      try {
        val result = ctx.router.describePartition(partition, 1000)
        ctx.metadata = image(index = replacement)
        ctx.transport.sent.head.future.complete(describeReply())
        assertFutureThrows(classOf[InvalidRequestException], result)
        ctx.metadata = image()
        assertFutureThrows(classOf[InvalidRequestException], ctx.router.describePartition(partition, 1000))
        assertEquals(1, ctx.transport.sent.size)
      } finally ctx.close()
    }
  }

  @Test
  def testMalformedResponsesCannotAcknowledgeDifferentWork(): Unit = {
    for (response <- Seq(
      appendReply().data().setTopicId(new Uuid(9, 9)),
      appendReply().data().setPhysicalBaseOffset(3).setPhysicalLastOffset(5),
      appendReply().data().setIndexedThroughBaseOffset(-1).setIndexedThroughLastOffset(-1).setIndexedThroughRecordCount(0),
      appendReply().data().setGlobalBaseOffset(-1))) {
      val ctx = new Context
      try {
        val result = ctx.router.appendIndex(append, 1000)
        ctx.transport.sent.head.future.complete(new AppendGlobalSequenceIndexResponse(response))
        assertFutureThrows(classOf[InvalidRequestException], result)
      } finally ctx.close()
    }
  }

  @Test
  def testCloseCompletesLocalRemoteAndWaitingRequestsWithoutCancelingLocalWrites(): Unit = {
    val ctx = new Context(image(leader = 1))
    try {
      val local = new CompletableFuture[RegistrationResponse]()
      when(ctx.coordinator.registerIndexer(registration, 10)).thenReturn(local)
      val registrationResult = ctx.router.registerIndexer(registration, 1000)
      ctx.metadata = image()
      val remote = ctx.router.appendIndex(append, 1000)
      ctx.router.close()
      assertFutureThrows(classOf[CoordinatorNotAvailableException], registrationResult)
      assertFutureThrows(classOf[CoordinatorNotAvailableException], remote)
      assertFalse(local.isCancelled)
      assertTrue(ctx.transport.sent.head.future.isCancelled)
      assertTrue(ctx.transport.closed)
      assertFutureThrows(classOf[CoordinatorNotAvailableException], ctx.router.describePartition(partition, 1000))
      local.complete(new RegistrationResponse(true, Optional.of(owner), 10))
      ctx.time.sleep(1000)
      assertEquals(1, ctx.transport.sent.size)
    } finally ctx.close()
  }

  @Test
  def testDisabledTopicAndInvalidDeadlineNeverDispatch(): Unit = {
    val ctx = new Context(image(enabled = false))
    try {
      assertFutureThrows(classOf[IllegalArgumentException], ctx.router.describePartition(partition, 0))
      assertFutureThrows(classOf[InvalidRequestException], ctx.router.describePartition(partition, 1000))
      assertTrue(ctx.transport.sent.isEmpty)
      verify(ctx.coordinator, never()).describePartition(any())
    } finally ctx.close()
  }

  @Test
  def testObsoleteResponseEpochCannotCompleteAppend(): Unit = {
    val ctx = new Context
    try {
      val result = ctx.router.appendIndex(append, 1000)
      ctx.transport.sent.head.future.complete(appendReply(epoch = 9))
      assertFalse(result.isDone)
      ctx.time.sleep(100)
      ctx.transport.sent(1).future.complete(appendReply())
      assertEquals(10, result.join().value.coordinatorLeaderEpoch())
    } finally ctx.close()
  }

  @Test
  def testCancellationStopsRetries(): Unit = {
    val ctx = new Context
    try {
      val result = ctx.router.appendIndex(append, 1000)
      ctx.transport.sent.head.future.completeExceptionally(DisconnectException.INSTANCE)
      result.cancel(false)
      ctx.time.sleep(1000)
      assertEquals(1, ctx.transport.sent.size)
      assertTrue(result.isCancelled)
    } finally ctx.close()
  }

  @Test
  def testRegistrationResponseCannotSubstituteAnotherOwner(): Unit = {
    val ctx = new Context
    try {
      val result = ctx.router.registerIndexer(registration, 1000)
      ctx.transport.sent.head.future.complete(new RegisterGlobalSequenceIndexerResponse(registerReply().data().setIndexerGeneration(1)))
      assertFutureThrows(classOf[InvalidRequestException], result)
    } finally ctx.close()
  }

}
