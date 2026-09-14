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

import kafka.cluster.Partition
import org.apache.kafka.common.{Node, TopicPartition, Uuid}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, TimeoutException}
import org.apache.kafka.common.metadata.{ConfigRecord, PartitionRecord, TopicRecord}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard._
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.log.UnifiedLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, never, verify, when}

import java.util.Optional
import java.util.concurrent.CompletableFuture
import scala.collection.mutable

class GlobalSequenceRetentionManagerTest {
  import IndexRoutingManager._
  private val id = new Uuid(1, 2)
  private val key = new PartitionKey(id, 0)
  private val tp = new TopicPartition("ordered", 0)
  private val route = CoordinatorLocation(new Uuid(3, 4), 0, new Node(2, "localhost", 9002), 10, 1)

  private def image(topicId: Uuid = id, enabled: Boolean = true, leader: Int = 2): MetadataImage = {
    val delta = new MetadataDelta(MetadataImage.EMPTY)
    delta.replay(new TopicRecord().setTopicId(topicId).setName(tp.topic()))
    delta.replay(new PartitionRecord().setTopicId(topicId).setPartitionId(0).setLeader(leader).setLeaderEpoch(3)
      .setReplicas(java.util.List.of(1, 2)).setIsr(java.util.List.of(1, 2)))
    delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName(tp.topic())
      .setName(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG).setValue(enabled.toString))
    delta.apply(MetadataProvenance.EMPTY)
  }

  private class Context extends AutoCloseable {
    val workers = new GlobalSequenceTestExecutor
    val time = new MockTime(0, 0)
    val replicas: ReplicaManager = mock(classOf[ReplicaManager])
    val router: IndexRoutingManager = mock(classOf[IndexRoutingManager])
    val source: Partition = mock(classOf[Partition])
    val log: UnifiedLog = mock(classOf[UnifiedLog])
    val calls = mutable.ArrayBuffer.empty[CompletableFuture[RoutedResult[PartitionDescription]]]
    var currentRoute = route
    when(replicas.onlinePartition(tp)).thenReturn(Some(source))
    when(source.topicId).thenReturn(Some(id))
    when(source.log).thenReturn(Some(log))
    when(source.futureLog).thenReturn(None)
    when(router.isCurrent(any[PartitionKey], any[CoordinatorLocation])).thenAnswer(invocation =>
      invocation.getArgument[CoordinatorLocation](1) == currentRoute)
    when(router.describePartition(any[PartitionKey], anyLong())).thenAnswer { _ =>
      val future = new CompletableFuture[RoutedResult[PartitionDescription]]()
      calls += future
      future
    }
    val retention = new GlobalSequenceRetentionManager(replicas, router, workers, time.scheduler, 1000, 100)
    def start(): Unit = {
      retention.onMetadataUpdate(image())
      assertTrue(calls.isEmpty, "Metadata publication must only schedule the query")
      workers.runAll()
    }
    def complete(last: Long = 4): Unit = {
      calls.last.complete(RoutedResult(new PartitionDescription(Optional.of(new PhysicalBatch(key, 0, last, (last + 1).toInt)),
        Optional.empty[IndexerIdentity]()), currentRoute))
      workers.runAll()
    }
    def refresh(): Unit = { time.sleep(100); workers.runAll() }
    override def close(): Unit = { retention.close(); workers.shutdown(); time.scheduler.clear() }
  }

  @Test
  def testFollowersAndFutureLogsUseOnlyCommittedProgressWithoutRegisteringOwnership(): Unit = {
    val c = new Context
    try {
      val futureLog = mock(classOf[UnifiedLog])
      when(c.source.futureLog).thenReturn(Some(futureLog))
      c.start()
      verify(c.log, never()).updateGlobalSequenceIndexedOffset(any[Uuid], anyLong())
      c.complete()
      verify(c.log).updateGlobalSequenceIndexedOffset(id, 5)
      verify(futureLog).updateGlobalSequenceIndexedOffset(id, 5)
      verify(c.router, never()).registerIndexer(any[RegistrationRequest], anyLong())
      c.retention.onMetadataUpdate(image(leader = 1))
      c.refresh()
      assertEquals(2, c.calls.size)
    } finally c.close()
  }

  @Test
  def testOneOutstandingRefreshPerReplicaAndRetryDoesNotReleaseThePin(): Unit = {
    val c = new Context
    try {
      c.start()
      c.refresh()
      c.refresh()
      assertEquals(1, c.calls.size)
      c.calls.head.completeExceptionally(new TimeoutException("index unavailable"))
      c.workers.runAll()
      verify(c.log, never()).updateGlobalSequenceIndexedOffset(any[Uuid], anyLong())
      c.refresh()
      assertEquals(2, c.calls.size)
      c.complete()
      verify(c.log).updateGlobalSequenceIndexedOffset(id, 5)
    } finally c.close()
  }

  @Test
  def testQueuedResponseFromOldIndexLeaderCannotAdvanceThePin(): Unit = {
    val c = new Context
    try {
      c.start()
      c.calls.head.complete(RoutedResult(new PartitionDescription(Optional.of(new PhysicalBatch(key, 0, 4, 5)), Optional.empty()), route))
      c.currentRoute = route.copy(leaderEpoch = 11)
      c.workers.runAll()
      verify(c.log, never()).updateGlobalSequenceIndexedOffset(any[Uuid], anyLong())
      c.refresh()
      c.complete()
      verify(c.log).updateGlobalSequenceIndexedOffset(id, 5)
    } finally c.close()
  }

  @Test
  def testReplacementLogWaitsForItsOwnQuery(): Unit = {
    val c = new Context
    try {
      c.start()
      val replacement = mock(classOf[UnifiedLog])
      when(c.source.log).thenReturn(Some(replacement))
      c.complete()
      verify(c.log, never()).updateGlobalSequenceIndexedOffset(any[Uuid], anyLong())
      verify(replacement, never()).updateGlobalSequenceIndexedOffset(any[Uuid], anyLong())
      c.refresh()
      c.complete()
      verify(replacement).updateGlobalSequenceIndexedOffset(id, 5)
    } finally c.close()
  }

  @Test
  def testNewFutureLogIsDiscoveredByPeriodicRefreshWithoutMetadataChange(): Unit = {
    val c = new Context
    try {
      c.start()
      val future = mock(classOf[UnifiedLog])
      when(c.source.futureLog).thenReturn(Some(future))
      c.complete()
      verify(future, never()).updateGlobalSequenceIndexedOffset(any[Uuid], anyLong())
      c.refresh()
      c.complete()
      verify(future).updateGlobalSequenceIndexedOffset(id, 5)
    } finally c.close()
  }

  @Test
  def testDeletionRecreationAndShutdownIgnoreLateResults(): Unit = {
    val c = new Context
    try {
      c.start()
      c.calls.head.complete(RoutedResult(new PartitionDescription(Optional.of(new PhysicalBatch(key, 0, 4, 5)), Optional.empty()), route))
      c.retention.onMetadataUpdate(MetadataImage.EMPTY)
      c.workers.runAll()
      verify(c.log, never()).updateGlobalSequenceIndexedOffset(any[Uuid], anyLong())
      val newId = Uuid.randomUuid()
      when(c.source.topicId).thenReturn(Some(newId))
      c.retention.onMetadataUpdate(image(topicId = newId))
      c.workers.runAll()
      assertEquals(2, c.calls.size)
      c.retention.close()
      assertTrue(c.calls.last.isCancelled)
      c.refresh()
      assertEquals(2, c.calls.size)
      verify(c.log, never()).updateGlobalSequenceIndexedOffset(any[Uuid], anyLong())
    } finally c.close()
  }

  @Test
  def testNoProgressDoesNotUseSpeculativeOwnerState(): Unit = {
    val c = new Context
    try {
      c.start()
      c.calls.head.complete(RoutedResult(new PartitionDescription(Optional.empty(),
        Optional.of(new IndexerIdentity(2, 3, 9, Uuid.randomUuid()))), route))
      c.workers.runAll()
      verify(c.log).updateGlobalSequenceIndexedOffset(id, 0)
    } finally c.close()
  }

  @Test
  def testOrdinaryAndOfflineReplicasDoNotQueryOrReleasePins(): Unit = {
    val c = new Context
    try {
      c.retention.onMetadataUpdate(image(enabled = false))
      c.workers.runAll()
      assertTrue(c.calls.isEmpty)
      c.start()
      when(c.replicas.onlinePartition(tp)).thenReturn(None)
      c.complete()
      c.refresh()
      assertEquals(1, c.calls.size)
      verify(c.log, never()).updateGlobalSequenceIndexedOffset(any[Uuid], anyLong())
    } finally c.close()
  }

  @Test
  def testFatalQueryErrorKeepsPinAndDoesNotRetryOnUnrelatedMetadata(): Unit = {
    val c = new Context
    try {
      c.start()
      c.calls.head.completeExceptionally(new ClusterAuthorizationException("denied"))
      c.workers.runAll()
      c.retention.onMetadataUpdate(image())
      c.refresh()
      assertEquals(1, c.calls.size)
      verify(c.log, never()).updateGlobalSequenceIndexedOffset(any[Uuid], anyLong())
    } finally c.close()
  }
}
