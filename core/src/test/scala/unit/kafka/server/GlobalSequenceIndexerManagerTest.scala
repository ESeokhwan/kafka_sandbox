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

import kafka.cluster.{Partition, PartitionListener}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.errors.{FencedLeaderEpochException, NotLeaderOrFollowerException}
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.apache.kafka.common.metadata.{ConfigRecord, PartitionRecord, TopicRecord}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{PartitionDescription, PartitionKey}
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.server.util.MockTime
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito.{mock, when}

import java.util.concurrent.CompletableFuture
import scala.collection.mutable

class GlobalSequenceIndexerManagerTest {
  private val topicId = new Uuid(1, 2)
  private val key = new PartitionKey(topicId, 0)
  private val tp = new TopicPartition("ordered", 0)

  private def image(epoch: Int = 3, leader: Int = 1, id: Uuid = topicId, enabled: Boolean = true): MetadataImage = {
    val delta = new MetadataDelta(MetadataImage.EMPTY)
    delta.replay(new TopicRecord().setTopicId(id).setName("ordered"))
    delta.replay(new PartitionRecord().setTopicId(id).setPartitionId(0).setLeader(leader).setLeaderEpoch(epoch)
      .setReplicas(java.util.List.of(1, 2)).setIsr(java.util.List.of(1, 2)))
    delta.replay(new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName("ordered")
      .setName(TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG).setValue(enabled.toString))
    delta.apply(MetadataProvenance.EMPTY)
  }

  private class Context extends AutoCloseable {
    val workers = new GlobalSequenceTestExecutor
    val time = new MockTime(0, 0)
    val replicas: ReplicaManager = mock(classOf[ReplicaManager])
    val source: Partition = mock(classOf[Partition])
    val router: IndexRoutingManager = mock(classOf[IndexRoutingManager])
    val calls = mutable.ArrayBuffer.empty[CompletableFuture[IndexRoutingManager.RoutedResult[PartitionDescription]]]
    when(replicas.onlinePartition(tp)).thenReturn(Some(source))
    when(source.topicPartition).thenReturn(tp)
    when(source.topicId).thenReturn(Some(topicId))
    when(source.isLeader).thenReturn(true)
    when(source.getLeaderEpoch).thenReturn(3)
    when(source.maybeAddListener(any[PartitionListener])).thenReturn(true)
    when(router.describePartition(any[PartitionKey], anyLong())).thenAnswer { _ =>
      val result = new CompletableFuture[IndexRoutingManager.RoutedResult[PartitionDescription]]()
      calls += result
      result
    }
    val manager = new GlobalSequenceIndexerManager(1, replicas, mock(classOf[GlobalSequenceSourceReader]), router,
      workers, time.scheduler, 1000, 1024, mock(classOf[GlobalSequenceRetentionManager]), time)
    override def close(): Unit = { manager.close(); time.scheduler.clear() }
  }

  @Test
  def testProduceReceiptCannotAttachToAnotherSourceLifetime(): Unit = {
    val c = new Context
    try {
      val receipt = GlobalSequenceAppendReceipt(c.source, topicId, 3, 0, 2)
      assertFutureThrows(classOf[NotLeaderOrFollowerException], c.manager.awaitIndexed(receipt, 1000000000L))
      c.manager.onMetadataUpdate(image())
      val pending = c.manager.awaitIndexed(receipt, 1000000000L)
      assertFalse(pending.isDone)
      when(c.source.getLeaderEpoch).thenReturn(4)
      c.manager.onMetadataUpdate(image(epoch = 4))
      assertFutureThrows(classOf[NotLeaderOrFollowerException], c.manager.awaitIndexed(receipt, 1000000000L))
      c.workers.runAll()
      assertFutureThrows(classOf[NotLeaderOrFollowerException], pending)
      assertFutureThrows(classOf[NotLeaderOrFollowerException], c.manager.awaitIndexed(
        receipt.copy(topicId = Uuid.randomUuid(), leaderEpoch = 4), 1000000000L))
      val replaced = mock(classOf[Partition])
      when(replaced.topicPartition).thenReturn(tp)
      assertFutureThrows(classOf[NotLeaderOrFollowerException], c.manager.awaitIndexed(
        receipt.copy(source = replaced, leaderEpoch = 4), 1000000000L))
    } finally c.close()
  }

  @Test
  def testOnlyStartsEnabledLocalLeadersWithMatchingPhysicalLog(): Unit = {
    val c = new Context
    try {
      c.manager.onMetadataUpdate(image(enabled = false))
      assertTrue(c.manager.indexer(key).isEmpty)
      c.manager.onMetadataUpdate(image(leader = 2))
      assertTrue(c.manager.indexer(key).isEmpty)
      c.manager.onMetadataUpdate(image(id = Uuid.randomUuid()))
      assertTrue(c.workers.tasks.isEmpty)
      c.manager.onMetadataUpdate(image())
      assertTrue(c.calls.isEmpty, "No RPC on the metadata publisher thread")
      val indexer = c.manager.indexer(key).get
      c.workers.runAll()
      assertEquals(1, c.calls.size)
      c.manager.onMetadataUpdate(image())
      c.workers.runAll()
      assertSame(indexer, c.manager.indexer(key).get)
      assertEquals(1, c.calls.size)
    } finally c.close()
  }

  @Test
  def testFencedInstanceIsNotRestartedUntilSourceEpochChanges(): Unit = {
    val c = new Context
    try {
      c.manager.onMetadataUpdate(image())
      c.workers.runAll()
      val old = c.manager.indexer(key).get
      c.calls.head.completeExceptionally(new FencedLeaderEpochException("another indexer owns this epoch"))
      c.workers.runAll()
      assertTrue(old.isStopped)
      c.manager.onMetadataUpdate(image())
      c.workers.runAll()
      assertSame(old, c.manager.indexer(key).get)
      assertEquals(1, c.calls.size)
      when(c.source.getLeaderEpoch).thenReturn(5)
      c.manager.onMetadataUpdate(image(epoch = 5))
      c.workers.runAll()
      assertNotSame(old, c.manager.indexer(key).get)
      assertEquals(5, c.manager.indexer(key).get.sourceLeaderEpoch)
      assertEquals(2, c.calls.size)
    } finally c.close()
  }

  @Test
  def testResignationDeletionAndShutdownStopOldLifetimes(): Unit = {
    val c = new Context
    try {
      c.manager.onMetadataUpdate(image())
      c.workers.runAll()
      val old = c.manager.indexer(key).get
      c.manager.onMetadataUpdate(image(leader = 2, epoch = 4))
      assertTrue(old.isStopped)
      assertTrue(c.calls.head.isCancelled)
      assertTrue(c.manager.indexer(key).isEmpty)
      when(c.source.getLeaderEpoch).thenReturn(5)
      c.manager.onMetadataUpdate(image(epoch = 5))
      val current = c.manager.indexer(key).get
      c.manager.onMetadataUpdate(MetadataImage.EMPTY)
      assertTrue(current.isStopped)
      assertTrue(c.manager.indexer(key).isEmpty)
      c.manager.onMetadataUpdate(image(epoch = 5))
      val last = c.manager.indexer(key).get
      c.manager.close()
      assertTrue(last.isStopped)
      assertTrue(c.workers.isShutdown)
      c.manager.onMetadataUpdate(image(epoch = 5))
      assertTrue(c.manager.indexer(key).isEmpty)
    } finally c.close()
  }
}
