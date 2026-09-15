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
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.errors.{FencedLeaderEpochException, NotLeaderOrFollowerException, TimeoutException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.test.TestUtils.assertFutureThrows
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock

import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.collection.Map

class GlobalSequenceProduceTest {
  private class Context {
    val time = new MockTime(0, 0)
    var current = true
    var hw = 100L
    val waiters = new GlobalSequenceIndexWaiters(time.scheduler, time, () => current, () => hw)
    def await(offset: Long, remainingMs: Long = 100): CompletableFuture[Void] =
      waiters.await(offset, time.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(remainingMs))
  }

  @Test
  def testWaiterAdmissionFairnessAndTimeoutDoNotLoseCommittedProgress(): Unit = {
    val time = new MockTime(0, 0)
    val limits = new GlobalSequenceTestResources(time)
    val a = new GlobalSequenceIndexWaiters(time.scheduler, time, () => true, () => 100L, limits.resources, "a")
    val b = new GlobalSequenceIndexWaiters(time.scheduler, time, () => true, () => 100L, limits.resources, "b")
    try {
      val expired = a.await(10, TimeUnit.MILLISECONDS.toNanos(10))
      assertFutureThrows(classOf[org.apache.kafka.common.errors.ThrottlingQuotaExceededException], a.await(10, TimeUnit.SECONDS.toNanos(1)))
      val other = b.await(20, TimeUnit.SECONDS.toNanos(1))
      time.sleep(10)
      assertFutureThrows(classOf[TimeoutException], expired)
      val retried = a.await(10, TimeUnit.SECONDS.toNanos(1))
      a.advance(10)
      b.advance(20)
      assertTrue(retried.isDone)
      assertTrue(other.isDone)
      assertTrue(a.await(10, TimeUnit.SECONDS.toNanos(1)).isDone)
      assertEquals(0L, limits.resources.used(org.apache.kafka.coordinator.globalsequence.GlobalSequenceResources.Scope.PRODUCE))
    } finally { limits.close(); time.scheduler.clear() }
  }

  @Test
  def testCompletedBeforeAndDuringRegistrationAndIndependentRanges(): Unit = {
    val c = new Context
    c.waiters.advance(10)
    assertTrue(c.await(10).isDone)
    assertEquals(0, c.waiters.size)
    val first = c.await(12)
    val second = c.await(15)
    c.waiters.advance(12)
    assertTrue(first.isDone)
    assertFalse(second.isDone)
    c.waiters.advance(15)
    assertTrue(second.isDone)
    assertEquals(0, c.waiters.size)
    c.time.sleep(200)
    assertFalse(first.isCompletedExceptionally)
  }

  @Test
  def testRecoveredIndexProgressAlsoWaitsForCurrentDataHighWatermark(): Unit = {
    val c = new Context
    c.hw = 5
    c.waiters.advance(10)
    val future = c.await(10)
    assertFalse(future.isDone)
    c.hw = 10
    c.waiters.advance(10)
    assertTrue(future.isDone)
    assertFalse(future.isCompletedExceptionally)
  }

  @Test
  def testDeadlineDetachesOnlyExpiredWaiterAndKeepsProgress(): Unit = {
    val c = new Context
    val expired = c.await(10, 20)
    val surviving = c.await(10, 100)
    c.time.sleep(20)
    assertFutureThrows(classOf[TimeoutException], expired)
    assertEquals(1, c.waiters.size)
    c.waiters.advance(10)
    assertTrue(surviving.isDone)
    assertTrue(c.await(10).isDone)
    assertEquals(0, c.waiters.size)
  }

  @Test
  def testExpiredBudgetCannotBeRestartedByAlreadyCommittedProgress(): Unit = {
    val c = new Context
    c.waiters.advance(10)
    assertFutureThrows(classOf[TimeoutException], c.await(10, 0))
    assertEquals(0, c.waiters.size)
  }

  @Test
  def testCancellationOnlyRemovesItsSubscription(): Unit = {
    val c = new Context
    val cancelled = c.await(10)
    val survivor = c.await(10)
    cancelled.cancel(false)
    assertEquals(1, c.waiters.size)
    c.waiters.advance(10)
    assertFalse(survivor.isCompletedExceptionally)
    assertTrue(survivor.isDone)
    assertTrue(cancelled.isCancelled)
  }

  @Test
  def testSourceEpochChangeCannotAckReusedOffsets(): Unit = {
    val c = new Context
    val pending = c.await(10)
    c.current = false
    c.waiters.advance(10)
    assertFutureThrows(classOf[NotLeaderOrFollowerException], pending)
    assertFutureThrows(classOf[NotLeaderOrFollowerException], c.await(10))
  }

  @Test
  def testEpochChangeWhileReadingHighWatermarkCannotAckOldReceipt(): Unit = {
    val time = new MockTime(0, 0)
    var current = true
    val waiters = new GlobalSequenceIndexWaiters(time.scheduler, time, () => current, () => {
      current = false
      10L
    })
    waiters.advance(10)
    assertFutureThrows(classOf[NotLeaderOrFollowerException], waiters.await(10, TimeUnit.SECONDS.toNanos(1)))
  }

  @Test
  def testConcurrentSubscriptionAndCommitNeverLoseCompletion(): Unit = {
    for (_ <- 0 until 100) {
      val c = new Context
      val subscribing = CompletableFuture.supplyAsync(() => c.await(10))
      val committing = CompletableFuture.runAsync(() => c.waiters.advance(10))
      committing.get(5, TimeUnit.SECONDS)
      val result = subscribing.get(5, TimeUnit.SECONDS)
      assertTrue(result.isDone)
      assertFalse(result.isCompletedExceptionally)
      assertEquals(0, c.waiters.size)
      c.time.scheduler.clear()
    }
  }

  @Test
  def testCloseDoesNotInvokeResponseCallbackOnPartitionListenerThread(): Unit = {
    val c = new Context
    val executor = new GlobalSequenceTestExecutor
    val pending = c.await(10)
    c.waiters.close(Errors.NOT_LEADER_OR_FOLLOWER.exception(), executor)
    assertFalse(pending.isDone)
    assertEquals(0, c.waiters.size)
    assertFutureThrows(classOf[NotLeaderOrFollowerException], c.await(10))
    executor.runAll()
    assertFutureThrows(classOf[NotLeaderOrFollowerException], pending)
  }

  @Test
  def testMixedResponsesKeepPhysicalOffsetsAndOnlyWaitOnSuccessfulGlobalPartitions(): Unit = {
    val id = Uuid.randomUuid()
    def tp(name: String) = new TopicIdPartition(id, new TopicPartition(name, 0))
    val ordered = tp("ordered")
    val ordinary = tp("ordinary")
    val failed = tp("failed")
    val second = tp("second")
    val response = new PartitionResponse(Errors.NONE, 50, 123, 4)
    val responses = Map(ordered -> response, ordinary -> new PartitionResponse(Errors.NONE, 8, 100, 2),
      failed -> new PartitionResponse(Errors.NOT_ENOUGH_REPLICAS), second -> new PartitionResponse(Errors.NONE, 70, 123, 4))
    val receipt = GlobalSequenceAppendReceipt(mock(classOf[Partition]), id, 3, 50, 54)
    val secondReceipt = receipt.copy(firstOffset = 70, lastOffset = 79)
    val firstWait = new CompletableFuture[Void]()
    val secondWait = new CompletableFuture[Void]()
    var calls = 0
    var completed: Map[TopicIdPartition, PartitionResponse] = Map.empty
    GlobalSequenceProduce.awaitIndexes(responses, Map(ordered -> receipt, failed -> receipt, second -> secondReceipt), value => {
      calls += 1
      if (value == receipt) firstWait else secondWait
    }, result => completed = result)
    assertEquals(2, calls)
    firstWait.complete(null)
    assertTrue(completed.isEmpty)
    secondWait.completeExceptionally(new FencedLeaderEpochException("lost ownership"))
    assertEquals(Errors.NONE, completed(ordered).error)
    assertEquals(50L, completed(ordered).baseOffset)
    assertEquals(123L, completed(ordered).logAppendTime)
    assertEquals(4L, completed(ordered).logStartOffset)
    assertEquals(Errors.NONE, completed(ordinary).error)
    assertEquals(8L, completed(ordinary).baseOffset)
    assertEquals(Errors.NOT_ENOUGH_REPLICAS, completed(failed).error)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, completed(second).error)
    assertEquals(70L, completed(second).baseOffset)
  }
}
