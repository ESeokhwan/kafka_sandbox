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

import org.apache.kafka.common.Uuid
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{PartitionKey, PhysicalBatch}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookup.{Mapping, Snapshot}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.concurrent.{Callable, Executors, TimeUnit}

class GlobalSequenceIndexCacheTest {
  import GlobalSequenceIndexCache._

  private val topic = new Uuid(1, 2)
  private val other = new Uuid(3, 4)
  private val index = new Uuid(5, 6)
  private val scope = Scope(index, 0, 10, new Object, new Object, new Object)
  private val snapshot = new Snapshot(index, 0, 10, 200, 200)
  private def entry(id: Uuid, base: Long, count: Int = 3): Entry =
    Entry(new Mapping(base, new PhysicalBatch(new PartitionKey(id, 0), base, base + count - 1, count)), base + 2)

  @Test
  def testInteriorHitsAndInsertionsRefreshGlobalAndPerTopicLru(): Unit = {
    val cache = new GlobalSequenceIndexCache(3, 2)
    try {
      val a = entry(topic, 0)
      val b = entry(topic, 3)
      val c = entry(topic, 6)
      cache.putAll(scope, Seq(a, b))
      assertEquals(Some(a), cache.get(scope, topic, 1, snapshot))
      cache.putAll(scope, Seq(c))
      assertEquals(None, cache.get(scope, topic, 4, snapshot), "The topic limit evicts B after A was touched")
      cache.putAll(scope, Seq(entry(other, 0)))
      assertEquals(Some(a), cache.get(scope, topic, 2, snapshot))
      cache.putAll(scope, Seq(entry(other, 3)))
      assertEquals(None, cache.get(scope, topic, 7, snapshot), "The global limit evicts C")
      assertEquals(3, cache.size)
      assertEquals(2L, cache.evictionCount)
      cache.putAll(scope, Seq(a))
      cache.putAll(scope, Seq(entry(other, 6)))
      assertEquals(Some(a), cache.get(scope, topic, 0, snapshot), "Reinserting A also makes it most recent")
      assertEquals(3, cache.size)
    } finally cache.close()
  }

  @Test
  def testSnapshotMustCoverTheWholeCommittedPairAndGlobalRange(): Unit = {
    val cache = new GlobalSequenceIndexCache(4, 4)
    try {
      val mapping = entry(topic, 3)
      cache.putAll(scope, Seq(mapping))
      assertEquals(None, cache.get(scope, topic, 4, new Snapshot(index, 0, 10, 4, 6)))
      assertEquals(None, cache.get(scope, topic, 4, new Snapshot(index, 0, 10, 5, 5)))
      assertEquals(Some(mapping), cache.get(scope, topic, 4, new Snapshot(index, 0, 10, 5, 6)))
      assertEquals(None, cache.get(scope, topic, 6, snapshot), "The upper bound is exclusive")
    } finally cache.close()
  }

  @Test
  def testTopicAndIndexGenerationsFenceLateInsertions(): Unit = {
    val cache = new GlobalSequenceIndexCache(10, 10)
    try {
      val fresh = scope.copy(generation = new Object)
      val value = entry(topic, 0)
      cache.putAll(fresh, Seq(value))
      cache.putAll(scope, Seq(entry(topic, 3)))
      assertEquals(None, cache.get(fresh, topic, 4, snapshot))
      assertEquals(Some(value), cache.get(fresh, topic, 1, snapshot))
      for (changed <- Seq(scope.copy(leaderEpoch = 11), scope.copy(indexTopicId = other),
        scope.copy(indexPartition = 1), scope.copy(log = new Object), scope.copy(partition = new Object))) {
        assertEquals(None, cache.get(changed, topic, 4, snapshot))
      }
      assertEquals(None, cache.get(scope, other, 4, snapshot))
    } finally cache.close()
  }

  @Test
  def testConcurrentAccessRespectsCapacityAndCloseRejectsLateWrites(): Unit = {
    val cache = new GlobalSequenceIndexCache(20, 4)
    val executor = Executors.newFixedThreadPool(4)
    try {
      val tasks = (0 until 4).map { worker =>
        executor.submit(new Callable[Unit] {
          override def call(): Unit = {
            val id = new Uuid(1, worker + 1)
            for (i <- 0 until 1000) {
              val value = entry(id, i % 64)
              cache.putAll(scope, Seq(value))
              cache.get(scope, id, value.mapping.globalBaseOffset(), snapshot)
              assertTrue(cache.size <= 20)
            }
          }
        })
      }
      tasks.foreach(_.get(10, TimeUnit.SECONDS))
      assertTrue(cache.size <= 16)
      cache.close()
      cache.putAll(scope, Seq(entry(topic, 0)))
      assertEquals(0, cache.size)
    } finally { executor.shutdownNow(); cache.close() }
  }

  @Test
  def testObjectIdentityScopesDoNotUseValueEquality(): Unit = {
    val first = new String("log")
    val second = new String("log")
    val cache = new GlobalSequenceIndexCache(1, 1)
    val original = scope.copy(log = new Identity(first))
    try {
      cache.putAll(original, Seq(entry(topic, 0)))
      assertTrue(first.equals(second))
      assertEquals(Some(entry(topic, 0)), cache.get(scope.copy(log = new Identity(first)), topic, 1, snapshot))
      assertEquals(None, cache.get(scope.copy(log = new Identity(second)), topic, 1, snapshot))
      original.log.asInstanceOf[Identity].clear()
      cache.putAll(scope, Seq(entry(topic, 3)))
      assertEquals(1, cache.size, "A collected identity must still allow eviction from all indexes")
    } finally cache.close()
  }

  @Test
  def testDisableAndMetricLifecycle(): Unit = {
    val disabled = new GlobalSequenceIndexCache(0, 1)
    disabled.putAll(scope, Seq(entry(topic, 0)))
    assertEquals(None, disabled.get(scope, topic, 0, snapshot))
    assertEquals(0, disabled.size)
    disabled.close()
    val metrics = new Metrics
    val cache = new GlobalSequenceIndexCache(1, 1, metrics)
    val names = Seq("hits-total", "misses-total", "evictions-total", "entries")
      .map(metrics.metricName(_, "global-sequence-index-cache"))
    try {
      cache.putAll(scope, Seq(entry(topic, 0)))
      cache.get(scope, topic, 1, snapshot)
      cache.get(scope, topic, 4, snapshot)
      cache.putAll(scope, Seq(entry(topic, 3)))
      names.foreach(name => assertEquals(1L, metrics.metric(name).metricValue()))
      cache.close()
      names.foreach(name => assertNull(metrics.metric(name)))
    } finally { cache.close(); metrics.close() }
  }
}
