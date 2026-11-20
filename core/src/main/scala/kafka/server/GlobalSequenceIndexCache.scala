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
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.metrics.{Gauge, MetricConfig, Metrics}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceLookup.{Mapping, Snapshot}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.PhysicalBatch

import java.util.{Arrays, LinkedHashMap, List => JList, TreeMap}
import java.lang.ref.WeakReference
import java.util.concurrent.atomic.LongAdder
import scala.collection.mutable

private[server] object GlobalSequenceIndexCache {
  /** Identity equality without keeping an unloaded shard or a closed log alive. */
  final class Identity(value: AnyRef) extends WeakReference[AnyRef](value) {
    private val identityHash = System.identityHashCode(value)
    override def hashCode(): Int = identityHash
    override def equals(that: Any): Boolean = that match {
      case other: Identity =>
        val current = get()
        (this eq other) || (current != null && (current eq other.get()))
      case _ => false
    }
  }
  // Identity-bearing objects keep late reads in their original shard/log namespace.
  case class Scope(indexTopicId: Uuid, indexPartition: Int, leaderEpoch: Int,
                   generation: AnyRef, partition: AnyRef, log: AnyRef)
  final class Entry private (val mapping: Mapping, val indexEndOffset: Long, private val batchEnds: Array[Int]) {
    def appendBatches(from: Long, end: Long, limit: Int, selected: mutable.ArrayBuffer[Mapping],
                      checkDeadline: () => Unit): Unit = {
      if (batchEnds.length == 0) selected += mapping
      else {
        val range = mapping.batch()
        val relative = (from - mapping.globalBaseOffset()).toInt
        val found = Arrays.binarySearch(batchEnds, relative + 1)
        var i = if (found >= 0) found else -found - 1
        var start = if (i == 0) 0 else batchEnds(i - 1)
        while (i < batchEnds.length && mapping.globalBaseOffset() + start < end && selected.size < limit) {
          checkDeadline()
          val count = batchEnds(i) - start
          val base = range.baseOffset() + start
          selected += new Mapping(mapping.globalBaseOffset() + start,
            new PhysicalBatch(range.partition(), base, base + count - 1, count))
          start = batchEnds(i)
          i += 1
        }
      }
    }

    def sameRange(other: Entry): Boolean =
      mapping == other.mapping && indexEndOffset == other.indexEndOffset && Arrays.equals(batchEnds, other.batchEnds)
  }

  object Entry {
    private val SingleBatch = Array.emptyIntArray

    def apply(mapping: Mapping, indexEndOffset: Long, counts: JList[Integer] = JList.of[Integer]()): Entry = {
      val ends = if (counts.isEmpty) SingleBatch else {
        val values = new Array[Int](counts.size())
        val iterator = counts.iterator()
        var end = 0
        var i = 0
        while (iterator.hasNext) {
          end += iterator.next().intValue()
          values(i) = end
          i += 1
        }
        values
      }
      new Entry(mapping, indexEndOffset, ends)
    }
  }
  private case class Key(scope: Scope, topicId: Uuid, globalBaseOffset: Long)
}

/** Exact LRU with a range index for interior offsets. All indexes share one short-held monitor. */
private[server] class GlobalSequenceIndexCache(maxEntries: Int, maxEntriesPerTopic: Int, metrics: Metrics = null)
  extends AutoCloseable {
  import GlobalSequenceIndexCache._

  require(maxEntries >= 0 && maxEntriesPerTopic > 0, "Invalid index cache capacity")
  private val lru = new LinkedHashMap[Key, Entry](16, 0.75f, true)
  private val topics = mutable.HashMap.empty[Uuid, LinkedHashMap[Key, Entry]]
  private val ranges = mutable.HashMap.empty[(Scope, Uuid), TreeMap[java.lang.Long, Entry]]
  private val scans = new LongAdder
  private var hits = 0L
  private var misses = 0L
  private var evictions = 0L
  private var closed = false

  private val metricNames = Option(metrics).toSeq.flatMap { registry =>
    Seq[(String, () => Long)]("hits-total" -> (() => hitCount), "misses-total" -> (() => missCount),
      "evictions-total" -> (() => evictionCount), "scan-lookups-total" -> (() => scans.sum()),
      "entries" -> (() => size.toLong)).map { case (name, read) =>
      val metric = registry.metricName(name, "global-sequence-index-cache", "Global sequence mapping cache " + name)
      registry.addMetric(metric, new Gauge[Long] {
        override def value(config: MetricConfig, now: Long): Long = read()
      })
      metric
    }
  }

  def get(scope: Scope, topicId: Uuid, offset: Long, snapshot: Snapshot): Option[Entry] = synchronized {
    if (closed || maxEntries == 0) return None
    val entry = ranges.get((scope, topicId)).flatMap(index => Option(index.floorEntry(offset))).map(_.getValue)
      .filter(value => value.mapping.globalEndOffset() > offset &&
        value.indexEndOffset <= snapshot.indexHighWatermark() && value.mapping.globalEndOffset() <= snapshot.committedGlobalEnd())
    entry match {
      case Some(value) =>
        val key = Key(scope, topicId, value.mapping.globalBaseOffset())
        lru.get(key)
        topics(topicId).get(key)
        hits += 1
      case None => misses += 1
    }
    entry
  }

  def putAll(scope: Scope, entries: Seq[Entry]): Unit = synchronized {
    if (closed || maxEntries == 0) return
    entries.foreach { entry =>
      val topicId = entry.mapping.batch().partition().topicId()
      val key = Key(scope, topicId, entry.mapping.globalBaseOffset())
      val previous = lru.get(key)
      if (previous != null && !previous.sameRange(entry))
        throw new CorruptRecordException("Committed mapping changed within one index cache generation")
      lru.put(key, entry)
      val topic = topics.getOrElseUpdate(topicId, new LinkedHashMap[Key, Entry](16, 0.75f, true))
      topic.put(key, entry)
      ranges.getOrElseUpdate((scope, topicId), new TreeMap[java.lang.Long, Entry]).put(key.globalBaseOffset, entry)
      if (topic.size() > maxEntriesPerTopic) evict(topic.keySet().iterator().next())
      if (lru.size() > maxEntries) evict(lru.keySet().iterator().next())
    }
  }

  private def evict(key: Key): Unit = {
    lru.remove(key)
    val topic = topics(key.topicId)
    topic.remove(key)
    if (topic.isEmpty) topics.remove(key.topicId)
    val index = ranges((key.scope, key.topicId))
    index.remove(key.globalBaseOffset)
    if (index.isEmpty) ranges.remove((key.scope, key.topicId))
    evictions += 1
  }

  def recordScan(): Unit = scans.increment()

  def enabled: Boolean = maxEntries > 0

  def size: Int = synchronized { lru.size() }
  def hitCount: Long = synchronized { hits }
  def missCount: Long = synchronized { misses }
  def evictionCount: Long = synchronized { evictions }

  override def close(): Unit = {
    synchronized {
      closed = true
      lru.clear()
      topics.clear()
      ranges.clear()
    }
    Option(metrics).foreach(registry => metricNames.foreach(registry.removeMetric))
  }
}
