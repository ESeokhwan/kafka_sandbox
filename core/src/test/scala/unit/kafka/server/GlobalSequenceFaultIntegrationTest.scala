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

import kafka.examples.globalsequence.GlobalSequenceReadDemo
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, NewPartitionReassignment, NewTopic, RecordsToDelete}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.{ElectionType, TopicPartition, Uuid}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.internals.Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME
import org.apache.kafka.common.message.{FetchGlobalSequenceRequestData, FetchGlobalSequenceResponseData, LookupGlobalSequenceRequestData, ProduceRequestData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.test.{KafkaClusterTestKit, TestKitNodes}
import org.apache.kafka.coordinator.globalsequence.{GlobalSequenceCoordinatorConfig, GlobalSequenceCoordinatorRecordSerde}
import org.apache.kafka.coordinator.globalsequence.GlobalSequenceCoordinatorShard.{AppendRequest, PartitionKey, PhysicalBatch}
import org.apache.kafka.coordinator.globalsequence.generated.{BatchIndexKey, BatchIndexValue, TopicMetadataKey, TopicMetadataValue}
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.storage.internals.log.AppendOrigin
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration
import java.util
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Faults alter actual replica fetchers/lifetimes; the oracle reads durable files and index records independently. */
@Timeout(240)
class GlobalSequenceFaultIntegrationTest {
  private case class TopicRef(name: String, id: Uuid) {
    def tp(partition: Int): TopicPartition = new TopicPartition(name, partition)
    def key(partition: Int): PartitionKey = new PartitionKey(id, partition)
  }
  private case class Physical(partition: Int, base: Long, last: Long, values: Vector[Int])
  private case class Indexed(global: Long, partition: Int, base: Long, last: Long, count: Int)
  private def value(number: Int): Array[Byte] = ByteBuffer.allocate(4).putInt(number).array()
  private def records(numbers: Seq[Int]): MemoryRecords =
    MemoryRecords.withRecords(Compression.NONE, numbers.map(n => new SimpleRecord(value(n))): _*)

  private class Context extends AutoCloseable {
    val cluster = new KafkaClusterTestKit.Builder(new TestKitNodes.Builder().setNumBrokerNodes(3).setNumControllerNodes(1).build())
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_NUM_PARTITIONS_CONFIG, "2")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_REPLICATION_FACTOR_CONFIG, "3")
      .setConfigProp(GlobalSequenceCoordinatorConfig.INDEX_TOPIC_MIN_ISR_CONFIG, "2")
      .setConfigProp(GlobalSequenceCoordinatorConfig.WRITE_TIMEOUT_MS_CONFIG, "500")
      .setConfigProp(GlobalSequenceCoordinatorConfig.RETENTION_REFRESH_INTERVAL_MS_CONFIG, "100")
      // Paused index followers must stay in ISR, so the test controls HW rather than racing ISR eviction.
      .setConfigProp("replica.lag.time.max.ms", "600000")
      .build()
    var admin: Admin = _
    val stopped = mutable.Set.empty[Int]
    def brokers: Vector[BrokerServer] = cluster.brokers().values().asScala.toVector.sortBy(_.config.brokerId)
    def live: Vector[BrokerServer] = brokers.filterNot(b => stopped(b.config.brokerId))
    def start(): Unit = {
      cluster.format()
      cluster.startup()
      cluster.waitForReadyBrokers()
      admin = Admin.create(cluster.clientProperties())
    }
    def create(name: String): TopicRef = {
      val ids = brokers.map(b => Int.box(b.config.brokerId))
      val created = admin.createTopics(util.List.of(new NewTopic(name, util.Map.of(Int.box(0), ids.asJava,
        Int.box(1), (ids.tail :+ ids.head).asJava)).configs(util.Map.of(
        TopicConfig.GLOBAL_SEQUENCE_ENABLED_CONFIG, "true", TopicConfig.CLEANUP_POLICY_CONFIG, "delete"))))
      created.all().get(30, TimeUnit.SECONDS)
      val ref = TopicRef(name, created.topicId(name).get(30, TimeUnit.SECONDS))
      await("Source metadata and committed owners") {
        live.forall(_.metadataCache.getTopicId(name) == ref.id) && (0 to 1).forall { p =>
          Try(live.head.indexRoutingManager.describePartition(ref.key(p), 1000).get(2, TimeUnit.SECONDS))
            .toOption.exists(_.value.currentIndexer().isPresent)
        }
      }
      ref
    }
    def await(reason: String)(condition: => Boolean): Unit =
      TestUtils.waitUntilTrue(() => condition, reason, 30000)
    def leader(tp: TopicPartition): BrokerServer = {
      val id = live.head.metadataCache.getImage().topics().getTopic(tp.topic()).partitions().get(tp.partition()).leader
      live.find(_.config.brokerId == id).get
    }
    def indexTp(ref: TopicRef): TopicPartition =
      new TopicPartition(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, live.head.globalSequenceCoordinator.partitionFor(ref.id))
    def progress(ref: TopicRef, p: Int, last: Long): Unit = await(s"Committed progress $ref/$p through $last") {
      Try(live.head.indexRoutingManager.describePartition(ref.key(p), 1000).get(2, TimeUnit.SECONDS)).toOption.exists { r =>
        r.value.committedProgress().isPresent && r.value.committedProgress().get().lastOffset() == last
      }
    }
    def append(ref: TopicRef, p: Int, numbers: Seq[Int], indexed: Boolean = true): Physical = {
      val info = leader(ref.tp(p)).replicaManager.onlinePartition(ref.tp(p)).get.appendRecordsToLeader(
        records(numbers), AppendOrigin.CLIENT, -1, RequestLocal.noCaching)
      await("Data batch committed on every live replica") {
        live.forall(_.replicaManager.localLog(ref.tp(p)).exists(_.highWatermark() > info.lastOffset()))
      }
      if (indexed) progress(ref, p, info.lastOffset())
      Physical(p, info.firstOffset(), info.lastOffset(), numbers.toVector)
    }
    def produce(ref: TopicRef, p: Int, data: MemoryRecords, timeoutMs: Int): ProduceResponse = {
      val topic = new ProduceRequestData.TopicProduceData().setName(ref.name).setPartitionData(util.List.of(
        new ProduceRequestData.PartitionProduceData().setIndex(p).setRecords(MemoryRecords.readableRecords(data.buffer().duplicate()))))
      val request = ProduceRequest.builder(new ProduceRequestData().setAcks(-1.toShort).setTimeoutMs(timeoutMs)
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(util.List.of(topic).iterator()))).build(12.toShort)
      val broker = leader(ref.tp(p))
      IntegrationTestUtils.connectAndReceive[ProduceResponse](request, broker.socketServer, broker.config.interBrokerListenerName)
    }
    def fetch(ref: TopicRef, broker: BrokerServer, start: Long = 0, end: Long = Long.MaxValue): FetchGlobalSequenceResponseData = {
      val request = new FetchGlobalSequenceRequest.Builder(new FetchGlobalSequenceRequestData().setTopicId(ref.id)
        .setGlobalStartOffset(start).setGlobalEndOffsetExclusive(end).setMaxBytes(1).setMaxBatches(2)
        .setTimeoutMs(10000)).build(1.toShort)
      IntegrationTestUtils.connectAndReceive[FetchGlobalSequenceResponse](request, broker.socketServer, broker.config.interBrokerListenerName).data()
    }
    def move(tp: TopicPartition, target: BrokerServer): Unit = {
      val targetId = Int.box(target.config.brokerId)
      val ids = Seq(targetId) ++ brokers.map(b => Int.box(b.config.brokerId)).filterNot(_ == targetId)
      admin.alterPartitionReassignments(util.Map.of(tp, util.Optional.of(new NewPartitionReassignment(ids.asJava))))
        .all().get(30, TimeUnit.SECONDS)
      await("All replicas rejoined ISR before election") {
        admin.listPartitionReassignments().reassignments().get(10, TimeUnit.SECONDS).isEmpty &&
          live.forall(_.metadataCache.getImage().topics().getTopic(tp.topic()).partitions().get(tp.partition()).isr.length == 3)
      }
      admin.electLeaders(ElectionType.PREFERRED, util.Set.of(tp)).all().get(30, TimeUnit.SECONDS)
      await("Leader election metadata") {
        live.forall(_.metadataCache.getImage().topics().getTopic(tp.topic()).partitions().get(tp.partition()).leader == target.config.brokerId)
      }
    }
    def shutdown(broker: BrokerServer): Unit = { stopped += broker.config.brokerId; broker.shutdown() }
    def restart(broker: BrokerServer): Unit = {
      broker.startup()
      stopped -= broker.config.brokerId
      cluster.waitForReadyBrokers()
    }
    def pauseIndex(ref: TopicRef): AutoCloseable = {
      val tp = indexTp(ref)
      val owner = leader(tp)
      val image = owner.metadataCache.getImage().topics().getTopic(tp.topic())
      val epoch = image.partitions().get(tp.partition()).leaderEpoch
      await("Index caught up before the fault") {
        val log = owner.replicaManager.localLog(tp).get
        log.highWatermark() == log.logEndOffset()
      }
      val node = owner.metadataCache.getAliveBrokerNode(owner.config.brokerId, owner.config.interBrokerListenerName).get
      val endpoint = new BrokerEndPoint(node.id(), node.host(), node.port())
      val followers = live.filterNot(_ eq owner)
      followers.foreach { follower =>
        assertTrue(follower.replicaManager.replicaFetcherManager.removeFetcherForPartitions(Set(tp)).contains(tp))
      }
      new AutoCloseable {
        override def close(): Unit = followers.filterNot(b => stopped(b.config.brokerId)).foreach { follower =>
          val current = follower.metadataCache.getImage().topics().getTopic(tp.topic()).partitions().get(tp.partition())
          // Election metadata reconstructs fetchers itself; never reinstall an obsolete source epoch.
          if (current.leader == owner.config.brokerId && current.leaderEpoch == epoch) {
            follower.replicaManager.replicaFetcherManager.addFetcherForPartitions(Map(tp -> InitialFetchState(
              Some(image.id()), endpoint, epoch, follower.replicaManager.localLog(tp).get.logEndOffset())))
          }
        }
      }
    }
    def replayOldAppend(request: AppendRequest): Short = {
      val tp = indexTp(TopicRef("", request.batch().partition().topicId()))
      val broker = leader(tp)
      val epoch = broker.metadataCache.getImage().topics().getTopic(tp.topic()).partitions().get(tp.partition()).leaderEpoch
      IntegrationTestUtils.connectAndReceive[AppendGlobalSequenceIndexResponse](GlobalSequenceProtocol.appendRequest(request, epoch).build(),
        broker.socketServer, broker.config.interBrokerListenerName).data().errorCode()
    }

    /** Snapshot of actual source batches, without consulting global mappings or committed progress. */
    def physical(ref: TopicRef): Vector[Physical] = (0 to 1).toVector.flatMap { p =>
      val log = leader(ref.tp(p)).replicaManager.localLog(ref.tp(p)).get
      val hw = log.highWatermark()
      log.logSegments().asScala.toVector.flatMap(_.log().batches().iterator().asScala).filter { batch =>
        !batch.isControlBatch && batch.lastOffset() < hw
      }.map { batch =>
        batch.ensureValid()
        Physical(p, batch.baseOffset(), batch.lastOffset(), batch.iterator().asScala.map(_.value().getInt()).toVector)
      }
    }
    /** Consumer reads committed index records; verify each allocation's adjacent metadata record too. */
    def indexes(ref: TopicRef): Vector[Indexed] = {
      val props = cluster.clientProperties()
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
      try {
        val tp = indexTp(ref)
        consumer.assign(util.List.of(tp))
        consumer.seekToBeginning(util.List.of(tp))
        val end = consumer.endOffsets(util.List.of(tp)).get(tp)
        val serde = new GlobalSequenceCoordinatorRecordSerde
        val decoded = mutable.ArrayBuffer.empty[(Long, org.apache.kafka.coordinator.common.runtime.CoordinatorRecord)]
        await("Independent committed index scan") {
          consumer.poll(Duration.ofMillis(100)).asScala.filter(_.offset() < end).foreach { record =>
            decoded += ((record.offset(), serde.deserialize(ByteBuffer.wrap(record.key()), ByteBuffer.wrap(record.value()))))
          }
          consumer.position(tp) >= end
        }
        decoded.zipWithIndex.collect { case ((offset, record), position) if record.key().isInstanceOf[BatchIndexKey] &&
          record.key().asInstanceOf[BatchIndexKey].topicId() == ref.id =>
          val key = record.key().asInstanceOf[BatchIndexKey]
          val batch = record.value().message().asInstanceOf[BatchIndexValue]
          assertTrue(position + 1 < decoded.size, "Committed allocation must include adjacent topic metadata")
          val (nextOffset, next) = decoded(position + 1)
          assertEquals(offset + 1, nextOffset)
          assertEquals(ref.id, next.key().asInstanceOf[TopicMetadataKey].topicId())
          assertEquals(key.globalBaseOffset() + batch.recordCount(), next.value().message().asInstanceOf[TopicMetadataValue].nextGlobalOffset())
          Indexed(key.globalBaseOffset(), batch.physicalPartition(), batch.physicalBaseOffset(), batch.physicalLastOffset(), batch.recordCount())
        }.toVector
      } finally consumer.close()
    }
    def verify(ref: TopicRef, expected: Map[Int, Seq[Int]]): Vector[Indexed] = {
      val source = physical(ref)
      expected.foreach { case (partition, values) => assertEquals(values, source.filter(_.partition == partition).flatMap(_.values)) }
      val index = indexes(ref)
      assertEquals(source.size, index.size, "Exactly one mapping per durable physical batch")
      assertEquals(index.size, index.map(i => (i.partition, i.base)).distinct.size, "No duplicate physical identity")
      val byPhysical = source.map(p => (p.partition, p.base) -> p).toMap
      var next = 0L
      val global = index.flatMap { i =>
        assertEquals(next, i.global, "Global ranges are contiguous and non-overlapping in index log order")
        val data = byPhysical((i.partition, i.base))
        assertEquals(data.last, i.last)
        assertEquals(data.values.size, i.count)
        next += i.count
        data.values.zipWithIndex.map { case (number, offset) => (i.global + offset, number) }
      }
      expected.keys.foreach { p => assertEquals(source.filter(_.partition == p).map(_.base), index.filter(_.partition == p).map(_.base)) }
      live.foreach { broker =>
        for ((start, end) <- Seq((0L, next), (1L, math.max(1L, next - 1)))) {
          var cursor = start
          val actual = mutable.ArrayBuffer.empty[(Long, Int)]
          while (cursor < end) {
            val page = fetch(ref, broker, cursor, end)
            assertEquals(Errors.NONE.code(), page.errorCode(), page.errorMessage())
            assertEquals(next, page.committedGlobalEndOffset())
            assertTrue(page.nextGlobalOffset() > cursor && page.nextGlobalOffset() <= end)
            page.batches().asScala.foreach { entry =>
              val batch = entry.records().asInstanceOf[MemoryRecords].batches().iterator().next()
              batch.ensureValid()
              batch.iterator().asScala.foreach { record =>
                val offset = entry.globalBaseOffset() + record.offset() - entry.physicalBaseOffset()
                if (offset >= entry.selectedGlobalStartOffset() && offset < entry.selectedGlobalEndOffset())
                  actual += ((offset, record.value().getInt()))
              }
            }
            cursor = page.nextGlobalOffset()
          }
          assertEquals(global.filter { case (offset, _) => offset >= start && offset < end }, actual.toVector)
        }
      }
      // Exercise the documented client (including ApiVersions negotiation) against the same independent oracle.
      for (mode <- Seq("lookup", "read_uncommitted", "read_committed")) {
        val bytes = new ByteArrayOutputStream
        val output = new PrintStream(bytes, true, UTF_8)
        try GlobalSequenceReadDemo.read(cluster.clientProperties(), ref.id, 1, next - 1, mode, output)
        finally output.close()
        val lines = bytes.toString(UTF_8).linesIterator.toVector
        assertTrue(lines.last.contains(s"next=${next - 1}"), lines.toString)
        if (mode == "lookup") {
          assertEquals(index.count(i => i.global + i.count > 1 && i.global < next - 1), lines.count(_.startsWith("mapping\t")))
        } else {
          val selected = lines.filter(_.startsWith("record\t")).map { line =>
            val fields = line.split("\t")
            (fields(1).toLong, ByteBuffer.wrap(util.Base64.getDecoder.decode(fields(4))).getInt())
          }
          assertEquals(global.filter { case (offset, _) => offset >= 1 && offset < next - 1 }, selected)
        }
      }
      index
    }
    override def close(): Unit = { if (admin != null) admin.close(); cluster.close() }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testIndexHwTimeoutAndResponseLossRecoverWithoutDuplicateMappings(failover: Boolean): Unit = {
    val c = new Context
    try {
      c.start()
      val topic = c.create("hw-fault")
      c.append(topic, 0, Seq(10))
      c.append(topic, 1, Seq(20))
      val indexTp = c.indexTp(topic)
      val oldLeader = c.leader(indexTp)
      val paused = c.pauseIndex(topic)
      val data = MemoryRecords.withIdempotentRecords(Compression.NONE, 100L, 0.toShort, 0,
        new SimpleRecord(value(11)), new SimpleRecord(value(12)), new SimpleRecord(value(13)))
      try {
        val timedOut = c.produce(topic, 0, data, 700)
        assertEquals(Errors.REQUEST_TIMED_OUT.code(), timedOut.data().responses().iterator().next().partitionResponses().get(0).errorCode())
        c.await("Index append exists above stalled HW") {
          val log = oldLeader.replicaManager.localLog(indexTp).get
          log.logEndOffset() > log.highWatermark()
        }
        assertEquals(Vector(10, 11, 12, 13), c.physical(topic).filter(_.partition == 0).flatMap(_.values))
        assertEquals(2L, c.fetch(topic, c.live.head).committedGlobalEndOffset(), "Uncommitted allocation must not be visible")
        if (failover) {
          c.shutdown(oldLeader)
          c.await("New index leader") { Try(c.leader(indexTp)).toOption.exists(_.config.brokerId != oldLeader.config.brokerId) }
          c.restart(oldLeader)
        } else paused.close()
        c.progress(topic, 0, 3)
        // Re-send the same producer sequence as after an unknown Produce outcome or lost response.
        for (_ <- 0 until 2) {
          val response = c.produce(topic, 0, data, 10000).data().responses().iterator().next().partitionResponses().get(0)
          assertEquals(Errors.NONE.code(), response.errorCode())
          assertEquals(1L, response.baseOffset())
        }
        c.append(topic, 1, Seq(21, 22))
        c.verify(topic, Map(0 -> Seq(10, 11, 12, 13), 1 -> Seq(20, 21, 22)))
      } finally paused.close()
    } finally c.close()
  }

  @Test
  def testCommittedDataRecoversAfterRestartAndSourceRoundTripFencesDelayedAppend(): Unit = {
    val c = new Context
    try {
      c.start()
      val topic = c.create("restart-fault")
      c.append(topic, 0, Seq(10, 11))
      c.append(topic, 1, Seq(20))
      val first = c.leader(topic.tp(0))
      val description = first.indexRoutingManager.describePartition(topic.key(0), 5000).get(6, TimeUnit.SECONDS).value
      first.globalSequenceIndexerManager.indexer(topic.key(0)).get.close()
      val missing = c.append(topic, 0, Seq(12, 13, 14), indexed = false)
      val delayed = new AppendRequest(new PhysicalBatch(topic.key(0), missing.base, missing.last, missing.values.size),
        description.committedProgress().get().baseOffset(), missing.last + 1, description.currentIndexer().get())
      // A quiet partition's progress must survive a much newer tail belonging to another partition.
      (21 to 28).foreach(number => c.append(topic, 1, Seq(number)))
      c.shutdown(first)
      c.await("Source follower promotion") { Try(c.leader(topic.tp(0))).toOption.exists(_.config.brokerId != first.config.brokerId) }
      c.progress(topic, 0, missing.last)
      c.restart(first)
      c.move(topic.tp(0), first)
      c.progress(topic, 0, missing.last)
      c.move(c.indexTp(topic), c.brokers.find(_ != c.leader(c.indexTp(topic))).get)
      assertEquals(Errors.FENCED_LEADER_EPOCH.code(), c.replayOldAppend(delayed))
      c.append(topic, 0, Seq(15))
      val before = c.verify(topic, Map(0 -> (10 to 15), 1 -> (20 to 28)))
      val indexLeader = c.leader(c.indexTp(topic))
      c.shutdown(indexLeader)
      c.restart(indexLeader)
      c.progress(topic, 0, 5)
      assertEquals(before, c.verify(topic, Map(0 -> (10 to 15), 1 -> (20 to 28))))
    } finally c.close()
  }

  @Test
  def testRetainedIndexReportsMissingDataAndRecreatedTopicStartsANewSequence(): Unit = {
    val c = new Context
    try {
      c.start()
      val old = c.create("recreated")
      c.append(old, 0, Seq(10, 11, 12))
      c.append(old, 1, Seq(20))
      val owner = c.live.head.indexRoutingManager.describePartition(old.key(1), 5000).get(6, TimeUnit.SECONDS).value.currentIndexer().get()
      val delayed = new AppendRequest(new PhysicalBatch(old.key(1), 0, 0, 1), -1, 1, owner)
      val indexes = c.verify(old, Map(0 -> Seq(10, 11, 12), 1 -> Seq(20)))
      c.await("Deletion pins cover the indexed data") {
        c.live.forall(_.replicaManager.localLog(old.tp(1)).get.globalSequenceDeletionLimit() == 1)
      }
      c.admin.deleteRecords(util.Map.of(old.tp(1), RecordsToDelete.beforeOffset(1))).all().get(30, TimeUnit.SECONDS)
      c.live.foreach { broker =>
        val first = c.fetch(old, broker)
        assertEquals(3L, first.nextGlobalOffset())
        val unavailable = c.fetch(old, broker, first.nextGlobalOffset())
        assertEquals(Errors.OFFSET_OUT_OF_RANGE.code(), unavailable.errorCode())
        assertEquals(3L, unavailable.nextGlobalOffset())
      }
      val bytes = new ByteArrayOutputStream
      val output = new PrintStream(bytes, true, UTF_8)
      try assertThrows(classOf[org.apache.kafka.common.KafkaException], () =>
        GlobalSequenceReadDemo.read(c.cluster.clientProperties(), old.id, 0, 4, "read_uncommitted", output))
      finally output.close()
      val lines = bytes.toString(UTF_8).linesIterator.toVector
      assertEquals(3, lines.count(_.startsWith("record\t")), "A failed page retains the valid prefix")
      assertTrue(lines.last.contains("next=3"))
      assertTrue(lines.last.contains("error=OFFSET_OUT_OF_RANGE"))
      assertEquals(indexes, c.indexes(old), "Deleting source data must not erase the retained index")
      c.admin.deleteTopics(util.List.of(old.name)).all().get(30, TimeUnit.SECONDS)
      c.await("Old UUID disappeared from metadata") { c.live.forall(_.metadataCache.getImage().topics().getTopic(old.id) == null) }
      val fresh = c.create(old.name)
      assertNotEquals(old.id, fresh.id)
      c.live.foreach { broker =>
        assertEquals(Errors.UNKNOWN_TOPIC_ID.code(), c.fetch(old, broker).errorCode())
        assertEquals(0L, c.fetch(fresh, broker).committedGlobalEndOffset())
        val request = new LookupGlobalSequenceRequest.Builder(new LookupGlobalSequenceRequestData().setTopicId(old.id)
          .setGlobalEndOffsetExclusive(Long.MaxValue)).build()
        assertEquals(Errors.UNKNOWN_TOPIC_ID.code(), IntegrationTestUtils.connectAndReceive[LookupGlobalSequenceResponse](request,
          broker.socketServer, broker.config.interBrokerListenerName).data().errorCode())
      }
      assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), c.replayOldAppend(delayed))
      c.append(fresh, 0, Seq(100, 101))
      c.append(fresh, 1, Seq(200))
      c.verify(fresh, Map(0 -> Seq(100, 101), 1 -> Seq(200)))
    } finally c.close()
  }
}
