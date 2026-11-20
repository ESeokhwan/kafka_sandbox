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

package kafka.interceptor

import kafka.network.RequestChannel
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.internals.Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME
import org.apache.kafka.common.message.{FetchGlobalSequenceRequestData, ProduceRequestData}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ClientInformation, ListenerName, Send}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.{FetchGlobalSequenceRequest, FetchRequest, ProduceRequest, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.{LogContext, MockTime}
import org.apache.kafka.coordinator.globalsequence.{GlobalSequenceCoordinator, GlobalSequenceCoordinatorConfig, GlobalSequenceLookup, GlobalSequenceResources}
import org.apache.kafka.image.{MetadataImage, TopicImage, TopicsImage}
import org.apache.kafka.metadata.{LeaderRecoveryState, PartitionRegistration}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import java.net.InetAddress
import java.nio.file.{Files, Path}
import java.util
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

class GlobalSequenceMonitoringInterceptorTest {
  @TempDir
  var outputDir: Path = _

  @Test
  def testSamplesProduceFetchAndCommittedOffsetsToCsv(): Unit = {
    val brokerId = 2
    val topic = "ordered"
    val topicId = Uuid.randomUuid()
    val indexId = Uuid.randomUuid()
    val leaderEpoch = 7
    val indexPartition = new PartitionRegistration.Builder()
      .setLeader(brokerId).setLeaderEpoch(leaderEpoch).setPartitionEpoch(0)
      .setReplicas(Array(brokerId)).setIsr(Array(brokerId)).setDirectories(Array(Uuid.randomUuid()))
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).build()
    val metadata = mock(classOf[MetadataImage])
    when(metadata.topics()).thenReturn(TopicsImage.EMPTY
      .including(new TopicImage(topic, topicId, util.Map.of()))
      .including(new TopicImage(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME, indexId, util.Map.of(Int.box(0), indexPartition))))
    val config = new GlobalSequenceCoordinatorConfig(new AbstractConfig(GlobalSequenceCoordinatorConfig.CONFIG_DEF, util.Map.of(
      "global.sequence.monitor.topic", topic,
      "global.sequence.monitor.produce.topic.prefix", "ord",
      "global.sequence.monitor.interval.ms", "60000",
      "global.sequence.monitor.output.dir", outputDir.toString
    ), false))
    val time = new MockTime(0, 1000, 0)
    val metrics = new Metrics(time)
    val resources = new GlobalSequenceResources(config, metrics, time)
    resources.progress(GlobalSequenceResources.Progress.INDEXING_LAG, 23)
    val coordinator = mock(classOf[GlobalSequenceCoordinator])
    val snapshot = new CompletableFuture[GlobalSequenceLookup.Snapshot]
    when(coordinator.committedSnapshot(topicId, leaderEpoch)).thenReturn(snapshot, new CompletableFuture[GlobalSequenceLookup.Snapshot])
    val monitor = new GlobalSequenceMonitoringInterceptor(config, brokerId, () => metadata, metrics, time, new LogContext())
    monitor.start(coordinator)
    try {
      val batch = MemoryRecords.withRecords(Compression.gzip().build(),
        new SimpleRecord(Array[Byte](1)), new SimpleRecord(Array[Byte](2)))
      def produce(version: Short, entries: (String, Uuid)*): Unit = {
        val data = new ProduceRequestData().setAcks(0.toShort)
        entries.foreach { case (name, id) =>
          val partition = new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(batch)
          data.topicData().add(new ProduceRequestData.TopicProduceData()
            .setName(name).setTopicId(id).setPartitionData(util.List.of(partition)))
        }
        val buffer = new ProduceRequest(data, version).serializeWithHeader(new RequestHeader(ApiKeys.PRODUCE, version, "producer", 1))
        val context = new RequestContext(RequestHeader.parse(buffer), "test", InetAddress.getLoopbackAddress,
          KafkaPrincipal.ANONYMOUS, new ListenerName("PLAINTEXT"), SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false)
        val request = new RequestChannel.Request(0, context, time.nanoseconds(), MemoryPool.NONE, buffer, null)
        monitor.beforeSendRequestToQueue(request, "test")
        monitor.beforeHandleRequest(request)
        monitor.beforeSendResponseToQueue(new RequestChannel.StartThrottlingResponse(request))
        val terminal = if (version == 12) new RequestChannel.NoOpResponse(request)
        else new RequestChannel.CloseConnectionResponse(request)
        monitor.beforeSendResponseToQueue(terminal)
        monitor.beforeSendResponseToQueue(terminal)
        monitor.beforeSendResponseToQueue(new RequestChannel.EndThrottlingResponse(request))
      }
      produce(12, (topic, Uuid.ZERO_UUID), ("other", Uuid.ZERO_UUID))
      produce(13, ("", topicId), ("", Uuid.randomUuid()))

      val fetch = mock(classOf[RequestChannel.Request])
      when(fetch.header).thenReturn(new RequestHeader(ApiKeys.FETCH_GLOBAL_SEQUENCE, 0.toShort, "consumer", 2))
      when(fetch.body[FetchGlobalSequenceRequest]).thenReturn(new FetchGlobalSequenceRequest.Builder(
        new FetchGlobalSequenceRequestData().setTopicId(topicId)).build())
      when(fetch.startTimeNanos).thenReturn(time.nanoseconds())
      monitor.beforeHandleRequest(fetch)
      time.sleep(3)
      monitor.beforeSendResponseToQueue(new RequestChannel.StartThrottlingResponse(fetch))
      monitor.beforeSendResponseToQueue(new RequestChannel.SendResponse(fetch, mock(classOf[Send]), None, None,
        errorCounts = Map(Errors.REQUEST_TIMED_OUT -> 1)))

      val measured = mock(classOf[RequestChannel.Request])
      when(measured.header).thenReturn(new RequestHeader(ApiKeys.FETCH_GLOBAL_SEQUENCE, 0.toShort, "consumer", 3))
      val otherTopicId = Uuid.randomUuid()
      when(measured.body[FetchGlobalSequenceRequest]).thenReturn(new FetchGlobalSequenceRequest.Builder(
        new FetchGlobalSequenceRequestData().setTopicId(otherTopicId)).build())
      monitor.beforeHandleRequest(measured)
      verify(measured).fetchTiming_=(any(classOf[RequestChannel.FetchTiming]))
      val timing = new RequestChannel.FetchTiming
      timing.lookupStartNs = 3000000L
      timing.lookupCompleteNs = 5000000L
      timing.lastRangeCallbackNs = 8000000L
      timing.pageReadyNs = 9000000L
      when(measured.fetchTiming).thenReturn(timing)
      when(measured.startTimeNanos).thenReturn(1000000L)
      when(measured.requestDequeueTimeNanos).thenReturn(2000000L)
      when(measured.responseCompleteTimeNanos).thenReturn(11000000L)
      when(measured.responseDequeueTimeNanos).thenReturn(12000000L)
      val sent = new RequestChannel.SendResponse(measured, mock(classOf[Send]), None, None)
      monitor.afterProcessResponse(sent, "connection")
      timing.sendCompleteNs = 14000000L
      timing.sendBuffers = 17
      timing.sendBytes = 1024L
      timing.writeCalls = 3
      timing.zeroByteWrites = 1
      timing.firstWriteNs = 12300000L
      timing.lastWriteEndNs = 13900000L
      timing.writeElapsedNs = 500000L
      monitor.afterProcessResponse(sent, "connection")

      val consumer = mock(classOf[RequestChannel.Request])
      when(consumer.header).thenReturn(new RequestHeader(ApiKeys.FETCH, 12.toShort, "consumer", 4))
      val body = mock(classOf[FetchRequest])
      when(consumer.body[FetchRequest]).thenReturn(body)
      monitor.beforeHandleRequest(consumer)
      verify(consumer).fetchTiming_=(any(classOf[RequestChannel.FetchTiming]))
      when(body.isFromFollower).thenReturn(true)
      monitor.beforeHandleRequest(consumer)
      verify(consumer, times(1)).fetchTiming_=(any(classOf[RequestChannel.FetchTiming]))
      val consumerTiming = new RequestChannel.FetchTiming
      consumerTiming.sendBytes = 2048L
      consumerTiming.writeCalls = 2
      consumerTiming.firstWriteNs = 12300000L
      consumerTiming.lastWriteEndNs = 13900000L
      consumerTiming.writeElapsedNs = 500000L
      consumerTiming.sendCompleteNs = 14000000L
      when(consumer.fetchTiming).thenReturn(consumerTiming)
      when(consumer.startTimeNanos).thenReturn(1000000L)
      when(consumer.requestDequeueTimeNanos).thenReturn(2000000L)
      when(consumer.responseCompleteTimeNanos).thenReturn(11000000L)
      when(consumer.responseDequeueTimeNanos).thenReturn(12000000L)
      monitor.afterProcessResponse(new RequestChannel.SendResponse(consumer, mock(classOf[Send]), None, None), "connection")

      resources.recordSourceRead(3, 2.5)
      resources.recordSourceRead(0, 0.25)
      time.sleep(997)
      monitor.sample(Some(coordinator))
      time.sleep(1000)
      monitor.sample(Some(coordinator))
      verify(coordinator, times(1)).committedSnapshot(topicId, leaderEpoch)

      snapshot.complete(new GlobalSequenceLookup.Snapshot(indexId, 0, leaderEpoch, 110, 42))
      time.sleep(1000)
      monitor.sample(Some(coordinator))
      verify(coordinator, times(2)).committedSnapshot(topicId, leaderEpoch)
    } finally {
      monitor.shutdown()
      resources.close()
      metrics.close()
    }

    val csv = Files.readAllLines(outputDir.resolve("global-sequence-monitor-2.csv")).asScala
    assertEquals("timestamp_ms,interval_ms,broker_id,topic,topic_id,metric_group,metric_name,scope,value", csv.head)
    assertTrue(csv.contains("2000,1000,2,ordered,,global-sequence-monitor,produce-received-records,,4"))
    Seq("prequeue", "handler-started", "terminal").foreach { stage =>
      assertTrue(csv.contains(s"2000,1000,2,ord,,global-sequence-produce-pipeline,$stage-records-total,,4"))
      assertTrue(csv.contains(s"3000,1000,2,ord,,global-sequence-produce-pipeline,$stage-records-total,,4"))
    }
    assertTrue(csv.contains("2000,1000,2,ordered,,global-sequence-monitor,fetch-received-requests,,1"))
    assertTrue(csv.contains("2000,1000,2,ordered,,global-sequence-monitor,fetch-completed-requests,,1"))
    assertTrue(csv.contains("2000,1000,2,ordered,,global-sequence-monitor,fetch-failed-requests,,1"))
    assertTrue(csv.contains("2000,1000,2,ordered,,global-sequence-monitor,fetch-to-response-queue-ms-avg,,3.0"))
    val timings = csv.filter(_.contains(",global-sequence-fetch-timing,"))
    assertEquals(Seq(
      "samples,page,1",
      "samples,range-page,1",
      "duration-ms-total,request-queue,1.0",
      "duration-ms-total,handler-to-lookup,1.0",
      "duration-ms-total,index-lookup,2.0",
      "duration-ms-total,lookup-to-page-ready,4.0",
      "duration-ms-total,page-ready-to-response-queue,2.0",
      "duration-ms-total,response-queue,1.0",
      "duration-ms-total,response-send,2.0",
      "duration-ms-total,lookup-to-last-range-callback,3.0",
      "duration-ms-total,last-range-callback-to-page-ready,1.0",
      "samples,socket-write-page,1",
      "samples,response-send-buffer-page,1",
      "count-total,response-send-buffers,17",
      "count-total,socket-write-calls,3",
      "count-total,zero-byte-socket-write-calls,1",
      "bytes-total,response-send,1024",
      "duration-ms-total,response-dequeue-to-first-write,0.3",
      "duration-ms-total,socket-write,0.5",
      "duration-ms-total,between-socket-writes,1.1",
      "duration-ms-total,last-write-to-send-complete,0.1"
    ).map("2000,1000,2,,,global-sequence-fetch-timing," + _), timings.toSeq)
    val consumerTimings = csv.filter(_.contains(",consumer-fetch-timing,"))
    assertEquals(Seq(
      "samples,socket-write-page,1",
      "count-total,socket-write-calls,2",
      "count-total,zero-byte-socket-write-calls,0",
      "bytes-total,response-send,2048",
      "duration-ms-total,response-dequeue-to-first-write,0.3",
      "duration-ms-total,socket-write,0.5",
      "duration-ms-total,between-socket-writes,1.1",
      "duration-ms-total,last-write-to-send-complete,0.1",
      "duration-ms-total,request-queue,1.0",
      "duration-ms-total,handler-to-response-queue,9.0",
      "duration-ms-total,response-queue,1.0",
      "duration-ms-total,response-send,2.0"
    ).map("2000,1000,2,,,consumer-fetch-timing," + _), consumerTimings.toSeq)
    assertTrue(csv.contains("2000,1000,2,,,global-sequence-resources,physical-offsets,indexing_lag,23"))
    assertTrue(csv.contains("2000,1000,2,,,global-sequence-resources,reads-total,source_read,2.0"))
    assertTrue(csv.contains("2000,1000,2,,,global-sequence-resources,batches-total,source_read,3.0"))
    assertTrue(csv.contains("2000,1000,2,,,global-sequence-resources,duration-ms-total,source_read,2.75"))
    assertTrue(csv.contains("3000,1000,2,ordered,,global-sequence-monitor,produce-received-records,,0"))
    assertTrue(csv.contains(s"3000,0,2,ordered,$topicId,global-sequence-monitor,committed-global-end-offset,0,42"))
  }
}
