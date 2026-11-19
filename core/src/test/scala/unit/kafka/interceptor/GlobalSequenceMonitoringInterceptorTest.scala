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
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.{FetchGlobalSequenceRequest, ProduceRequest, RequestHeader}
import org.apache.kafka.common.utils.{LogContext, MockTime}
import org.apache.kafka.coordinator.globalsequence.{GlobalSequenceCoordinator, GlobalSequenceCoordinatorConfig, GlobalSequenceLookup, GlobalSequenceResources}
import org.apache.kafka.image.{MetadataImage, TopicImage, TopicsImage}
import org.apache.kafka.metadata.{LeaderRecoveryState, PartitionRegistration}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

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
        val request = mock(classOf[RequestChannel.Request])
        when(request.header).thenReturn(new RequestHeader(ApiKeys.PRODUCE, version, "producer", 1))
        when(request.body[ProduceRequest]).thenReturn(new ProduceRequest(data, version))
        monitor.beforeHandleRequest(request)
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
      verify(measured).globalFetchTiming_=(any(classOf[RequestChannel.GlobalFetchTiming]))
      val timing = new RequestChannel.GlobalFetchTiming
      timing.lookupStartNs = 3000000L
      timing.lookupCompleteNs = 5000000L
      timing.lastRangeCallbackNs = 8000000L
      timing.pageReadyNs = 9000000L
      when(measured.globalFetchTiming).thenReturn(timing)
      when(measured.startTimeNanos).thenReturn(1000000L)
      when(measured.requestDequeueTimeNanos).thenReturn(2000000L)
      when(measured.responseCompleteTimeNanos).thenReturn(11000000L)
      when(measured.responseDequeueTimeNanos).thenReturn(12000000L)
      val sent = new RequestChannel.SendResponse(measured, mock(classOf[Send]), None, None)
      monitor.afterProcessResponse(sent, "connection")
      timing.sendCompleteNs = 14000000L
      monitor.afterProcessResponse(sent, "connection")

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
      "duration-ms-total,last-range-callback-to-page-ready,1.0"
    ).map("2000,1000,2,,,global-sequence-fetch-timing," + _), timings.toSeq)
    assertTrue(csv.contains("2000,1000,2,,,global-sequence-resources,physical-offsets,indexing_lag,23"))
    assertTrue(csv.contains("2000,1000,2,,,global-sequence-resources,reads-total,source_read,2.0"))
    assertTrue(csv.contains("2000,1000,2,,,global-sequence-resources,batches-total,source_read,3.0"))
    assertTrue(csv.contains("2000,1000,2,,,global-sequence-resources,duration-ms-total,source_read,2.75"))
    assertTrue(csv.contains("3000,1000,2,ordered,,global-sequence-monitor,produce-received-records,,0"))
    assertTrue(csv.contains(s"3000,0,2,ordered,$topicId,global-sequence-monitor,committed-global-end-offset,0,42"))
  }
}
