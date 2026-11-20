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
import moniq.{IMonitorLog, MonitorQueue}
import moniq.writer.strategy.FileMonitorLogWriteStrategy
import moniq.writer.strategy.FileMonitorLogWriteStrategy.Format
import moniq.writer.{BatchPolicy, FlushPolicy, MonitorLogWriter}
import org.apache.kafka.common.{MetricName, Uuid}
import org.apache.kafka.common.internals.Topic.GLOBAL_SEQUENCE_INDEX_TOPIC_NAME
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests.{FetchGlobalSequenceRequest, FetchRequest, ProduceRequest}
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time}
import org.apache.kafka.coordinator.globalsequence.{GlobalSequenceCoordinator, GlobalSequenceCoordinatorConfig, GlobalSequenceCoordinatorService, GlobalSequenceLookup, GlobalSequenceResources}
import org.apache.kafka.image.MetadataImage

import java.nio.file.Path
import java.time.Duration
import java.util
import java.util.concurrent.{CompletableFuture, Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, LongAdder}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Samples GST metrics for a benchmark without adding work to the indexer. */
final class GlobalSequenceMonitoringInterceptor(
  config: GlobalSequenceCoordinatorConfig,
  brokerId: Int,
  metadata: () => MetadataImage,
  metrics: Metrics,
  time: Time,
  logContext: LogContext
) extends BrokerInterceptor {
  import GlobalSequenceMonitoringInterceptor._

  private val topic = config.monitorTopic()
  private val produceTopicPrefix = config.monitorProduceTopicPrefix()
  private val producePrequeue = new LongAdder
  private val produceHandlerStarted = new LongAdder
  private val produceTerminal = new LongAdder
  private val produceUnknownCountBatches = new LongAdder
  private val intervalMs = config.monitorIntervalMs()
  private val logger = logContext.logger(classOf[GlobalSequenceMonitoringInterceptor])
  private val receivedRecords = new AtomicLong
  private val receivedFetches = new AtomicLong
  private val fetchStatsLock = new Object
  private var completedFetches = 0L
  private var failedFetches = 0L
  private var fetchLatencyNs = 0L
  private var maxFetchLatencyNs = 0L
  private val fetchTimingLock = new Object
  private val fetchTimingTotalsNs = new Array[Long](FetchTimingScopes.length)
  private var fetchTimingSamples = 0L
  private var rangeTimingSamples = 0L
  private val gstWrites = new WriteTotals
  private val consumerWrites = new WriteTotals

  private val queue = new MonitorQueue
  private var writer: MonitorLogWriter = _
  private var writerThread: Thread = _
  private var scheduler: ScheduledExecutorService = _
  private var lastSampleClockMs = 0L
  private var pendingSnapshot: Option[CompletableFuture[CommittedIndexSample]] = None

  // BrokerServer calls this once the local coordinator has been created.
  def start(coordinator: GlobalSequenceCoordinator): Unit = {
    val output = Path.of(config.monitorOutputDir(), s"global-sequence-monitor-$brokerId.csv")
    val strategy = new FileMonitorLogWriteStrategy(output, Format.COMMA_SEPARATED)
    writer = new MonitorLogWriter(queue, strategy, BatchPolicy.unbounded(), FlushPolicy.after(Duration.ofMillis(intervalMs)))
    writerThread = KafkaThread.daemon(s"global-sequence-monitor-writer-$brokerId", () => {
      try writer.run()
      finally strategy.close()
    })
    writerThread.start()

    lastSampleClockMs = time.hiResClockMs()
    scheduler = Executors.newSingleThreadScheduledExecutor((task: Runnable) =>
      KafkaThread.daemon(s"global-sequence-monitor-sampler-$brokerId", task))
    scheduler.scheduleWithFixedDelay(() => runSample(Some(coordinator)), intervalMs, intervalMs, TimeUnit.MILLISECONDS)
    logger.info(s"Monitoring global sequence topic $topic every $intervalMs ms at $output " +
      s"(append linger ${config.appendLingerMs()} ms)")
  }

  override def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = {
    if (produceTopicPrefix.isEmpty || request.header.apiKey() != ApiKeys.PRODUCE) return

    var pipelineCount = 0L
    var legacyCount = 0L
    var unknownCount = 0L
    try {
      val topics = metadata().topics()
      request.body[ProduceRequest].data().topicData().forEach { entry =>
        val name = if (entry.topicId() == Uuid.ZERO_UUID) entry.name()
        else Option(topics.getTopic(entry.topicId())).map(_.name()).orNull
        if (name != null) {
          val pipelineMatch = name.startsWith(produceTopicPrefix)
          val legacyMatch = name == topic
          if (pipelineMatch || legacyMatch) entry.partitionData().forEach { partition =>
            partition.records() match {
              case records: Records => records.batches().forEach { batch =>
                val count = batch.countOrNull()
                if (count == null) {
                  if (pipelineMatch) unknownCount += 1
                } else if (count > 0) {
                  if (pipelineMatch) pipelineCount += count.longValue()
                  if (legacyMatch) legacyCount += count.longValue()
                }
              }
              case _ =>
            }
          }
        }
      }
      request.monitoredProduceRecords = pipelineCount
      request.monitoredProduceReceivedRecords = legacyCount
      if (pipelineCount > 0) producePrequeue.add(pipelineCount)
      if (unknownCount > 0) produceUnknownCountBatches.add(unknownCount)
    } catch {
      case NonFatal(error) => logger.debug("Could not read Produce batch headers for monitoring", error)
    }
  }

  override def beforeHandleRequest(request: RequestChannel.Request): Unit = request.header.apiKey() match {
    case ApiKeys.PRODUCE if produceTopicPrefix.nonEmpty =>
      if (request.monitoredProduceRecords > 0) produceHandlerStarted.add(request.monitoredProduceRecords)
      if (request.monitoredProduceReceivedRecords > 0) receivedRecords.addAndGet(request.monitoredProduceReceivedRecords)
    case ApiKeys.PRODUCE if topic.nonEmpty =>
      try {
        val data = request.body[ProduceRequest].data()
        val monitored = metadata().topics().getTopic(topic)
        val matching = data.topicData().asScala.iterator.filter { entry =>
          if (entry.topicId() == Uuid.ZERO_UUID) entry.name() == topic
          else monitored != null && entry.topicId() == monitored.id()
        }
        val count = matching.flatMap(_.partitionData().asScala).map(_.records()).collect {
          case records: Records => records.batches().asScala.iterator.map(_.countOrNull())
            .filter(count => count != null && count > 0).map(_.longValue()).sum
        }.sum
        receivedRecords.addAndGet(count)
      } catch {
        // The request handler owns validation and error handling for malformed batches.
        case NonFatal(error) => logger.debug("Could not read Produce batch headers for monitoring", error)
      }
    case ApiKeys.FETCH_GLOBAL_SEQUENCE =>
      // Keep this broker-wide so dynamically named experiment topics are included.
      request.fetchTiming = new RequestChannel.FetchTiming
      if (monitorsFetch(request)) receivedFetches.incrementAndGet()
    case ApiKeys.FETCH if !request.body[FetchRequest].isFromFollower =>
      request.fetchTiming = new RequestChannel.FetchTiming
    case _ =>
  }

  private def monitorsFetch(request: RequestChannel.Request): Boolean = {
    val monitored = metadata().topics().getTopic(topic)
    monitored != null && monitored.id() == request.body[FetchGlobalSequenceRequest].data().topicId()
  }

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = response match {
    case _: RequestChannel.NoOpResponse | _: RequestChannel.CloseConnectionResponse | _: RequestChannel.SendResponse
      if produceTopicPrefix.nonEmpty && response.request.monitoredProduceRecords > 0 =>
      val request = response.request
      request.synchronized {
        if (!request.monitoredProduceTerminal) {
          request.monitoredProduceTerminal = true
          produceTerminal.add(request.monitoredProduceRecords)
        }
      }
    case sent: RequestChannel.SendResponse
      if sent.request.header.apiKey() == ApiKeys.FETCH_GLOBAL_SEQUENCE && monitorsFetch(sent.request) =>
      val elapsedNs = math.max(0L, time.nanoseconds() - sent.request.startTimeNanos)
      val failed = sent.errorCounts.exists { case (error, count) => error != Errors.NONE && count > 0 }
      fetchStatsLock.synchronized {
        completedFetches += 1
        if (failed) failedFetches += 1
        fetchLatencyNs += elapsedNs
        maxFetchLatencyNs = math.max(maxFetchLatencyNs, elapsedNs)
      }
    case _ =>
  }

  override def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {
    val request = response.request
    val timing = request.fetchTiming
    if (timing != null && timing.sendCompleteNs != 0L) {
      val dequeued = request.requestDequeueTimeNanos
      val queued = request.responseCompleteTimeNanos
      val sending = request.responseDequeueTimeNanos
      fetchTimingLock.synchronized {
        if (request.header.apiKey() == ApiKeys.FETCH_GLOBAL_SEQUENCE) {
          fetchTimingSamples += 1
          fetchTimingTotalsNs(0) += dequeued - request.startTimeNanos
          fetchTimingTotalsNs(1) += timing.lookupStartNs - dequeued
          fetchTimingTotalsNs(2) += timing.lookupCompleteNs - timing.lookupStartNs
          fetchTimingTotalsNs(3) += timing.pageReadyNs - timing.lookupCompleteNs
          fetchTimingTotalsNs(4) += queued - timing.pageReadyNs
          fetchTimingTotalsNs(5) += sending - queued
          fetchTimingTotalsNs(6) += timing.sendCompleteNs - sending
          if (timing.lastRangeCallbackNs != 0L) {
            rangeTimingSamples += 1
            fetchTimingTotalsNs(7) += timing.lastRangeCallbackNs - timing.lookupCompleteNs
            fetchTimingTotalsNs(8) += timing.pageReadyNs - timing.lastRangeCallbackNs
          }
          if (timing.writeCalls > 0) gstWrites.record(request, timing)
        } else if (timing.writeCalls > 0) {
          consumerWrites.record(request, timing)
        }
      }
    }
  }

  private def runSample(coordinator: Option[GlobalSequenceCoordinator]): Unit = {
    try sample(coordinator)
    catch {
      case NonFatal(error) => logger.warn("Could not sample global sequence metrics", error)
    }
  }

  private[interceptor] def sample(coordinator: Option[GlobalSequenceCoordinator]): Unit = {
    val nowClockMs = time.hiResClockMs()
    val durationMs = nowClockMs - lastSampleClockMs
    // Leave request counts in place when the writer falls behind so the next interval includes them.
    if (durationMs <= 0 || queue.size() >= 4096 || !writerThread.isAlive) return
    lastSampleClockMs = nowClockMs
    val nowMs = time.milliseconds()

    def emit(name: String, value: AnyVal): Unit =
      writer.submit(Sample(nowMs, durationMs, brokerId, topic, "", "global-sequence-monitor", name, "", value.toString))

    if (produceTopicPrefix.nonEmpty) {
      def emitProduce(name: String, counter: LongAdder): Unit =
        writer.submit(Sample(nowMs, durationMs, brokerId, produceTopicPrefix, "", "global-sequence-produce-pipeline",
          name, "", counter.sum().toString))

      emitProduce("prequeue-records-total", producePrequeue)
      emitProduce("handler-started-records-total", produceHandlerStarted)
      emitProduce("terminal-records-total", produceTerminal)
      emitProduce("unknown-record-count-batches-total", produceUnknownCountBatches)
    }

    emit("produce-received-records", receivedRecords.getAndSet(0))
    emit("fetch-received-requests", receivedFetches.getAndSet(0))
    val (completed, failed, latencyNs, maxLatencyNs) = fetchStatsLock.synchronized {
      val window = (completedFetches, failedFetches, fetchLatencyNs, maxFetchLatencyNs)
      completedFetches = 0
      failedFetches = 0
      fetchLatencyNs = 0
      maxFetchLatencyNs = 0
      window
    }
    emit("fetch-completed-requests", completed)
    emit("fetch-failed-requests", failed)
    if (completed > 0) {
      emit("fetch-to-response-queue-ms-avg", latencyNs.toDouble / completed / 1000000)
      emit("fetch-to-response-queue-ms-max", maxLatencyNs.toDouble / 1000000)
    }
    emit("append-linger-ms", config.appendLingerMs())

    val (pageSamples, rangeSamples, stageTotals, gst, consumer) = fetchTimingLock.synchronized {
      val window = (fetchTimingSamples, rangeTimingSamples, fetchTimingTotalsNs.clone(),
        gstWrites.snapshotAndReset(), consumerWrites.snapshotAndReset())
      fetchTimingSamples = 0L
      rangeTimingSamples = 0L
      java.util.Arrays.fill(fetchTimingTotalsNs, 0L)
      window
    }
    def emitTiming(group: String, name: String, scope: String, value: AnyVal): Unit =
      writer.submit(Sample(nowMs, durationMs, brokerId, "", "", group, name, scope, value.toString))

    if (pageSamples > 0) {
      emitTiming(GstTimingGroup, "samples", "page", pageSamples)
      emitTiming(GstTimingGroup, "samples", "range-page", rangeSamples)
      FetchTimingScopes.indices.foreach { stage =>
        emitTiming(GstTimingGroup, "duration-ms-total", FetchTimingScopes(stage), stageTotals(stage) / 1000000.0)
      }
    }
    def emitWrites(group: String, writes: WriteSample, includeQueues: Boolean): Unit = {
      if (writes.pages > 0) {
        emitTiming(group, "samples", "socket-write-page", writes.pages)
        if (writes.bufferPages > 0) {
          emitTiming(group, "samples", "response-send-buffer-page", writes.bufferPages)
          emitTiming(group, "count-total", "response-send-buffers", writes.buffers)
        }
        emitTiming(group, "count-total", "socket-write-calls", writes.calls)
        emitTiming(group, "count-total", "zero-byte-socket-write-calls", writes.zeroByteCalls)
        emitTiming(group, "bytes-total", "response-send", writes.bytes)
        val scopes = if (includeQueues) WriteTimingScopes ++ ConsumerTimingScopes else WriteTimingScopes
        scopes.indices.foreach { stage =>
          emitTiming(group, "duration-ms-total", scopes(stage), writes.timesNs(stage) / 1000000.0)
        }
      }
    }
    emitWrites(GstTimingGroup, gst, includeQueues = false)
    emitWrites("consumer-fetch-timing", consumer, includeQueues = true)

    metrics.metrics().asScala.iterator.filter { case (name, _) => includesMetric(name) }.foreach { case (name, metric) =>
      writer.submit(Sample(nowMs, durationMs, brokerId, "", "", name.group(), name.name(),
        name.tags().getOrDefault("scope", ""), metric.metricValue().toString))
    }

    // Reap the previous read on the sampler thread. A slow coordinator can leave at most one read queued.
    pendingSnapshot.filter(_.isDone).foreach { future =>
      pendingSnapshot = None
      if (!future.isCompletedExceptionally) {
        val observation = future.join()
        val snapshot = observation.snapshot
        def emitSnapshot(name: String, value: Long): Unit =
          writer.submit(Sample(observation.timestampMs, 0, brokerId, topic, observation.topicId.toString,
            "global-sequence-monitor", name, snapshot.indexPartition().toString, value.toString))
        emitSnapshot("committed-global-end-offset", snapshot.committedGlobalEnd())
        emitSnapshot("index-high-watermark", snapshot.indexHighWatermark())
        emitSnapshot("index-leader-epoch", snapshot.leaderEpoch())
      }
    }

    if (pendingSnapshot.isEmpty) coordinator.foreach { service =>
      val topics = metadata().topics()
      val monitored = topics.getTopic(topic)
      val index = topics.getTopic(GLOBAL_SEQUENCE_INDEX_TOPIC_NAME)
      if (monitored != null && index != null) {
        val leader = index.partitions().get(service.partitionFor(monitored.id()))
        if (leader != null && leader.leader == brokerId) {
          pendingSnapshot = Some(service.committedSnapshot(monitored.id(), leader.leaderEpoch)
            .thenApply(snapshot => CommittedIndexSample(time.milliseconds(), monitored.id(), snapshot)))
        }
      }
    }
  }

  override def shutdown(): Unit = {
    if (writer == null) return
    try {
      try {
        if (scheduler != null) {
          scheduler.shutdown()
          if (scheduler.awaitTermination(5L, TimeUnit.SECONDS)) runSample(None)
          else scheduler.shutdownNow()
        }
      } finally {
        writer.gracefulShutdown()
        if (writerThread != null) {
          writerThread.join(5000L)
          if (writerThread.isAlive) logger.warn("Timed out waiting for global sequence monitor CSV writes")
        }
      }
    } catch {
      case _: InterruptedException => Thread.currentThread().interrupt()
    }
  }
}

private object GlobalSequenceMonitoringInterceptor {
  private val FetchTimingScopes = Vector("request-queue", "handler-to-lookup", "index-lookup",
    "lookup-to-page-ready", "page-ready-to-response-queue", "response-queue", "response-send",
    "lookup-to-last-range-callback", "last-range-callback-to-page-ready")
  private val GstTimingGroup = "global-sequence-fetch-timing"
  private val WriteTimingScopes = Vector("response-dequeue-to-first-write", "socket-write",
    "between-socket-writes", "last-write-to-send-complete")

  private val ConsumerTimingScopes = Vector("request-queue", "handler-to-response-queue", "response-queue", "response-send")

  private final case class WriteSample(pages: Long, bufferPages: Long, buffers: Long,
                                      calls: Long, zeroByteCalls: Long, bytes: Long, timesNs: Array[Long])

  private final class WriteTotals {
    private var pages = 0L
    private var bufferPages = 0L
    private var buffers = 0L
    private var calls = 0L
    private var zeroByteCalls = 0L
    private var bytes = 0L
    private val timesNs = new Array[Long](WriteTimingScopes.length + ConsumerTimingScopes.length)

    def record(request: RequestChannel.Request, timing: RequestChannel.FetchTiming): Unit = {
      pages += 1
      if (timing.sendBuffers >= 0) {
        bufferPages += 1
        buffers += timing.sendBuffers
      }
      calls += timing.writeCalls
      zeroByteCalls += timing.zeroByteWrites
      bytes += timing.sendBytes
      val dequeue = request.responseDequeueTimeNanos
      timesNs(0) += timing.firstWriteNs - dequeue
      timesNs(1) += timing.writeElapsedNs
      timesNs(2) += timing.lastWriteEndNs - timing.firstWriteNs - timing.writeElapsedNs
      timesNs(3) += timing.sendCompleteNs - timing.lastWriteEndNs
      timesNs(4) += request.requestDequeueTimeNanos - request.startTimeNanos
      timesNs(5) += request.responseCompleteTimeNanos - request.requestDequeueTimeNanos
      timesNs(6) += dequeue - request.responseCompleteTimeNanos
      timesNs(7) += timing.sendCompleteNs - dequeue
    }

    def snapshotAndReset(): WriteSample = {
      val sample = WriteSample(pages, bufferPages, buffers, calls, zeroByteCalls, bytes, timesNs.clone())
      pages = 0L
      bufferPages = 0L
      buffers = 0L
      calls = 0L
      zeroByteCalls = 0L
      bytes = 0L
      java.util.Arrays.fill(timesNs, 0L)
      sample
    }
  }

  private val ResourceScopes = Set("indexing_lag", "coordinator_write", "index_route", "index_rpc",
    "index_read", "lookup_route", "fetch", "data_read", "rpc_retry", "source_read")

  def includesMetric(name: MetricName): Boolean = name.group() match {
    case GlobalSequenceResources.GROUP => ResourceScopes.contains(name.tags().get("scope"))
    case GlobalSequenceCoordinatorService.METRICS_GROUP =>
      name.name().startsWith("event-") || name.name().startsWith("batch-flush-time-ms-") || name.name() == "thread-idle-ratio-avg"
    case "global-sequence-index-cache" => true
    case _ => false
  }

  final case class CommittedIndexSample(timestampMs: Long, topicId: Uuid, snapshot: GlobalSequenceLookup.Snapshot)

  private val Headers = util.List.of("timestamp_ms", "interval_ms", "broker_id", "topic", "topic_id",
    "metric_group", "metric_name", "scope", "value")

  final case class Sample(timestampMs: Long, intervalMs: Long, brokerId: Int, topic: String, topicId: String,
                          group: String, name: String, scope: String, value: String) extends IMonitorLog {
    override def getHeaders: util.List[String] = Headers

    override def getValues: util.List[String] =
      util.List.of(timestampMs.toString, intervalMs.toString, brokerId.toString, topic, topicId, group, name, scope, value)
  }
}
