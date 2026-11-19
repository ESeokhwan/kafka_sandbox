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
import org.apache.kafka.common.requests.{FetchGlobalSequenceRequest, ProduceRequest}
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time}
import org.apache.kafka.coordinator.globalsequence.{GlobalSequenceCoordinator, GlobalSequenceCoordinatorConfig, GlobalSequenceCoordinatorService, GlobalSequenceLookup, GlobalSequenceResources}
import org.apache.kafka.image.MetadataImage

import java.nio.file.Path
import java.time.Duration
import java.util
import java.util.concurrent.{CompletableFuture, Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
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
  private var writeTimingSamples = 0L
  private val writeCountTotals = new Array[Long](WriteCountScopes.length)
  private val writeTimingTotalsNs = new Array[Long](WriteTimingScopes.length)

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

  override def beforeHandleRequest(request: RequestChannel.Request): Unit = request.header.apiKey() match {
    case ApiKeys.PRODUCE =>
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
      request.globalFetchTiming = new RequestChannel.GlobalFetchTiming
      if (monitorsFetch(request)) receivedFetches.incrementAndGet()
    case _ =>
  }

  private def monitorsFetch(request: RequestChannel.Request): Boolean = {
    val monitored = metadata().topics().getTopic(topic)
    monitored != null && monitored.id() == request.body[FetchGlobalSequenceRequest].data().topicId()
  }

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = response match {
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
    val timing = request.globalFetchTiming
    if (timing != null && timing.sendCompleteNs != 0L) {
      val dequeued = request.requestDequeueTimeNanos
      val queued = request.responseCompleteTimeNanos
      val sending = request.responseDequeueTimeNanos
      fetchTimingLock.synchronized {
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
        if (timing.writeCalls > 0) {
          writeTimingSamples += 1
          writeCountTotals(0) += timing.sendBuffers
          writeCountTotals(1) += timing.writeCalls
          writeCountTotals(2) += timing.zeroByteWrites
          writeTimingTotalsNs(0) += timing.firstWriteNs - sending
          writeTimingTotalsNs(1) += timing.writeElapsedNs
          writeTimingTotalsNs(2) += timing.lastWriteEndNs - timing.firstWriteNs - timing.writeElapsedNs
          writeTimingTotalsNs(3) += timing.sendCompleteNs - timing.lastWriteEndNs
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

    val (pageSamples, rangeSamples, stageTotals, writeSamples, writeCounts, writeTimes) = fetchTimingLock.synchronized {
      val window = (fetchTimingSamples, rangeTimingSamples, fetchTimingTotalsNs.clone(),
        writeTimingSamples, writeCountTotals.clone(), writeTimingTotalsNs.clone())
      fetchTimingSamples = 0L
      rangeTimingSamples = 0L
      writeTimingSamples = 0L
      java.util.Arrays.fill(fetchTimingTotalsNs, 0L)
      java.util.Arrays.fill(writeCountTotals, 0L)
      java.util.Arrays.fill(writeTimingTotalsNs, 0L)
      window
    }
    def emitTiming(name: String, scope: String, value: AnyVal): Unit =
      writer.submit(Sample(nowMs, durationMs, brokerId, "", "", "global-sequence-fetch-timing", name, scope, value.toString))

    if (pageSamples > 0) {
      emitTiming("samples", "page", pageSamples)
      emitTiming("samples", "range-page", rangeSamples)
      FetchTimingScopes.indices.foreach { stage =>
        emitTiming("duration-ms-total", FetchTimingScopes(stage), stageTotals(stage) / 1000000.0)
      }
    }
    if (writeSamples > 0) {
      emitTiming("samples", "socket-write-page", writeSamples)
      WriteCountScopes.indices.foreach { metric =>
        emitTiming("count-total", WriteCountScopes(metric), writeCounts(metric))
      }
      WriteTimingScopes.indices.foreach { stage =>
        emitTiming("duration-ms-total", WriteTimingScopes(stage), writeTimes(stage) / 1000000.0)
      }
    }

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
  private val WriteCountScopes = Vector("response-send-buffers", "socket-write-calls", "zero-byte-socket-write-calls")
  private val WriteTimingScopes = Vector("response-dequeue-to-first-write", "socket-write",
    "between-socket-writes", "last-write-to-send-complete")

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
