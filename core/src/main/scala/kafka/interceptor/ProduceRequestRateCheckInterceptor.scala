package kafka.interceptor

import kafka.interceptor.strategy.ProduceRequestThroughputKafkaLogWriteStrategy
import kafka.interceptor.util.ProduceRequestOutcomeCounter
import kafka.network.RequestChannel
import moniq.MonitorQueue
import moniq.writer.strategy.FileMonitorLogWriteStrategy.Format
import moniq.writer.strategy.{CompositeMonitorLogWriteStrategy, FileMonitorLogWriteStrategy}
import moniq.writer.{BatchPolicy, MonitorLogWriter}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.utils.LogContext

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ExecutorService, Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}
import scala.util.control.NonFatal

class ProduceRequestRateCheckInterceptor(
  val logContext: LogContext,
  settings: ProduceRequestThroughputSettings,
  currentTimeMillis: () => Long = () => System.currentTimeMillis()
) extends IBrokerInterceptor {

  private val logger = logContext.logger(classOf[ProduceRequestRateCheckInterceptor])
  private val acceptingRecords = new AtomicBoolean(false)

  private var responseCounter: ProduceRequestOutcomeCounter = _
  private var monitorLogWriter: MonitorLogWriter = _
  private var monitorLogThread: Thread = _
  private var monitorWriteStrategy: CompositeMonitorLogWriteStrategy = _
  private var fileMonitorWriteStrategy: FileMonitorLogWriteStrategy = _
  private var monitorLogExtensionHandler: MonitorLogExtensionHandler = _
  private var monitorLogRollOutExtensionHandler: ProduceRequestMonitorLogRollOutExtensionHandler = _
  private var monitorLogControlExecutor: ExecutorService = _
  private var measurementScheduler: ScheduledExecutorService = _
  private var lastMeasurementTimeMs = 0L
  private var previousSuccessfulRequestCount = 0L
  private var previousFailedRequestCount = 0L

  override def init(): Unit = {
    fileMonitorWriteStrategy = new FileMonitorLogWriteStrategy(
      settings.outputPath,
      settings.rollOutInterval,
      settings.maxRecordsPerFile,
      Format.COMMA_SEPARATED
    )
    monitorWriteStrategy = if (settings.realtimeLogEnabled) {
      new CompositeMonitorLogWriteStrategy(
        new ProduceRequestThroughputKafkaLogWriteStrategy(logContext),
        fileMonitorWriteStrategy
      )
    } else {
      new CompositeMonitorLogWriteStrategy(fileMonitorWriteStrategy)
    }
    val batchPolicy = if (settings.realtimeLogEnabled) BatchPolicy.fixedSize(1) else settings.batchPolicy
    val flushPolicy = if (settings.realtimeLogEnabled) moniq.writer.FlushPolicy.disabled() else settings.flushPolicy
    monitorLogWriter = new MonitorLogWriter(
      new MonitorQueue(), monitorWriteStrategy, batchPolicy, flushPolicy)
    monitorLogThread = new Thread(monitorLogWriter, "produce-request-throughput-writer")
    responseCounter = new ProduceRequestOutcomeCounter
    lastMeasurementTimeMs = currentTimeMillis()
    previousSuccessfulRequestCount = 0L
    previousFailedRequestCount = 0L
    measurementScheduler = newMeasurementScheduler()
    acceptingRecords.set(true)
    monitorLogThread.start()
    monitorLogControlExecutor = MonitorLogExtensionHandler.newBoundedExecutor("produce-request-throughput-control")
    monitorLogExtensionHandler = new MonitorLogExtensionHandler(
      monitorLogWriter,
      monitorLogControlExecutor,
      targetName = "produce-request-throughput",
    )
    val monitorLogRollOutDelegate = new MonitorLogRollOutExtensionHandler(
      monitorLogWriter,
      fileMonitorWriteStrategy,
      monitorLogControlExecutor
    )
    monitorLogRollOutExtensionHandler = new ProduceRequestMonitorLogRollOutExtensionHandler(
      monitorLogRollOutDelegate,
      () => resetMeasurementBaseline()
    )
    measurementScheduler.scheduleAtFixedRate(
      () => runScheduledMeasurement(),
      settings.measurementIntervalMs,
      settings.measurementIntervalMs,
      TimeUnit.MILLISECONDS
    )
  }

  override def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = {}

  override def beforeHandleRequest(request: RequestChannel.Request): Unit = {}

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {
    if (response.request.header.apiKey() == ApiKeys.PRODUCE) {
      produceRequestSucceeded(response).foreach(recordHandledProduceRequest)
    }
  }

  override def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {}

  override def extensionHandlers: Seq[BrokerExtensionHandler] =
    Option(monitorLogExtensionHandler).toSeq ++ Option(monitorLogRollOutExtensionHandler).toSeq

  private[interceptor] def produceRequestSucceeded(response: RequestChannel.Response): Option[Boolean] =
    response match {
      case sendResponse: RequestChannel.SendResponse =>
        Some(!sendResponse.errorCounts.exists { case (error, count) => error != Errors.NONE && count > 0 })
      case _: RequestChannel.NoOpResponse => Some(true)
      case _: RequestChannel.CloseConnectionResponse => Some(false)
      case _: RequestChannel.StartThrottlingResponse | _: RequestChannel.EndThrottlingResponse => None
    }

  private[interceptor] def recordHandledProduceRequest(successful: Boolean): Unit = {
    if (acceptingRecords.get()) {
      responseCounter.record(successful)
    }
  }

  private[interceptor] def emitMeasurement(includeEmpty: Boolean = true): Unit = synchronized {
    val currentTimeMs = currentTimeMillis()
    val measurementDurationMs = currentTimeMs - lastMeasurementTimeMs
    if (measurementDurationMs <= 0L) {
      return
    }

    val snapshot = responseCounter.snapshot()
    val successfulRequestCount = snapshot.successfulRequestCount - previousSuccessfulRequestCount
    val failedRequestCount = snapshot.failedRequestCount - previousFailedRequestCount
    val hasRequests = successfulRequestCount > 0L || failedRequestCount > 0L
    val doesFirstRequestCome = previousSuccessfulRequestCount > 0L || previousFailedRequestCount > 0L
    if (hasRequests || (includeEmpty && doesFirstRequestCome)) {
      monitorLogWriter.submit(new ProduceRequestThroughputMonitorLog(
        currentTimeMs,
        measurementDurationMs,
        successfulRequestCount,
        failedRequestCount
      ))
    }
    previousSuccessfulRequestCount = snapshot.successfulRequestCount
    previousFailedRequestCount = snapshot.failedRequestCount
    lastMeasurementTimeMs = currentTimeMs
  }

  private def runScheduledMeasurement(): Unit = {
    try {
      emitMeasurement()
    } catch {
      case NonFatal(error) =>
        logger.error("Failed to emit produce request throughput measurement", error)
    }
  }

  private def resetMeasurementBaseline(): Unit = synchronized {
    responseCounter.reset()
    previousSuccessfulRequestCount = 0L
    previousFailedRequestCount = 0L
    lastMeasurementTimeMs = currentTimeMillis()
  }

  private def newMeasurementScheduler(): ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, "produce-request-throughput-scheduler")
        thread.setDaemon(true)
        thread
      }
    })

  private def stopMeasurementScheduler(): Option[InterruptedException] = {
    measurementScheduler.shutdown()
    try {
      if (!measurementScheduler.awaitTermination(5L, TimeUnit.SECONDS)) {
        measurementScheduler.shutdownNow()
      }
      None
    } catch {
      case error: InterruptedException =>
        measurementScheduler.shutdownNow()
        Some(error)
    }
  }

  override def shutdown(): Unit = {
    if (monitorLogWriter == null || monitorLogThread == null || !acceptingRecords.compareAndSet(true, false)) {
      return
    }
    val schedulerInterruption = stopMeasurementScheduler()
    emitMeasurement(includeEmpty = false)
    if (monitorLogExtensionHandler != null) {
      monitorLogExtensionHandler.shutdown()
    }
    if (monitorLogRollOutExtensionHandler != null) {
      monitorLogRollOutExtensionHandler.shutdown()
    }
    if (monitorLogControlExecutor != null) {
      monitorLogControlExecutor.shutdownNow()
    }
    monitorLogWriter.gracefulShutdown()
    var writerInterruption: Option[InterruptedException] = None
    try {
      monitorLogThread.join()
    } catch {
      case error: InterruptedException =>
        writerInterruption = Some(error)
    }
    monitorWriteStrategy.close()
    schedulerInterruption.orElse(writerInterruption).foreach { error =>
      Thread.currentThread().interrupt()
      throw new RuntimeException("ProduceRequestRateCheckInterceptor shutdown interrupted", error)
    }
  }

}
