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

class ProduceRequestRateCheckInterceptor(
  val logContext: LogContext,
  settings: ProduceRequestThroughputSettings,
  currentTimeMillis: () => Long = () => System.currentTimeMillis()
) extends IBrokerInterceptor {

  private var responseCounter: ProduceRequestOutcomeCounter = _
  private var monitorLogWriter: MonitorLogWriter = _
  private var monitorLogThread: Thread = _
  private var monitorWriteStrategy: CompositeMonitorLogWriteStrategy = _

  override def init(): Unit = {
    val fileWriteStrategy = new FileMonitorLogWriteStrategy(
      settings.outputPath,
      settings.rollOutInterval,
      settings.maxRecordsPerFile,
      Format.COMMA_SEPARATED
    )
    monitorWriteStrategy = new CompositeMonitorLogWriteStrategy(
      new ProduceRequestThroughputKafkaLogWriteStrategy(logContext),
      fileWriteStrategy
    )
    monitorLogWriter = new MonitorLogWriter(
      new MonitorQueue(), monitorWriteStrategy, BatchPolicy.fixedSize(1))
    monitorLogThread = new Thread(monitorLogWriter, "produce-request-throughput-writer")
    responseCounter = new ProduceRequestOutcomeCounter(settings.measurementIntervalMs, currentTimeMillis)
    monitorLogThread.start()
  }

  override def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = {}

  override def beforeHandleRequest(request: RequestChannel.Request): Unit = {}

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {
    if (response.request.header.apiKey() == ApiKeys.PRODUCE) {
      produceRequestSucceeded(response).foreach(recordHandledProduceRequest)
    }
  }

  override def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {}

  private[interceptor] def produceRequestSucceeded(response: RequestChannel.Response): Option[Boolean] =
    response match {
      case sendResponse: RequestChannel.SendResponse =>
        Some(!sendResponse.errorCounts.exists { case (error, count) => error != Errors.NONE && count > 0 })
      case _: RequestChannel.NoOpResponse => Some(true)
      case _: RequestChannel.CloseConnectionResponse => Some(false)
      case _: RequestChannel.StartThrottlingResponse | _: RequestChannel.EndThrottlingResponse => None
    }

  private[interceptor] def recordHandledProduceRequest(successful: Boolean): Unit = {
    responseCounter.record(successful, (currentTime, measurementDurationMs,
                                        successfulRequestCount, failedRequestCount) => {
      monitorLogWriter.submit(new ProduceRequestThroughputMonitorLog(
        currentTime,
        measurementDurationMs,
        successfulRequestCount,
        failedRequestCount
      ))
    })
  }

  override def shutdown(): Unit = {
    if (monitorLogWriter == null || monitorLogThread == null) {
      return
    }
    monitorLogWriter.gracefulShutdown()
    try {
      monitorLogThread.join()
    } catch {
      case error: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("ProduceRequestRateCheckInterceptor shutdown interrupted", error)
    }
    monitorWriteStrategy.close()
  }

}
