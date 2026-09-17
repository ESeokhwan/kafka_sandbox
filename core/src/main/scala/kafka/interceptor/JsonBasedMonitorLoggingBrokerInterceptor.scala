package kafka.interceptor

import kafka.network.RequestChannel
import moniq.util.{FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor, ILatencyMonitoringMessageAdaptor}
import moniq.writer.strategy.{CompositeMonitorLogWriteStrategy, FileMonitorLogWriteStrategy}
import moniq.writer.MonitorLogWriter
import moniq.{JsonBasedLatencyMonitorLog, MonitorQueue}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.utils.{LogContext, Utils}

import java.util.concurrent.ExecutorService

class JsonBasedMonitorLoggingBrokerInterceptor(
  val logContext: LogContext,
  settings: MonitorLoggingSettings
) extends IBrokerInterceptor {

  private val messageAdapter: ILatencyMonitoringMessageAdaptor = new FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor()

  private var monitorQueue: MonitorQueue = _
  private var monitorLogWriter: MonitorLogWriter = _
  private var monitorLogThread: Thread = _
  private var monitorLogExtensionHandler: MonitorLogExtensionHandler = _
  private var monitorLogRollOutExtensionHandler: MonitorLogRollOutExtensionHandler = _
  private var monitorLogControlExecutor: ExecutorService = _
  private var fileMonitorWriteStrategy: FileMonitorLogWriteStrategy = _
  private var monitorWriteStrategy: CompositeMonitorLogWriteStrategy = _

  override def init(): Unit = {
    monitorQueue = new MonitorQueue()
    fileMonitorWriteStrategy = new FileMonitorLogWriteStrategy(
      settings.outputPath, settings.rollOutInterval, settings.maxLogsPerFile, settings.format)
    monitorWriteStrategy = new CompositeMonitorLogWriteStrategy(fileMonitorWriteStrategy)
    monitorLogWriter = new MonitorLogWriter(
      monitorQueue, monitorWriteStrategy, settings.batchPolicy, settings.flushPolicy, settings.preprocessingWorkerCount)
    monitorLogThread = new Thread(monitorLogWriter)
    monitorLogThread.start()
    monitorLogControlExecutor = MonitorLogExtensionHandler.newBoundedExecutor("monitor-log-control")
    monitorLogExtensionHandler = new MonitorLogExtensionHandler(monitorLogWriter, monitorLogControlExecutor)
    monitorLogRollOutExtensionHandler = new MonitorLogRollOutExtensionHandler(
      monitorLogWriter, fileMonitorWriteStrategy, monitorLogControlExecutor)
  }

  override def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = {
    val currentTime = System.currentTimeMillis()
    if (request.header.apiKey == ApiKeys.PRODUCE) {
      val produceRequest = request.body[ProduceRequest]
      produceRequest.data().topicData().forEach(topic => topic.partitionData.forEach { partition =>
        val memoryRecords: MemoryRecords = partition.records.asInstanceOf[MemoryRecords]
        memoryRecords.batches.forEach(batch => {
          batch.forEach(record => {
            val value = record.value()
            if (value != null) {
              monitorLogWriter.submit(
                new JsonBasedLatencyMonitorLog(messageAdapter, Utils.utf8(value), "NETWORK_PROCESSED", currentTime)
              )
            }
          })
        })
      })
    }
  }

  override def beforeHandleRequest(request: RequestChannel.Request): Unit = {}

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {
    val currentTime = System.currentTimeMillis()

    if (response.request.header.apiKey == ApiKeys.PRODUCE) {
      val produceRequest = response.request.body[ProduceRequest]
      produceRequest.data().topicData().forEach(topic => topic.partitionData.forEach { partition =>
        val memoryRecords: MemoryRecords = partition.records.asInstanceOf[MemoryRecords]
        memoryRecords.batches.forEach(batch => {
          batch.forEach(record => {
            val value = record.value()
            if (value != null) {
              monitorLogWriter.submit(
                new JsonBasedLatencyMonitorLog(messageAdapter, Utils.utf8(value), "IO_COMMITED", currentTime)
              )
            }
          })
        })
      })
    }
  }

  override def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {}

  override def extensionHandlers: Seq[BrokerExtensionHandler] =
    Option(monitorLogExtensionHandler).toSeq ++ Option(monitorLogRollOutExtensionHandler).toSeq

  override def shutdown(): Unit = {
    if (monitorLogWriter == null || monitorLogThread == null) {
      return
    }

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

    try {
      monitorLogThread.join()
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("MonitorLoggingBrokerInterceptor shutdown interrupted", e)
    }
    monitorWriteStrategy.close()
  }
}
