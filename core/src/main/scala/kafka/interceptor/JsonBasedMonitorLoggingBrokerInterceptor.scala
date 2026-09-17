package kafka.interceptor

import kafka.network.RequestChannel
import moniq.util.{FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor, ILatencyMonitoringMessageAdaptor}
import moniq.writer.strategy.FileMonitorLogWriteStrategy
import moniq.writer.strategy.FileMonitorLogWriteStrategy.Format
import moniq.writer.{BatchPolicy, MonitorLogWriter}
import moniq.{JsonBasedLatencyMonitorLog, MonitorQueue}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.utils.{LogContext, Utils}

import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.{ArrayBlockingQueue, ExecutorService, ThreadFactory, ThreadPoolExecutor, TimeUnit}

class JsonBasedMonitorLoggingBrokerInterceptor(val logContext: LogContext) extends IBrokerInterceptor {

  private val messageAdapter: ILatencyMonitoringMessageAdaptor = new FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor()

  private var monitorQueue: MonitorQueue = _
  private var monitorLogWriter: MonitorLogWriter = _
  private var monitorLogThread: Thread = _
  private var monitorLogExtensionHandler: MonitorLogExtensionHandler = _
  private var monitorLogExtensionExecutor: ExecutorService = _

  override def init(): Unit = {
    monitorQueue = new MonitorQueue()
    val monitorWriteStrategy = new FileMonitorLogWriteStrategy(Path.of("output/json-based-monitor.log"), Duration.ZERO, 1_000_000, Format.COMMA_SEPARATED)
    monitorLogWriter = new MonitorLogWriter(monitorQueue, monitorWriteStrategy, BatchPolicy.unbounded())
    monitorLogThread = new Thread(monitorLogWriter)
    monitorLogThread.start()
    monitorLogExtensionExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue[Runnable](1), new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, "monitor-log-extension")
        thread.setDaemon(true)
        thread
      }
    }, new ThreadPoolExecutor.AbortPolicy())
    monitorLogExtensionHandler = new MonitorLogExtensionHandler(monitorLogWriter, monitorLogExtensionExecutor)
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
              val message = Utils.utf8(value)
              val coreMessage = messageAdapter.extractMessageId(message)
              if (coreMessage.startsWith("R")) {
                monitorLogWriter.submit(
                  new JsonBasedLatencyMonitorLog(messageAdapter, Utils.utf8(value), "NETWORK_PROCESSED", currentTime)
                )
              }
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
              val message = Utils.utf8(value)
              val coreMessage = messageAdapter.extractMessageId(message)
              if (coreMessage.startsWith("R")) {
                monitorLogWriter.submit(
                  new JsonBasedLatencyMonitorLog(messageAdapter, Utils.utf8(value), "IO_COMMITED", currentTime)
                )
              }
            }
          })
        })
      })
    }
  }

  override def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {}

  override def extensionHandlers: Seq[BrokerExtensionHandler] = Option(monitorLogExtensionHandler).toSeq

  override def shutdown(): Unit = {
    if (monitorLogWriter == null || monitorLogThread == null) {
      return
    }

    if (monitorLogExtensionHandler != null) {
      monitorLogExtensionHandler.shutdown()
    }

    monitorLogWriter.gracefulShutdown()

    try {
      monitorLogThread.join()
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("MonitorLoggingBrokerInterceptor shutdown interrupted", e)
    }
  }
}
