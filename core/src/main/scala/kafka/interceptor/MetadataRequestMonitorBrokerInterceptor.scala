package kafka.interceptor

import kafka.monitor.writer.{ConsoleMonitorLogWriteStrategy, MonitorLogWriter}
import kafka.monitor.{MonitorLog, MonitorQueue}
import kafka.network.RequestChannel
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.utils.LogContext

class MetadataRequestMonitorBrokerInterceptor(val logContext: LogContext) extends IBrokerInterceptor {

  private var monitorQueue: MonitorQueue = _
  private var monitorLogWriter: MonitorLogWriter = _
  private var monitorLogThread: Thread = _

  override def init(): Unit = {
    monitorQueue = new MonitorQueue()
    monitorLogWriter = new MonitorLogWriter(
      monitorQueue, new ConsoleMonitorLogWriteStrategy(logContext, true, false), 1000)
    monitorLogThread = new Thread(monitorLogWriter)
    monitorLogThread.start()
  }

  override def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = {}

  override def beforeHandleRequest(request: RequestChannel.Request): Unit = {
    val currentTime = System.currentTimeMillis()
    val currentTimeNano = System.nanoTime()
    if (request.header.apiKey == ApiKeys.METADATA) {
      val metadataRequest = request.body[MetadataRequest]
      monitorQueue.enqueue(
        new MonitorLog(
          "METADATA",
          metadataRequest.data().toString,
          "REQUESTED",
          currentTime,
          currentTimeNano
        )
      )
      monitorLogWriter.notifyIfNeeded()
    }
  }

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {
    val currentTime = System.currentTimeMillis()
    val currentTimeNano = System.nanoTime()
    if (response.request.header.apiKey == ApiKeys.METADATA) {
      val metadataRequest = response.request.body[MetadataRequest]
      monitorQueue.enqueue(
        new MonitorLog(
          "METADATA",
          metadataRequest.data().toString,
          "BEFORE_SEND_RESPONSE_TO_QUEUE",
          currentTime,
          currentTimeNano
        )
      )
      monitorLogWriter.notifyIfNeeded()
    }
  }

  override def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {
    val currentTime = System.currentTimeMillis()
    val currentTimeNano = System.nanoTime()
    if (response.request.header.apiKey == ApiKeys.METADATA) {
      val metadataRequest = response.request.body[MetadataRequest]
      monitorQueue.enqueue(
        new MonitorLog(
          "METADATA",
          metadataRequest.data().toString,
          "COMPLETED",
          currentTime,
          currentTimeNano
        )
      )
      monitorLogWriter.notifyIfNeeded()
    }
  }

  override def shutdown(): Unit = {
    if (monitorLogWriter == null || monitorLogThread == null) {
      return
    }

    monitorLogWriter.gracefulShutdown()
    monitorLogWriter.syncedNotify()

    try {
      monitorLogThread.join()
    } catch {
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new RuntimeException("MonitorLoggingBrokerInterceptor shutdown interrupted", e)
    }
  }
}
