package kafka.interceptor

import kafka.monitor.writer.{MonitorLogWriter, ScrapableConsoleMonitorLogWriteStrategy}
import kafka.monitor.{MonitorLog, MonitorQueue}
import kafka.network.RequestChannel
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.utils.LogContext

class MonitorLoggingBrokerInterceptor(val logContext: LogContext) extends IBrokerInterceptor {

  private var monitorQueue: MonitorQueue = _
  private var monitorLogWriter: MonitorLogWriter = _
  private var monitorLogThread: Thread = _

  override def init(): Unit = {
    monitorQueue = new MonitorQueue()
    monitorLogWriter = new MonitorLogWriter(
      monitorQueue, new ScrapableConsoleMonitorLogWriteStrategy(), 1_000_000)
    monitorLogThread = new Thread(monitorLogWriter)
    monitorLogThread.start()
  }

  override def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = {
    val currentTime = System.currentTimeMillis()
    val currentTimeNano = System.nanoTime()

    if (request.header.apiKey == ApiKeys.METADATA) {
      val produceRequest = request.body[MetadataRequest]
      produceRequest.data().topics().forEach(topic => {
        val topicName = topic.name()
        monitorQueue.enqueue(new MonitorLog(
          "METADATA",
          topicName,
          "REQUESTED",
          currentTime,
          currentTimeNano
        ))
        monitorLogWriter.notifyIfNeeded()
      })
    }
  }

  override def beforeHandleRequest(request: RequestChannel.Request): Unit = {}

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {
    val currentTime = System.currentTimeMillis()
    val currentTimeNano = System.nanoTime()

    if (response.request.header.apiKey == ApiKeys.METADATA) {
      val metadataRequest = response.request.body[MetadataRequest]
      metadataRequest.data().topics().forEach(topic => {
        val topicName = topic.name()
        monitorQueue.enqueue(new MonitorLog(
          "METADATA",
          topicName,
          "RESPONDED",
          currentTime,
          currentTimeNano
        ))
        monitorLogWriter.notifyIfNeeded()
      })
    }
  }

  override def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {}

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
