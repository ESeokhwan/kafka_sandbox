package kafka.interceptor

import kafka.monitor.writer.{ConsoleMonitorLogWriteStrategy, MonitorLogWriter}
import kafka.monitor.{MonitorLog, MonitorQueue}
import kafka.network.RequestChannel
import org.apache.kafka.common.protocol.{ApiKeys, MessageUtil}
import org.apache.kafka.common.requests.{CreateTopicsRequest, MetadataRequest}
import org.apache.kafka.common.utils.LogContext

class MetadataRequestMonitorBrokerInterceptor(val logContext: LogContext) extends IBrokerInterceptor {

  private var monitorQueue: MonitorQueue = _
  private var monitorLogWriter: MonitorLogWriter = _
  private var monitorLogThread: Thread = _

  override def init(): Unit = {
    monitorQueue = new MonitorQueue()
    monitorLogWriter = new MonitorLogWriter(
      monitorQueue, new ConsoleMonitorLogWriteStrategy(logContext, true, false), 1)
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
          extractTopicNames(metadataRequest),
          "REQUESTED",
          currentTime,
          currentTimeNano
        )
      )
      monitorLogWriter.notifyIfNeeded()
    } else if (request.header.apiKey == ApiKeys.PRODUCE) {
      monitorQueue.enqueue(
        new MonitorLog(
          "PRODUCE",
          "",
          "REQUESTED",
          currentTime,
          currentTimeNano
        )
      )
      monitorLogWriter.notifyIfNeeded()
    } else if (request.header.apiKey() == ApiKeys.CREATE_TOPICS) {
      val createTopicsRequest = request.body[CreateTopicsRequest]
      monitorQueue.enqueue(
        new MonitorLog(
          "CREATE_TOPIC",
          createTopicsRequest.data().topics().toString,
          "REQUESTED",
          currentTime,
          currentTimeNano
        )
      )
    }
  }

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {}

  override def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {
    val currentTime = System.currentTimeMillis()
    val currentTimeNano = System.nanoTime()
    if (response.request.header.apiKey == ApiKeys.METADATA) {
      val metadataRequest = response.request.body[MetadataRequest]
      monitorQueue.enqueue(
        new MonitorLog(
          "METADATA",
          extractTopicNames(metadataRequest),
          "COMPLETED",
          currentTime,
          currentTimeNano
        )
      )
      monitorLogWriter.notifyIfNeeded()
    } else if (response.request.header.apiKey == ApiKeys.PRODUCE) {
      monitorQueue.enqueue(
        new MonitorLog(
          "PRODUCE",
          "",
          "COMPLETED",
          currentTime,
          currentTimeNano
        )
      )
      monitorLogWriter.notifyIfNeeded()
    } else if (response.request.header.apiKey == ApiKeys.CREATE_TOPICS) {
      val createTopicsRequest = response.request.body[CreateTopicsRequest]
      monitorQueue.enqueue(
        new MonitorLog(
          "CREATE_TOPIC",
          createTopicsRequest.data().topics().toString,
          "COMPLETED",
          currentTime,
          currentTimeNano
        )
      )
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

  private def extractTopicNames(metadataRequest: MetadataRequest): String = {
    if (metadataRequest.data().topics() == null) {
      return ""
    }
    MessageUtil.deepToString(metadataRequest.data().topics().stream().map(e => {
      e.name()
    }).iterator())
  }
}
