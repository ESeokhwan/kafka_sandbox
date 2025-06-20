package kafka.interceptor

import kafka.monitor.writer.{MonitorLogWriter, ScrapableConsoleMonitorLogWriteStrategy}
import kafka.monitor.{MonitorLog, MonitorQueue}
import kafka.network.RequestChannel
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{CreateTopicsRequest, DeleteTopicsRequest}
import org.apache.kafka.common.utils.LogContext

class TopicCreateRequestMonitorBrokerInterceptor(val logContext: LogContext) extends IBrokerInterceptor {

  private var monitorQueue: MonitorQueue = _
  private var monitorLogWriter: MonitorLogWriter = _
  private var monitorLogThread: Thread = _

  override def init(): Unit = {
    monitorQueue = new MonitorQueue()
    monitorLogWriter = new MonitorLogWriter(
      monitorQueue, new ScrapableConsoleMonitorLogWriteStrategy(), 1)
    monitorLogThread = new Thread(monitorLogWriter)
    monitorLogThread.start()
  }

  override def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = {
    val currentTime = System.currentTimeMillis()
    val currentTimeNano = System.nanoTime()
    if (request.header.apiKey== ApiKeys.CREATE_TOPICS) {
      val createTopicsRequest = request.body[CreateTopicsRequest]
      monitorQueue.enqueue(
        new MonitorLog(
          "CREATE_TOPIC",
          String.join(",", createTopicsRequest.data().topics().stream().map(_.name()).toList[String]),
          "REQUESTED",
          currentTime,
          currentTimeNano
        )
      )
      monitorLogWriter.notifyIfNeeded()
    } else if (request.header.apiKey == ApiKeys.DELETE_TOPICS) {
      val deleteTopicsRequest = request.body[DeleteTopicsRequest]
      monitorQueue.enqueue(
        new MonitorLog(
          "DELETE_TOPIC",
          String.join(",", deleteTopicsRequest.data().topics().stream().map(_.name()).toList[String]),
          "REQUESTED",
          currentTime,
          currentTimeNano
        )
      )
      monitorLogWriter.notifyIfNeeded()
    }
  }

  override def beforeHandleRequest(request: RequestChannel.Request): Unit = {}

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {
    val currentTime = System.currentTimeMillis()
    val currentTimeNano = System.nanoTime()
    if (response.request.header.apiKey == ApiKeys.CREATE_TOPICS) {
      val createTopicsRequest = response.request.body[CreateTopicsRequest]
      monitorQueue.enqueue(
        new MonitorLog(
          "CREATE_TOPIC",
          String.join(",", createTopicsRequest.data().topics().stream().map(_.name()).toList[String]),
          "COMPLETED",
          currentTime,
          currentTimeNano
        )
      )
      monitorLogWriter.notifyIfNeeded()
    } else if (response.request.header.apiKey == ApiKeys.DELETE_TOPICS) {
      val deleteTopicsRequest = response.request.body[DeleteTopicsRequest]
      monitorQueue.enqueue(
        new MonitorLog(
          "DELETE_TOPIC",
          String.join(",", deleteTopicsRequest.data().topics().stream().map(_.name()).toList[String]),
          "COMPLETED",
          currentTime,
          currentTimeNano
        )
      )
      monitorLogWriter.notifyIfNeeded()
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
        throw new RuntimeException("TopicCreateRequestMonitorBrokerInterceptor shutdown interrupted", e)
    }
  }
}
