package kafka.interceptor

import kafka.interceptor.strategy.KafkaLogWriteStrategy
import kafka.network.RequestChannel
import moniq.util.{FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor, ILatencyMonitoringMessageAdaptor}
import moniq.writer.{BatchPolicy, MonitorLogWriter}
import moniq.{JsonBasedLatencyMonitorLog, MonitorQueue}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.utils.{LogContext, Utils}

class JsonBasedMonitorLoggingBrokerInterceptor(val logContext: LogContext) extends IBrokerInterceptor {

  private val messageAdapter: ILatencyMonitoringMessageAdaptor = new FastExtractOnlyJsonBasedLatencyMonitoringMessageAdaptor()

  private var monitorQueue: MonitorQueue = _
  private var monitorLogWriter: MonitorLogWriter = _
  private var monitorLogThread: Thread = _

  override def init(): Unit = {
    monitorQueue = new MonitorQueue()
    monitorLogWriter = new MonitorLogWriter(
      monitorQueue, new KafkaLogWriteStrategy(logContext), BatchPolicy.unbounded())
    monitorLogThread = new Thread(monitorLogWriter)
    monitorLogThread.start()
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

  override def shutdown(): Unit = {
    if (monitorLogWriter == null || monitorLogThread == null) {
      return
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
