package kafka.interceptor

import kafka.interceptor.strategy.KafkaLogWriteStrategy
import kafka.network.RequestChannel
import moniq.writer.{BatchPolicy, MonitorLogWriter}
import moniq.{MonitorLog, MonitorQueue}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.utils.{LogContext, Utils}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

class MonitorLoggingBrokerInterceptor(val logContext: LogContext) extends IBrokerInterceptor {

  class Timestamps {
    var requestedTime: Long = _
    var requestedTimeNano: Long = _
    var completedTime: Long = _
    var completedTimeNano: Long = _
  }

  private var monitorQueue: MonitorQueue = _
  private var monitorLogWriter: MonitorLogWriter = _
  private var monitorLogThread: Thread = _

  private val requestMap = new ConcurrentHashMap[RequestChannel.Request, Timestamps]().asScala
  private val counter: AtomicLong = new AtomicLong(0)

  override def init(): Unit = {
    monitorQueue = new MonitorQueue()
    monitorLogWriter = new MonitorLogWriter(
      monitorQueue, new KafkaLogWriteStrategy(logContext), BatchPolicy.fixedSize(1))
    monitorLogThread = new Thread(monitorLogWriter)
    monitorLogThread.start()
  }

  override def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = {
    val currentTime = System.currentTimeMillis()
    val currentTimeNano = System.nanoTime()
    requestMap.put(request, new Timestamps {
      requestedTime = currentTime
      requestedTimeNano = currentTimeNano
    })
  }

  override def beforeHandleRequest(request: RequestChannel.Request): Unit = {}

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {
    val currentTime = System.currentTimeMillis()
    val currentTimeNano = System.nanoTime()

    val timestamps = requestMap.remove(response.request)
    timestamps match {
      case Some(ts) =>
        ts.completedTime = currentTime
        ts.completedTimeNano = currentTimeNano

        val curNum = counter.incrementAndGet()
        val api = response.request.header.apiKey.toString
        monitorLogWriter.submit(new MonitorLog(
          api,
          curNum.toString,
          "REQUESTED",
          ts.requestedTime,
          ts.requestedTimeNano
        ))
        monitorLogWriter.submit(new MonitorLog(
          api,
          curNum.toString,
          "COMPLETED",
          ts.completedTime,
          ts.completedTimeNano
        ))
      case None =>
    }

    if (response.request.header.apiKey == ApiKeys.PRODUCE) {
      val produceRequest = response.request.body[ProduceRequest]
      produceRequest.data().topicData().forEach(topic => topic.partitionData.forEach { partition =>
        val memoryRecords: MemoryRecords = partition.records.asInstanceOf[MemoryRecords]
        memoryRecords.batches.forEach(batch => {
          batch.forEach(record => {
            val value = record.value()
            if (value != null) {
              monitorLogWriter.submit(new MonitorLog(
                "PRODUCE",
                Utils.utf8(value),
                "COMMITED",
                currentTime,
                currentTimeNano
              ))
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
