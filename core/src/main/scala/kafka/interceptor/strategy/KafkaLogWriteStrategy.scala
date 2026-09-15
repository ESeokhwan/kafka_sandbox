package kafka.interceptor.strategy

import moniq.IMonitorLog
import moniq.writer.strategy.IMonitorLogWriteStrategy
import org.apache.kafka.common.utils.LogContext

final class KafkaLogWriteStrategy(val logContext: LogContext) extends IMonitorLogWriteStrategy {
  private val logger = logContext.logger(classOf[KafkaLogWriteStrategy])

  override def write(log: IMonitorLog): Unit = {
    if (!logger.isInfoEnabled) {
      return
    }

    val headers = log.getHeaders
    val values = log.getValues
    val line = new StringBuilder
    var index = 0
    while (index < values.size()) {
      if (index > 0) {
        line.append(", ")
      }
      if (index < headers.size()) {
        line.append(headers.get(index)).append(": ")
      }
      line.append(values.get(index))
      index += 1
    }

    logger.info("MonitorLog -- {}", line)
  }

  override def commit(): Boolean = true
}