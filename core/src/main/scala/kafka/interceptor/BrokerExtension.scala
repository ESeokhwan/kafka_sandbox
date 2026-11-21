package kafka.interceptor

import org.apache.kafka.common.Uuid

import java.util.concurrent.CompletionStage

final case class BrokerExtensionContext(
  brokerId: Int,
  connectionId: String,
  principalName: String,
  clientId: String,
  listenerName: String,
  deadlineNanos: Long
)

final case class BrokerExtensionCommand(
  requestId: Uuid,
  target: String,
  operation: String,
  payloadVersion: Short,
  payload: Array[Byte],
  context: BrokerExtensionContext
)

final case class BrokerExtensionResult(
  payloadVersion: Short = 0,
  payload: Array[Byte] = Array.emptyByteArray
)

trait BrokerExtensionHandler {
  def target: String

  def handle(command: BrokerExtensionCommand): CompletionStage[BrokerExtensionResult]
}
