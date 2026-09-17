package kafka.interceptor

import org.apache.kafka.common.errors.InvalidRequestException

import java.util.concurrent.{CompletableFuture, CompletionStage}
import java.util.regex.Pattern
import scala.util.control.NonFatal

final class BrokerExtensionRegistry(handlers: Seq[BrokerExtensionHandler]) {
  private val handlersByTarget: Map[String, BrokerExtensionHandler] = handlers.foldLeft(Map.empty[String, BrokerExtensionHandler]) {
    case (registeredHandlers, null) =>
      throw new IllegalArgumentException("Broker extension handler must not be null")
    case (registeredHandlers, handler) =>
      val target = BrokerExtensionRegistry.validateTarget(handler.target)
      if (registeredHandlers.contains(target)) {
        throw new IllegalArgumentException(s"Duplicate broker extension handler target: $target")
      }
      registeredHandlers + (target -> handler)
  }

  def dispatch(command: BrokerExtensionCommand): CompletionStage[BrokerExtensionResult] = {
    if (command == null) {
      return failed(new InvalidRequestException("Broker extension command must not be null"))
    }

    handlersByTarget.get(command.target) match {
      case None =>
        failed(new InvalidRequestException(s"Unknown broker extension target: ${command.target}"))
      case Some(handler) =>
        try {
          val result = handler.handle(command)
          if (result == null) {
            failed(new IllegalStateException(s"Broker extension handler ${handler.target} returned null"))
          } else {
            result
          }
        } catch {
          case NonFatal(error) => failed(error)
        }
    }
  }

  def targets: Set[String] = handlersByTarget.keySet

  private def failed(error: Throwable): CompletionStage[BrokerExtensionResult] =
    CompletableFuture.failedFuture[BrokerExtensionResult](error)
}

object BrokerExtensionRegistry {
  private val TargetPattern = Pattern.compile("[a-z0-9][a-z0-9._-]{0,63}")

  val empty: BrokerExtensionRegistry = new BrokerExtensionRegistry(Seq.empty)

  private[interceptor] def validateTarget(target: String): String = {
    if (target == null || !TargetPattern.matcher(target).matches()) {
      throw new IllegalArgumentException(s"Invalid broker extension handler target: $target")
    }
    target
  }
}
