package kafka.interceptor

import kafka.network.RequestChannel;

import java.util.concurrent.CompletionStage


class BrokerInterceptors(val interceptors: Vector[IBrokerInterceptor]) {
  @volatile private var extensionRegistry: BrokerExtensionRegistry = BrokerExtensionRegistry.empty

  def init(): Unit = {
    interceptors.foreach(_.init())
    extensionRegistry = new BrokerExtensionRegistry(interceptors.flatMap(_.extensionHandlers))
  }

  def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = {
    interceptors.foreach(_.beforeSendRequestToQueue(request, connectionId))
  }

  def beforeHandleRequest(request: RequestChannel.Request): Unit = {
    interceptors.foreach(_.beforeHandleRequest(request))
  }

  def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {
    interceptors.foreach(_.beforeSendResponseToQueue(response))
  }

  def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {
    interceptors.foreach(_.afterProcessResponse(response, connectionId))
  }

  def dispatchBrokerExtension(command: BrokerExtensionCommand): CompletionStage[BrokerExtensionResult] = {
    extensionRegistry.dispatch(command)
  }

  def shutdown(): Unit = {
    extensionRegistry = BrokerExtensionRegistry.empty
    interceptors.foreach(_.shutdown())
  }
}
