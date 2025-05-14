package kafka.interceptor

import kafka.network.RequestChannel

trait IBrokerInterceptor {
  def init(): Unit

  def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit

  def beforeHandleRequest(request: RequestChannel.Request): Unit

  def beforeSendResponseToQueue(response: RequestChannel.Response): Unit

  def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit

  def shutdown(): Unit
}
