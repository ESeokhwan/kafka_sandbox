package kafka.interceptor

import kafka.network.RequestChannel

trait IBrokerInterceptor {
  def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit

  def afterUnmuteChannel(response: RequestChannel.Response, connectionId: String): Unit

  def beforeSendResponseToQueue(response: RequestChannel.Response): Unit

  def beforeProcessResponse(response: RequestChannel.Response, connectionId: String): Unit

  def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit
}
