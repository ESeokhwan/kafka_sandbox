package kafka.interceptor

import kafka.network.RequestChannel;


class BrokerInterceptors(val interceptors: Vector[IBrokerInterceptor]) {

  def init(): Unit = {
    interceptors.foreach(_.init())
  }

  def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = {
    interceptors.foreach(_.beforeSendRequestToQueue(request, connectionId))
  }

  def afterUnmuteChannel(response: RequestChannel.Response, connectionId: String): Unit = {
    interceptors.foreach(_.afterUnmuteChannel(response, connectionId))
  }

  def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {
    interceptors.foreach(_.beforeSendResponseToQueue(response))
  }

  def beforeProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {
    interceptors.foreach(_.beforeProcessResponse(response, connectionId))
  }

  def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {
    interceptors.foreach(_.afterProcessResponse(response, connectionId))
  }

  def shutdown(): Unit = {
    interceptors.foreach(_.shutdown())
  }
}
