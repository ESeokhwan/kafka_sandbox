package kafka.interceptor

import kafka.network.RequestChannel

class BrokerInterceptor extends IBrokerInterceptor {

  override def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = {}

  override def afterUnmuteChannel(response: RequestChannel.Response, connectionId: String): Unit = {}

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {}

  override def beforeProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {}

  override def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = {}
}
