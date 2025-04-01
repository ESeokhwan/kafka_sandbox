package kafka.interceptor

import kafka.network.RequestChannel

class BrokerInterceptor extends IBrokerInterceptor {

  override def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = _

  override def afterUnmuteChannel(response: RequestChannel.Response, connectionId: String): Unit = _

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = _

  override def beforeProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = _

  override def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = _
}
