package kafka.interceptor

import kafka.interceptor.util.PrintableMessageCounter
import kafka.network.RequestChannel
import org.apache.kafka.common.protocol.ApiKeys
import kafka.utils._

class ProduceRequestRateCheckInterceptor extends IBrokerInterceptor with Logging {

  private val commitTerm = 1000

  val onRequestQueueCounter: PrintableMessageCounter = new PrintableMessageCounter(commitTerm)
  val onResponseQueueCounter: PrintableMessageCounter = new PrintableMessageCounter(commitTerm)

  override def beforeSendRequestToQueue(request: RequestChannel.Request, connectionId: String): Unit = {
    if (request.header.apiKey() == ApiKeys.PRODUCE) {
      onRequestQueueCounter.increaseCounter(1)
      onRequestQueueCounter.tryCommit((lastCommitTime, lastCommitCount, curCount, curTime) => {
        val messageRate = (curCount - lastCommitCount) / (curTime - lastCommitTime)
        infoWithTag("produce-rate-check", "Entered Produce Request Rate: " + messageRate)
      })
    }
  }

  override def afterUnmuteChannel(response: RequestChannel.Response, connectionId: String): Unit = _

  override def beforeSendResponseToQueue(response: RequestChannel.Response): Unit = {
    if (response.request.header.apiKey() == ApiKeys.PRODUCE) {
      onResponseQueueCounter.increaseCounter(1)
      onResponseQueueCounter.tryCommit((lastCommitTime, lastCommitCount, curCount, curTime) => {
        val messageRate = (curCount - lastCommitCount) / (curTime - lastCommitTime)
        infoWithTag("produce-rate-check", "Commited Produce Request Rate: " + messageRate)
      })
    }
  }

  override def beforeProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = _

  override def afterProcessResponse(response: RequestChannel.Response, connectionId: String): Unit = _
}
