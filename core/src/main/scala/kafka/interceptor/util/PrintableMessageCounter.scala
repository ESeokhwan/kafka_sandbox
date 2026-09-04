package kafka.interceptor.util

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

class PrintableMessageCounter(val commitTerm: Long) {

  private val counter: AtomicLong = new AtomicLong(0)
  private val isCommitting: AtomicBoolean = new AtomicBoolean(false)

  private var lastCommitTime: Long = 0L
  private var lastCommitCount: Long = 0L

  def increaseCounter(delta: Long): Unit = {
    counter.addAndGet(delta)
  }

  def tryCommit(f: (Long, Long, Long, Long) => Unit): Unit = {
    if (isCommitting.compareAndSet(false, true)) {
      if (lastCommitTime == 0L) {
        lastCommitTime = System.currentTimeMillis()
      } else if (System.currentTimeMillis() > lastCommitTime + commitTerm) {
        val curCount = counter.get()
        val curTimeMilli = System.currentTimeMillis()
        f(lastCommitTime, lastCommitCount, curTimeMilli, curCount)
        lastCommitTime = curTimeMilli
        lastCommitCount = curCount
      }
      isCommitting.set(false)
    }
  }

}
