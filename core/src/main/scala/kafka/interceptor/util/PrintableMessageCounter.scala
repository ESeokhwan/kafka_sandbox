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
    if (lastCommitTime == 0L) {
      if (isCommitting.compareAndSet(false, true)) {
        lastCommitTime = System.currentTimeMillis()
        isCommitting.set(false)
      }
    } else if (System.currentTimeMillis() > lastCommitTime + commitTerm) {
      if (isCommitting.compareAndSet(false, true)) {
        val curCount = counter.get()
        val curTimeMilli = System.currentTimeMillis()
        f(lastCommitTime, lastCommitCount, curTimeMilli, curCount)
        lastCommitTime = curTimeMilli
        lastCommitCount = curCount
        isCommitting.set(false)
      }
    }
  }

}
