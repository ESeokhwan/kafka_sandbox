package kafka.interceptor.util

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

class PrintableMessageCounter(
  val commitTerm: Long,
  currentTimeMillis: () => Long = () => System.currentTimeMillis()
) {

  require(commitTerm > 0, "commitTerm must be positive")

  private val counter: AtomicLong = new AtomicLong(0)
  private val isCommitting: AtomicBoolean = new AtomicBoolean(false)

  private var lastCommitTime: Long = 0L
  private var lastCommitCount: Long = 0L

  def increaseCounter(delta: Long): Unit = {
    counter.addAndGet(delta)
  }

  def tryCommit(f: (Long, Long, Long, Long) => Unit): Unit = {
    if (isCommitting.compareAndSet(false, true)) {
      try {
        val currentTime = currentTimeMillis()
        if (lastCommitTime == 0L) {
          lastCommitTime = currentTime
        } else if (currentTime - lastCommitTime > commitTerm) {
          val currentCount = counter.get()
          f(lastCommitTime, lastCommitCount, currentTime, currentCount)
          lastCommitTime = currentTime
          lastCommitCount = currentCount
        }
      } finally {
        isCommitting.set(false)
      }
    }
  }

}
