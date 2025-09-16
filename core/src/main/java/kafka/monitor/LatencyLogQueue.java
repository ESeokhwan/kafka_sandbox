package kafka.monitor;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class LatencyLogQueue {

  private final Queue<LatencyLog> queue;

  private final AtomicInteger size = new AtomicInteger(0);

  public LatencyLogQueue() {
    this.queue = new ConcurrentLinkedQueue<LatencyLog>();
  }

  public boolean enqueue(LatencyLog log) {
    boolean res = queue.add(log);
    size.incrementAndGet();
    return res;
  }

  public LatencyLog dequeue() {
    LatencyLog res = queue.poll();
    if (res != null) {
      size.decrementAndGet();
    }
    return res;
  }

  public int size() {
    return size.get();
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }
}
