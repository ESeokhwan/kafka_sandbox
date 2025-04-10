package kafka.monitor;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MonitorQueue {

  private final Queue<MonitorLog> queue;
  
  private final AtomicInteger size = new AtomicInteger(0);

  private MonitorQueue() {
    this.queue = new ConcurrentLinkedQueue<MonitorLog>();
  }

  public boolean enqueue(MonitorLog log) {
    boolean res = queue.add(log);
    size.incrementAndGet();
    return res;
  }

  public MonitorLog dequeue() {
    MonitorLog res = queue.poll();
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
