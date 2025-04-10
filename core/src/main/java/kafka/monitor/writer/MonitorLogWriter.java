package kafka.monitor.writer;

import kafka.monitor.MonitorQueue;

// TODO: 일정 시간동안 monitorQueue에 있는 데이터가 flush안되면 자동으로 flush해주는 기능 추가하기
public class MonitorLogWriter implements Runnable {

  private final MonitorQueue monitorQueue;
  
  private final IMonitorLogWriteStrategy writeStrategy;
  
  private final int batchSize;

  private boolean terminated = false;

  private int curWrittenCnt = 0;

  public MonitorLogWriter(MonitorQueue monitorQueue, IMonitorLogWriteStrategy writeStrategy, int batchSize) {
    this.monitorQueue = monitorQueue;
    this.writeStrategy = writeStrategy;
    this.batchSize = batchSize;
  }

  public void gracefulShutdown() {
    terminated = true;
  }

  public void syncedWait() {
    synchronized (this) {
      try {
        wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void syncedNotify() {
    synchronized (this) {
      notify();
    }
  }

  public void notifyIfNeeded() {
    if (monitorQueue.size() >= batchSize) {
      syncedNotify();
    }
  }

  @Override
  public void run() {
    while (!(terminated && monitorQueue.isEmpty())) {
      if (!terminated && monitorQueue.isEmpty()) {
        syncedWait();
      }
      writeStrategy.write(monitorQueue.dequeue());
      curWrittenCnt += 1;
      tryFlushBatch();
    }
    flushBatch();
  }

  private void tryFlushBatch() {
    if (curWrittenCnt >= batchSize) {
      flushBatch();
      if (!terminated && monitorQueue.size() < batchSize) {
        syncedWait();
      }
    }
  }

  private void flushBatch() {
    writeStrategy.commit();
    curWrittenCnt = 0;
  }

}