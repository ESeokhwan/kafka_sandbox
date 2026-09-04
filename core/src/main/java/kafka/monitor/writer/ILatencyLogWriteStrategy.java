package kafka.monitor.writer;

import kafka.monitor.LatencyLog;

public interface ILatencyLogWriteStrategy {
  void write(LatencyLog log);
  boolean commit();
}
