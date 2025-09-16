package kafka.monitor.writer;

import kafka.monitor.MonitorLog;

public interface IMonitorLogWriteStrategy {
  void write(MonitorLog log);
  boolean commit();
}
