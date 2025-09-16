package kafka.monitor.reader;

import kafka.monitor.MonitorLog;

import java.util.List;

public interface IMonitorLogReadStrategy {
  void read(List<MonitorLog> tar);
}
