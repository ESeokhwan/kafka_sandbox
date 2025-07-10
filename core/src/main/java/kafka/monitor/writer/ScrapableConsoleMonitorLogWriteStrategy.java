package kafka.monitor.writer;

import kafka.monitor.MonitorLog;

public class ScrapableConsoleMonitorLogWriteStrategy implements IMonitorLogWriteStrategy {

    public ScrapableConsoleMonitorLogWriteStrategy() {
    }

    @Override
    public void write(MonitorLog log) {
        System.out.println(log.getType() + ", " + log.getId() + ", " + log.getTimestamp() + ", " + log.getTimestampNano() + ", " + log.getState());
    }

    @Override
    public boolean commit() {
        System.out.flush();
        return true;
    }
}
