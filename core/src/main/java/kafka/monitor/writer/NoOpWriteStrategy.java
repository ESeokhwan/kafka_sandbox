package kafka.monitor.writer;

import kafka.monitor.MonitorLog;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class NoOpWriteStrategy implements IMonitorLogWriteStrategy {

    public NoOpWriteStrategy() {}

    @Override
    public void write(MonitorLog log) {}

    @Override
    public boolean commit() {
        return true;
    }
}
