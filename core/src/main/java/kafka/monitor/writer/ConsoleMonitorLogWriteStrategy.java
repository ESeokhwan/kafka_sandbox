package kafka.monitor.writer;

import kafka.monitor.MonitorLog;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class ConsoleMonitorLogWriteStrategy implements IMonitorLogWriteStrategy {

    private final boolean needPrettier;

    private final boolean needNanoTime;

    private final Logger logger;

    public ConsoleMonitorLogWriteStrategy(LogContext logContext, boolean needPrettier, boolean needNanoTime) {
        this.logger = logContext.logger(ConsoleMonitorLogWriteStrategy.class);

        this.needPrettier = needPrettier;
        this.needNanoTime = needNanoTime;
    }

    @Override
    public void write(MonitorLog log) {
        if (needNanoTime) {
            logger.info("MonitorLog: Type: {}, Id: {}, State: {}, Timestamp: {}, TimestampNano: {}",
                    log.getType(), log.getId(), log.getState(), prettierTimestamp(log.getTimestamp()), prettierTimestampNano(log.getTimestampNano()));
            return;
        }
        logger.info("MonitorLog: Type: {}, Id: {}, Timestamp: {}, State: {}",
                log.getType(), log.getId(), log.getState(), prettierTimestamp(log.getTimestamp()));
    }

    private String prettierTimestamp(long timestamp) {
        if (needPrettier) {
            return String.format("%1$tY-%1$tm-%1$tdT%1$tH:%1$tM:%1$tS.%1$tL", timestamp);
        } else {
            return String.valueOf(timestamp);
        }
    }

    private String prettierTimestampNano(long timestampNano) {
        if (needPrettier) {
            return String.valueOf(timestampNano % 1_000_000);
        } else {
            return String.valueOf(timestampNano);
        }
    }

    @Override
    public boolean commit() {
        return true;
    }
}
