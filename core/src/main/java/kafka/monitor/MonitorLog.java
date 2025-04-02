package kafka.monitor;

import java.util.Objects;

public class MonitorLog {
  public enum RequestType {
    PRODUCE, CONSUME
  }

  private final RequestType type;

  private final String messageId;

  private final long timestamp;

  private final long timestampNano;
  
  private final String state;
  
  public MonitorLog(RequestType type, String messageId, long timestamp, long timestampNano, String state) {
    this.type = type;
    this.messageId = messageId;
    this.timestamp = timestamp;
    this.timestampNano = timestampNano;
    this.state = state;
  }

  public RequestType getType() {
    return type;
  }
  
  public String getMessageId() {
    return messageId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getTimestampNano() {
    return timestampNano;
  }

  public String getState() {
    return state;
  }

  public MonitorLog withMessageId(String messageId) {
    return new MonitorLog(type, messageId, timestamp, timestampNano, state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, messageId, state);
  }

  @Override
  public boolean equals(Object oth) {
    if (this == oth) return true;
    if (oth == null || getClass() != oth.getClass()) return false;
    
    MonitorLog converted = (MonitorLog) oth;
    return Objects.equals(this.type, converted.type)
        && Objects.equals(this.messageId, converted.messageId)
        && Objects.equals(this.state, converted.state);
  }

  public final static String REQUESTED = "REQUESTED";
  public final static String BROKER_RECEIVED = "BROKER_RECEIVED";
  public final static String ENQUEUED_REQUEST_QUEUE = "ENQUEUED_REQUEST_QUEUE";
  public final static String COMMITED = "COMMITED";
  public final static String RESPONDED = "RESPONDED";
}
