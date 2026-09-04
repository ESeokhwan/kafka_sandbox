package kafka.monitor;

import java.util.Objects;

public class MonitorLog {

  private final String type;

  private final String id;

  private final String state;

  private final long timestamp;

  private final long timestampNano;

  public MonitorLog(String type, String id, String state, long timestamp, long timestampNano) {
    this.type = type;
    this.id = id;
    this.state = state;
    this.timestamp = timestamp;
    this.timestampNano = timestampNano;
  }

  public String getType() {
    return type;
  }

  public String getId() {
    return id;
  }

  public String getState() {
    return state;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getTimestampNano() {
    return timestampNano;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, id, state);
  }

  @Override
  public boolean equals(Object oth) {
    if (this == oth) return true;
    if (oth == null || getClass() != oth.getClass()) return false;
    
    MonitorLog converted = (MonitorLog) oth;
    return Objects.equals(this.type, converted.type)
            && Objects.equals(this.id, converted.id)
        && Objects.equals(this.state, converted.state);
  }

  public String toString() {
    return "MonitorLog{" +
            "type='" + type + '\'' +
            ", id='" + id + '\'' +
            ", state='" + state + '\'' +
            ", timestamp=" + timestamp +
            ", timestampNano=" + timestampNano +
            '}';
  }
}
