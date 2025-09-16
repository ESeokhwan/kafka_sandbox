package kafka.monitor;

import java.util.Objects;

public class LatencyLog {

  private final String type;

  private final String id;

  private final long latency;

  private final long latencyNano;

  public LatencyLog(String type, String id, long latency, long latencyNano) {
    this.type = type;
    this.id = id;
    this.latency = latency;
    this.latencyNano = latencyNano;
  }

  public String getType() {
    return type;
  }

  public String getId() {
    return id;
  }

  public long getLatency() {
    return latency;
  }

  public long getLatencyNano() {
    return latencyNano;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, id);
  }

  @Override
  public boolean equals(Object oth) {
    if (this == oth) return true;
    if (oth == null || getClass() != oth.getClass()) return false;
    
    LatencyLog converted = (LatencyLog) oth;
    return Objects.equals(this.type, converted.type)
            && Objects.equals(this.id, converted.id);
  }

  public String toString() {
    return "LatencyLog{" +
            "type='" + type + '\'' +
            ", id='" + id + '\'' +
            ", latency=" + latency +
            ", latencyNano=" + latencyNano +
            '}';
  }
}
