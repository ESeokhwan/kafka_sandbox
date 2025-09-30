package kafka.monitor.util;

public class ExtractOnlyNaiveMessageAdaptor extends NaiveMessageAdaptor {

  public ExtractOnlyNaiveMessageAdaptor(int messageSize) {
    super(messageSize);
  }

  @Override
  protected String getRandomPadding(int size) {
    throw new RuntimeException("This class is not for generating message. It is just for extracting message.");
  }
}
