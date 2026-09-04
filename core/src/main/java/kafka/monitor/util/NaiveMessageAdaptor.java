package kafka.monitor.util;

public abstract class NaiveMessageAdaptor implements IMessageAdaptor {

  private final static char divChar = '!';

  protected final int messageSize;

  protected NaiveMessageAdaptor(int messageSize) {
    this.messageSize = messageSize;
  }

  abstract String getRandomPadding(int paddingSize);

  @Override
  public String generate(String messageId) {
    int paddingSize = messageSize - messageId.length() - 1;
    String padding = getRandomPadding(paddingSize);
    return messageId + divChar + padding;
  }

  @Override
  public String extractMessageId(String message) {
    int messageIdSize = message.lastIndexOf(divChar);
    if (messageIdSize < 0) {
      messageIdSize = message.length();
    }

    return message.substring(0, messageIdSize);
  }
}
