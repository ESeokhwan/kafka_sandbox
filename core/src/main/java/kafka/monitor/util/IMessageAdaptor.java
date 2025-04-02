package kafka.monitor.util;

public interface IMessageAdaptor {
  String generate(String messageId); // TODO: 입력 받는 인자를 ByteBuffer, String을 앞에서부터 하나씩 읽어올 수 있는 wrapper로 변경하기

  String extractMessageId(String message);
}
