package kafka.monitor.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class EfficientMessageGenerator extends NaiveMessageAdaptor {

  private final static String paddingCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  private final static int UNIT_DOWNGRADE_FACTOR = 10;

  private List<String> paddingUnits;

  private final int paddingUnitSize;

  public EfficientMessageGenerator(int messageSize, int paddingUnitSize) {
    super(messageSize);
    this.paddingUnitSize = paddingUnitSize;
    init();
  }

  private void init() {
    Random random = new Random();
    paddingUnits = new ArrayList<>();
    int curUnitSize = paddingUnitSize;
    while (curUnitSize > 0) {
      StringBuilder builder = new StringBuilder(curUnitSize);
      for (int i = 0; i < curUnitSize; i++) {
        builder.append(paddingCharacters.charAt(random.nextInt(paddingCharacters.length())));
      }
      paddingUnits.add(builder.toString());
      curUnitSize /= UNIT_DOWNGRADE_FACTOR;
    }
  }

  @Override
  protected String getRandomPadding(int size) {
    StringBuilder builder = new StringBuilder(messageSize);

    int curUnitSize = paddingUnitSize;
    int curPaddingUnitIdx = 0;
    while (size > 0 && curUnitSize > 0) {
      while (size >= curUnitSize) {
        builder.append(paddingUnits.get(curPaddingUnitIdx));
        size -= curUnitSize;
      }
      curPaddingUnitIdx += 1;
      curUnitSize /= UNIT_DOWNGRADE_FACTOR;
    }

    return builder.toString();
  }
}
