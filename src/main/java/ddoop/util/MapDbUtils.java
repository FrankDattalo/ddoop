package ddoop.util;

public class MapDbUtils {

  public static String nameForElement(Class<?> clazz, String name) {
    return String.format("%s.%s", clazz.getName(), name);
  }
}
