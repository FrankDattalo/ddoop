package ddoop.state;

public class StateTest {
  public static void main(String[] args) {
    try (State state = new State("state.db")) {

      state.init();

      System.out.println(state);

      switch (args[0]) {
        case "ls":
          state.list(args[1]);
          return;
        case "rm":
          state.delete(args[1]);
          return;
        case "isdir":
          state.isDirectory(args[1]);
          return;
        case "set":
          state.put(args[1], args[2]);
          return;
        default:
          throw new IllegalArgumentException(args[0]);
      }

    }
  }
}
