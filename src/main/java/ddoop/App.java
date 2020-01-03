package ddoop;

import ddoop.raft.node.Node;
import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.rpc.json.JsonSerialization;
import ddoop.raft.rpc.udp.UdpRpc;
import ddoop.raft.state.mapdb.MapDbStateManager;
import ddoop.state.State;
import ddoop.util.ThreadPool;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class App {

  private static final Logger logger = LoggerFactory.getLogger(App.class);

  private static String getEnvOrDefault(String key, String defaultValue) {
    String val = System.getenv(key);

    return val != null ? val : defaultValue;
  }

  public static void main(String[] args) throws InterruptedException {


    int timeout = Integer.parseInt(getEnvOrDefault("RAFT_TIMEOUT", "250"));

    int port = Integer.parseInt(getEnvOrDefault("RAFT_PORT", "8000"));

    int httpPort = Integer.parseInt(getEnvOrDefault("API_PORT", "9000"));

    int packetSize = Integer.parseInt(getEnvOrDefault("RAFT_PACKET_SIZE", "4048"));

    NodeIdentity id = NodeIdentity.parse(System.getenv("SELF_ID"));

    String fildDbPath = getEnvOrDefault("DB_PATH", "state.db");

    List<NodeIdentity> nodes =
        Arrays.stream(System.getenv("NODES")
            .split(","))
            .map(NodeIdentity::parse)
            .collect(Collectors.toList());

    logger.trace("timeout: {}", timeout);
    logger.trace("udp port: {}", port);
    logger.trace("http port: {}", httpPort);

    logger.trace("node identity: {}", id);
    logger.trace("nodes: {}", nodes);

    ThreadPool threadPool = new ThreadPool();

    UdpRpc rpc = new UdpRpc(new JsonSerialization(), threadPool, port, packetSize);

    try (DB db = DBMaker.fileDB(fildDbPath).closeOnJvmShutdown().fileMmapEnableIfSupported().executorEnable().transactionEnable().make()) {

      logger.trace("db created");

      MapDbStateManager mapDbStateManager = new MapDbStateManager(db);
      State state = new State(db);

      mapDbStateManager.init();
      state.init();

      Node node = new Node(id, mapDbStateManager, state, nodes, rpc, threadPool, timeout);

      threadPool.onDaemonThread(() -> {
        logger.trace("Starting rpc...");
        rpc.start();
        node.start();
      });

      Vertx vertx = Vertx.vertx();

      Router router = Router.router(vertx);

      router.route().handler(BodyHandler.create());

      router.get("/print").handler(routingContext -> {
        routingContext.response().end(state.toString());
      });

      router.getWithRegex("/ls/(?<path>(([a-zA-Z0-9_-]+/)*[a-zA-Z0-9_-]+))").handler(routingContext -> {
        String path = "/" + routingContext.request().getParam("path");

        List<State.Path> result = state.list(path);

        routingContext.response().end(result.toString());
      });

      vertx.createHttpServer().requestHandler(router).listen(httpPort);
    }
  }
}
