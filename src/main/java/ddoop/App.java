package ddoop;

import ddoop.raft.node.Node;
import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.rpc.json.JsonSerialization;
import ddoop.raft.rpc.udp.UdpRpc;
import ddoop.raft.state.inmemory.CommandLoggingStateMachine;
import ddoop.raft.state.inmemory.InMemoryStateManager;
import ddoop.state.State;
import ddoop.util.ThreadPool;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
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

        try (State state = new State("state.db")) {

            state.init();

            switch (args[0]) {
                case "ls":
                    System.out.println(state.list(args[1]));
                    return;
                case "rm":
                    state.delete(args[1]);
                    return;
                case "isdir":
                    System.out.println(state.isDirectory(args[1]));
                    return;
                case "set":
                    state.put(args[1], args[2]);
                    return;
                case "print":
                    System.out.println(state);
                    return;
                default:
                    throw new IllegalArgumentException(args[0]);
            }

        }

//
//
//        int timeout = Integer.parseInt(getEnvOrDefault("TIMEOUT", "250"));
//
//        int port = Integer.parseInt(getEnvOrDefault("PORT", "8000"));
//
//        int httpPort = Integer.parseInt(getEnvOrDefault("API_PORT", "9000"));
//
//        int packetSize = Integer.parseInt(getEnvOrDefault("PACKET_SIZE", "4048"));
//
//        NodeIdentity id = NodeIdentity.parse(System.getenv("SELF_ID"));
//
//        List<NodeIdentity> nodes =
//                Arrays.stream(System.getenv("NODES")
//                        .split(","))
//                        .map(NodeIdentity::parse)
//                        .collect(Collectors.toList());
//
//        logger.trace("timeout: {}", timeout);
//        logger.trace("udp port: {}", port);
//        logger.trace("http port: {}", httpPort);;
//        logger.trace("node identity: {}", id);
//        logger.trace("nodes: {}", nodes);
//
//        ThreadPool threadPool = new ThreadPool();
//
//        UdpRpc rpc = new UdpRpc(new JsonSerialization(), threadPool, port, packetSize);
//
//        Node node = new Node(id, new InMemoryStateManager(), new CommandLoggingStateMachine(), nodes, rpc, threadPool, timeout);
//
//        threadPool.onDaemonThread(() -> {
//            logger.trace("Starting rpc...");
//            rpc.start();
//            node.start();
//        });
//
//        Vertx vertx = Vertx.vertx();
//
//        vertx.createHttpServer().requestHandler(httpServerRequest -> {
//           httpServerRequest.bodyHandler(body -> {
//                httpServerRequest.response().end();
//
//                String bodyAsString = new String(body.getBytes(), StandardCharsets.UTF_8);
//                String uri = httpServerRequest.uri();
//
//                try {
//                    node.send(String.format("%s %s = %s", httpServerRequest.method(), uri, bodyAsString));
//
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//           });
//        }).listen(httpPort);
    }
}
