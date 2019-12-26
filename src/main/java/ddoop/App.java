package ddoop;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors; 

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ddoop.raft.node.Node;
import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.rpc.json.JsonSerialization;
import ddoop.raft.rpc.udp.UdpRpc;
import ddoop.raft.state.inmemory.CommandLoggingStateMachine;
import ddoop.raft.state.inmemory.InMemoryStateManager;
import ddoop.util.ThreadPool;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;

public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws InterruptedException {

        int timeout = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);
        int httpPort = Integer.parseInt(args[2]);
        NodeIdentity id = NodeIdentity.parse(args[3]);
        List<NodeIdentity> nodes = Arrays.asList(args[4].split(",")).stream().map(NodeIdentity::parse)
                .collect(Collectors.toList());
 
        logger.debug("timeout: {}", timeout);
        logger.debug("udp port: {}", port);
        logger.debug("http port: {}", httpPort);
        logger.debug("node identity: {}", id);
        logger.debug("nodes: {}", nodes);

        ThreadPool threadPool = new ThreadPool();

        UdpRpc rpc = new UdpRpc(new JsonSerialization(), threadPool, port, 4048);
        
        Node node = new Node(id, new InMemoryStateManager(), new CommandLoggingStateMachine(), nodes, rpc, threadPool, timeout);

        threadPool.onDaemonThread(() -> {
            try {
                rpc.start();
                node.start();

            } finally {
                rpc.stop();
            }
        });

        HttpServer httpServer = Vertx.factory.vertx().createHttpServer();

        httpServer.requestHandler(request -> {

            String uri = request.path();
            String method = request.method().name();
            String value = request.getParam("value");

            try {
                String command = String.format("%s %s = %s", method, uri, value); 

                logger.debug("Sending command: {}", command);

                node.send(command);

            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();

                request.response().setStatusCode(500);
                request.response().end("error");
                return;
            }

            request.response().setStatusCode(200);
            request.response().end("ok");
        });

        httpServer.listen(httpPort);

        
    }
}
