package ddoop;

import ddoop.raft.node.Node;
import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.rpc.Serialization;
import ddoop.raft.rpc.http.HttpRpc;
import ddoop.raft.rpc.json.JsonSerialization;
import ddoop.raft.rpc.udp.UdpRpc;
import ddoop.raft.state.StateMachineApplier;
import ddoop.raft.state.StateManager;
import ddoop.raft.state.inmemory.CommandLoggingStateMachine;
import ddoop.raft.state.inmemory.InMemoryStateManager;
import ddoop.util.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws InterruptedException {

        int timeout = Integer.parseInt(System.getenv("TIMEOUT"));

        NodeIdentity id = NodeIdentity.parse(System.getenv("SELF_ID"));

        List<NodeIdentity> nodes = Arrays.stream(System.getenv("NODES").split(","))
                .map(NodeIdentity::parse)
                .collect(Collectors.toList());
 
        logger.trace("timeout: {}", timeout);
        logger.trace("node identity: {}", id);
        logger.trace("nodes: {}", nodes);

        ThreadPool threadPool = new ThreadPool();

        Serialization serialization = new JsonSerialization();
        UdpRpc internalRpc = new UdpRpc(serialization, threadPool, id.getTransportLocation().getPort(), 4048);
        HttpRpc clientRpc = new HttpRpc(serialization, threadPool, id.getApiLocation().getPort());

        StateManager stateManager = new InMemoryStateManager();
        StateMachineApplier stateMachineApplier = new CommandLoggingStateMachine();

        Node node = new Node(id, stateManager, stateMachineApplier, nodes, threadPool, timeout, internalRpc, clientRpc);

        logger.trace("Starting rpc components");

        internalRpc.start();

        clientRpc.start();

        node.start();
    }
}
