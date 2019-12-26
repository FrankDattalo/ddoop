package ddoop.raft.rpc;

import java.util.Objects;

/**
 * NodeIdentity represents a hostname port pair.
 */
public class NodeIdentity {
    private final String host;
    private final int port;

    public NodeIdentity(String host, int port) {
        if (port <= 0) {
            throw new IllegalArgumentException("port cannot be <= 0");
        }

        this.host = Objects.requireNonNull(host, "host cannot be null");
        this.port = port;
    }

    public static NodeIdentity parse(String str) {
        String[] split = str.split(":");

        if (split.length != 2) {
            throw new IllegalArgumentException("string must be of format host:port");
        }

        return new NodeIdentity(split[0], Integer.parseInt(split[1]));
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof NodeIdentity)) {
            return false;
        }
        NodeIdentity nodeIdentity = (NodeIdentity) o;
        return Objects.equals(host, nodeIdentity.host) && port == nodeIdentity.port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
    
}