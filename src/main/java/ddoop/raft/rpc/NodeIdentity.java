package ddoop.raft.rpc;

import java.util.Objects;

/**
 * NodeIdentity represents a hostname port pair.
 */
public class NodeIdentity {

    public static class Location {
        private final String host;
        private final int port;

        public Location(String host, int port) {
            if (port <= 0) {
                throw new IllegalArgumentException("port cannot be <= 0");
            }

            this.port = port;
            this.host = Objects.requireNonNull(host, "host cannot be null");
        }

        public static Location parse(String string) {
            String[] split = string.split(":");

            if (split.length != 2) {
                throw new IllegalArgumentException("Input string must be of the format host:port");
            }

            return new Location(split[0], Integer.parseInt(split[1]));
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Location location = (Location) o;
            return port == location.port &&
                    Objects.equals(host, location.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, port);
        }

        @Override
        public String toString() {
            return "Location{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    '}';
        }
    }

    private final Location transportLocation;
    private final Location apiLocation;

    public NodeIdentity(Location transport, Location api) {
        this.transportLocation = Objects.requireNonNull(transport, "transport cannot be null");
        this.apiLocation = Objects.requireNonNull(api, "api cannot be null");
    }

    public static NodeIdentity parse(String str) {
        String[] split = str.split("/");

        if (split.length != 2) {
            throw new IllegalArgumentException("string must be of format host:port/host:port");
        }

        return new NodeIdentity(
                Location.parse(split[0]),
                Location.parse(split[1])
        );
    }

    public Location getTransportLocation() {
        return transportLocation;
    }

    public Location getApiLocation() {
        return apiLocation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeIdentity that = (NodeIdentity) o;
        return Objects.equals(transportLocation, that.transportLocation) &&
                Objects.equals(apiLocation, that.apiLocation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transportLocation, apiLocation);
    }

    @Override
    public String toString() {
        return "NodeIdentity{" +
                "transportLocation=" + transportLocation +
                ", apiLocation=" + apiLocation +
                '}';
    }
}