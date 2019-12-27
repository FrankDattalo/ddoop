package ddoop.raft.rpc.message.base;

import ddoop.raft.rpc.NodeIdentity;

import java.util.Objects;

public class BindableClientCommandMessage
        implements AbstractClientCommandMessage, BindableMessage {

    private final NodeIdentity from;
    private final NodeIdentity to;
    private final String data;

    public BindableClientCommandMessage(NodeIdentity from, NodeIdentity to, String data) {
        this.from = from;
        this.to = to;
        this.data = Objects.requireNonNull(data, "data cannot be null");
    }

    @Override
    public NodeIdentity getFrom() {
        return from;
    }

    @Override
    public NodeIdentity getTo() {
        return to;
    }

    @Override
    public String getData() {
        return data;
    }

    @Override
    public String toString() {
        return "BindableClientCommandMessage{" +
                "from=" + from +
                ", to=" + to +
                ", data='" + data + '\'' +
                '}';
    }
}
