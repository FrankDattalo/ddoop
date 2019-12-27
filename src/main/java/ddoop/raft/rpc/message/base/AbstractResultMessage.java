package ddoop.raft.rpc.message.base;

import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.rpc.message.MessageType;

public abstract class AbstractResultMessage<T> implements Message {

    private final NodeIdentity from;
    private final NodeIdentity to;
    private final MessageType type;
    private final T data;
    private final boolean success;

    public AbstractResultMessage(NodeIdentity from, NodeIdentity to, MessageType type, T data, boolean success) {
        this.from = from;
        this.to = to;
        this.type = type;
        this.data = data;
        this.success = success;
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
    public MessageType getType() {
        return type;
    }

    public T getData() {
        return data;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public String toString() {
        return "AbstractResultMessage{" +
                "from=" + from +
                ", to=" + to +
                ", type=" + type +
                ", data=" + data +
                ", success=" + success +
                '}';
    }
}
