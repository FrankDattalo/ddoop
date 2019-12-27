package ddoop.raft.rpc.message;

import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.rpc.message.base.AbstractResultMessage;
import ddoop.raft.rpc.message.base.Message;

public class AppendEntitiesResultMessage extends AbstractResultMessage<Long> implements Message {

    public AppendEntitiesResultMessage(NodeIdentity from, NodeIdentity to, long term, boolean success) {
        super(from, to, MessageType.AppendEntitiesResult, term, success);
    }

    public long getTerm() {
        return this.getData();
    }
}
