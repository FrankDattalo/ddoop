package ddoop.raft.rpc.message;

import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.rpc.message.base.AbstractResultMessage;

public class RequestVoteResultMessage extends AbstractResultMessage<Long> {

    public RequestVoteResultMessage(NodeIdentity from, NodeIdentity to, long term, boolean success) {
        super(from, to, MessageType.RequestVoteResult, term, success);
    }

    public long getTerm() {
        return this.getData();
    }
}
