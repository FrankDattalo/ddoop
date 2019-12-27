package ddoop.raft.rpc.message.base;

import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.rpc.message.MessageType;

public interface AbstractRequestVoteMessage extends Message {

    public long getTerm();

    public NodeIdentity getCandidateId();

    public long getLastLogIndex();

    public long getLastLogTerm();

    @Override
    default public MessageType getType() {
        return MessageType.RequestVote;
    }
}
