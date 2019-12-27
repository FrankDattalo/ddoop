package ddoop.raft.rpc.message.base;

import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.rpc.message.MessageType;

import java.util.List;

public interface AbstractAppendEntitiesMessage extends Message {

    public long getTerm();

    public NodeIdentity getLeaderId();

    public long getPrevLogIndex();

    public long getPrevLogTerm();

    public List<String> getEntities();

    public long getLeaderCommit();
    
    @Override
    default public MessageType getType() {
        return MessageType.AppendEntities;
    }
}