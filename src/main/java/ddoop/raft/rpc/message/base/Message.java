package ddoop.raft.rpc.message.base;

import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.rpc.message.MessageType;

public interface Message {
    
    public NodeIdentity getFrom();

    public NodeIdentity getTo();
    
    public MessageType getType();
    
}