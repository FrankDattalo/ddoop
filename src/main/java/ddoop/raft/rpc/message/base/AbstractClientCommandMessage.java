package ddoop.raft.rpc.message.base;

import ddoop.raft.rpc.message.MessageType;

public interface  AbstractClientCommandMessage extends Message {

    public String getData();

    @Override
    default public MessageType getType() {
        return MessageType.ClientCommand;
    }
}
