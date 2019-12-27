package ddoop.raft.rpc.message;

import ddoop.raft.rpc.message.base.AbstractClientCommandMessage;

public abstract class ClientCommandMessage implements AbstractClientCommandMessage {

    public abstract void reply(String data, boolean success);
}
