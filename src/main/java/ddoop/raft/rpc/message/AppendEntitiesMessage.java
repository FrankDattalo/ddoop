package ddoop.raft.rpc.message;

import ddoop.raft.rpc.message.base.AbstractAppendEntitiesMessage;

public abstract class AppendEntitiesMessage implements AbstractAppendEntitiesMessage {

    public abstract void reply(long term, boolean success);
}
