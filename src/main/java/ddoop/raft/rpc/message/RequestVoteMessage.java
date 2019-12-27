package ddoop.raft.rpc.message;

import ddoop.raft.rpc.message.base.AbstractRequestVoteMessage;

public abstract class RequestVoteMessage implements AbstractRequestVoteMessage {

    public abstract void reply(long term, boolean success);
}
