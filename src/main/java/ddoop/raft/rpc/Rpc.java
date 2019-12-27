package ddoop.raft.rpc;

import ddoop.raft.rpc.message.base.BindableAppendEntitiesMessage;
import ddoop.raft.rpc.message.base.BindableClientCommandMessage;
import ddoop.raft.rpc.message.base.BindableRequestVoteMessage;
import ddoop.raft.rpc.message.base.Message;

/**
 * Remote Procedure Call interface for Raft.
 */
public interface Rpc {

    public void send(NodeIdentity.Location location, BindableAppendEntitiesMessage m) throws InterruptedException;

    public void send(NodeIdentity.Location location, BindableClientCommandMessage m) throws InterruptedException;

    public void send(NodeIdentity.Location location, BindableRequestVoteMessage m) throws InterruptedException;

    /**
     * Blocks until the next message is recieved.
     * @return The next message.
     * @throws InterruptedException If interrupted.
     */
    public Message next() throws InterruptedException;

}