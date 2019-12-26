package ddoop.raft.rpc;

/**
 * Remote Procedure Call interface for Raft.
 */
public interface Rpc {

    /**
     * Blocks until the message can be sent, then sends the message.
     * @param message The message to send.
     * @throws InterruptedException If interrupted.
     */
    public void send(Message message) throws InterruptedException;

    /**
     * Blocks until the next message is recieved.
     * @return The next message.
     * @throws InterruptedException If interrupted.
     */
    public Message next() throws InterruptedException;

    /**
     * Replies to the original message.
     * 
     * @param original The original message.
     * @param reply The response message.
     * 
     * @throws InterruptedException If interrupted.
     */
    // public void reply(Message original, Message reply) throws InterruptedException;
}