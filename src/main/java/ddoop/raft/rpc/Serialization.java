package ddoop.raft.rpc;

import ddoop.raft.rpc.message.base.Message;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Defines how messages are serialized and de-serialized during rpc network calls.
 */
public interface Serialization {
    
    /**
     * Serializes the message to the output stream, returning true if successful.
     * @param message The message to serialize.
     * @param outputStream The output stream to write to.
     * @return true if successful.
     */
    public boolean serialize(Message message, OutputStream outputStream);

    /**
     * DeSerializes the input stream to a message.
     * @param message The serialized input stream.
     * @return A de-serialized message or null.
     */
    public Message deSerialize(InputStream message);
} 