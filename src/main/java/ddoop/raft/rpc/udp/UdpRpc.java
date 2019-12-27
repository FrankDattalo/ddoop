package ddoop.raft.rpc.udp;

import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.rpc.Rpc;
import ddoop.raft.rpc.Serialization;
import ddoop.raft.rpc.message.*;
import ddoop.raft.rpc.message.base.*;
import ddoop.util.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

/**
 * Udp implementation of raft Rpc interface.
 */
public class UdpRpc implements Rpc {

    private static final Logger logger = LoggerFactory.getLogger(UdpRpc.class);

    private final BlockingQueue<MessageWithLocation> senderChannel = new LinkedTransferQueue<>();
    private final BlockingQueue<Message> receiverChannel = new LinkedTransferQueue<>();

    private final Serialization serialization;
    private final ThreadPool threadPool;
    private final int port;
    private final int packetSize;

    private boolean started = false;

    /**
     * Constructs a udp rpc.
     * 
     * @param serialization The serialization protocol.
     * @param threadPool    A thread pool used to instantiate threads from.
     * @param port          The port to listen on locally.
     * @param packetSize    The size of the byte buffer to allocate locally while
     *                      listening for messages.
     */
    public UdpRpc(Serialization serialization, ThreadPool threadPool, int port, int packetSize) {

        this.serialization = Objects.requireNonNull(serialization, "serialization cannot be null");
        this.threadPool = threadPool;
        this.port = port;
        this.packetSize = packetSize;
    }

    /**
     * Starts the background network threads if they are not already started.
     */
    public synchronized void start() {
        logger.trace("start()");

        if (this.started) {
            logger.trace("Not starting, because bridge is already started");
            return;
        }

        logger.trace("Starting...");

        this.threadPool.onDaemonThread(this::sender);

        this.threadPool.onDaemonThread(this::receiver);

        this.started = true;

        logger.trace("Started");
    }

    private void send(NodeIdentity.Location location, Message message) throws InterruptedException {
        logger.trace("Writing message to channel: {}", message);
        MessageWithLocation messageWithLocation = new MessageWithLocation();
        messageWithLocation.location = location;
        messageWithLocation.message = message;
        this.senderChannel.put(messageWithLocation);
        logger.trace("Message written to channel");
    }

    @Override
    public void send(NodeIdentity.Location location, BindableAppendEntitiesMessage m) throws InterruptedException {
        this.send(location, (Message) m);
    }

    @Override
    public void send(NodeIdentity.Location location, BindableClientCommandMessage m) throws InterruptedException {
        this.send(location, (Message) m);
    }

    @Override
    public void send(NodeIdentity.Location location, BindableRequestVoteMessage m) throws InterruptedException {
        this.send(location, (Message) m);
    }

    @Override
    public Message next() throws InterruptedException {
        logger.trace("Awaiting next message...");
        Message m = this.receiverChannel.take();
        logger.trace("Got message from channel: {}", m);
        return m;
    }

    /**
     * Sender network thread. Will continue to send forever, pulling messages from
     * the sender channel.
     * 
     * @return null
     * @throws InterruptedException if interrupted.
     */
    private Void sender() throws InterruptedException {

        // TODO: some nicer way of making this stop other than interrupting it?
        while (true) {

            logger.trace("Waiting for next message on channel");
            MessageWithLocation message = this.senderChannel.take();

            logger.trace("Pulled message from sender channel: {}", message);

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

            if (!this.serialization.serialize(message.message, byteArrayOutputStream)) {
                logger.error("Could not serialize message, ignoring");
                continue;
            }

            logger.trace("Message serialized");

            byte[] buffer = byteArrayOutputStream.toByteArray();
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            if (logger.isTraceEnabled()) {
                logger.trace("Serialized message: {}", new String(buffer, StandardCharsets.UTF_8));
            }

            try {

                packet.setAddress(InetAddress.getByName(message.location.getHost()));
                packet.setPort(message.location.getPort());

                logger.trace("Sending packet");

                try (DatagramSocket socket = new DatagramSocket()) {
                    socket.send(packet);
                }

                logger.trace("Packet sent");

            } catch (IOException e) {
                logger.error("Unable to send message, ignoring", e);
                continue;
            }
        }

    }

    /**
     * Receiver network thread. Will continue listening for messages forever,
     * writing them to the receiver channel.
     * 
     * @throws InterruptedException If interrupted.
     */
    private void receiver() throws InterruptedException {

        try {
            logger.trace("listening on port {}", this.port);

            try (DatagramSocket socket = new DatagramSocket(this.port)) {

                byte[] buffer = new byte[this.packetSize];

                // TODO: some nicer way of making this stop other than interrupting it?
                while (true) {
                    Arrays.fill(buffer, (byte) 0);
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                    try {
                        logger.trace("Waiting to recieve data");

                        socket.receive(packet);

                        logger.trace("Got message");

                    } catch (IOException e) {
                        logger.error("Error receiveing message, ignoring", e);
                        continue;
                    }

                    if (logger.isTraceEnabled()) {
                        logger.trace("Incoming message: {}", new String(buffer, StandardCharsets.UTF_8));
                    }

                    InputStream inputStream = new ByteArrayInputStream(buffer);

                    logger.trace("De-Serializing message");

                    Message message = this.serialization.deSerialize(inputStream);

                    if (message == null) {
                        logger.error("Could not de-serialize message, ignoring");
                        continue;
                    }

                    if (message instanceof BindableMessage) {
                        logger.trace("binding message");
                        message = bind((BindableMessage) message);
                    }

                    logger.trace("Sending de-serialized message to channel: {}", message);

                    this.receiverChannel.put(message);

                    logger.trace("Message sent to channel");
                }
            }

        } catch (SocketException se) {
            logger.error("Unable to bind to port", se);
        }
    }

    private Message bind(BindableMessage b) {

        try {
            switch (b.getType()) {
                case ClientCommand:
                    return new BoundClientCommandMessage((BindableClientCommandMessage) b);
                case AppendEntities:
                    return new BoundAppendEntitiesMessage((BindableAppendEntitiesMessage) b);
                case RequestVote:
                    return new BoundRequestVoteMessage((BindableRequestVoteMessage) b);
                default:
                    break;
            }

            logger.error("Unknown bindable message type {}", b.getClass());
            return null;

        } catch (RuntimeException e) {
            logger.error("error binding message", e);
            return null;
        }
    }

    private class BoundClientCommandMessage extends ClientCommandMessage {

        private final BindableClientCommandMessage base;

        private BoundClientCommandMessage(BindableClientCommandMessage base) {
            this.base = base;
        }

        @Override
        public String getData() {
            return base.getData();
        }


        @Override
        public NodeIdentity getFrom() {
            return base.getFrom();
        }

        @Override
        public NodeIdentity getTo() {
            return base.getTo();
        }

        @Override
        public String toString() {
            return "BoundClientCommandMessage{" +
                    "base=" + base +
                    '}';
        }
    }

    private class BoundAppendEntitiesMessage extends AppendEntitiesMessage {

        private final BindableAppendEntitiesMessage base;

        private BoundAppendEntitiesMessage(BindableAppendEntitiesMessage base) {
            this.base = base;
        }

        @Override
        public long getTerm() {
            return base.getTerm();
        }

        @Override
        public NodeIdentity getLeaderId() {
            return base.getLeaderId();
        }

        @Override
        public long getPrevLogIndex() {
            return base.getPrevLogIndex();
        }

        @Override
        public long getPrevLogTerm() {
            return base.getPrevLogTerm();
        }

        @Override
        public List<String> getEntities() {
            return base.getEntities();
        }

        @Override
        public long getLeaderCommit() {
            return base.getLeaderCommit();
        }

        @Override
        public NodeIdentity getFrom() {
            return base.getFrom();
        }

        @Override
        public NodeIdentity getTo() {
            return base.getTo();
        }

        @Override
        public String toString() {
            return "BoundAppendEntitiesMessage{" +
                    "base=" + base +
                    '}';
        }
    }

    private class BoundRequestVoteMessage extends RequestVoteMessage {

        private final BindableRequestVoteMessage base;

        private BoundRequestVoteMessage(BindableRequestVoteMessage base) {
            this.base = base;
        }

        @Override
        public long getTerm() {
            return base.getTerm();
        }

        @Override
        public NodeIdentity getCandidateId() {
            return base.getCandidateId();
        }

        @Override
        public long getLastLogIndex() {
            return base.getLastLogIndex();
        }

        @Override
        public long getLastLogTerm() {
            return base.getLastLogTerm();
        }


        @Override
        public NodeIdentity getFrom() {
            return base.getFrom();
        }

        @Override
        public NodeIdentity getTo() {
            return base.getTo();
        }

        @Override
        public String toString() {
            return "BoundRequestVoteMessage{" +
                    "base=" + base +
                    '}';
        }

        @Override
        public void reply(long term, boolean success) {
            UdpRpc.this.send(getFrom(), (Message) new RequestVoteResultMessage(getTo(), getFrom(), term, success));
        }
    }

    private static class MessageWithLocation {
        private NodeIdentity.Location location;
        private Message message;
    }
}