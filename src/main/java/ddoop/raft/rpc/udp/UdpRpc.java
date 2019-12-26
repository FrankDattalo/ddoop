package ddoop.raft.rpc.udp;

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
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ddoop.raft.rpc.Message;
import ddoop.raft.rpc.Rpc;
import ddoop.raft.rpc.Serialization;
import ddoop.util.ThreadPool;
import ddoop.util.ThreadPool.Task;

/**
 * Udp implementation of raft Rpc interface.
 */
public class UdpRpc implements Rpc {

    private static final Logger logger = LoggerFactory.getLogger(UdpRpc.class);

    private final BlockingQueue<Message> senderChannel = new LinkedTransferQueue<>();
    private final BlockingQueue<Message> receiverChannel = new LinkedTransferQueue<>();

    private final Serialization serialization;
    private final ThreadPool threadPool;
    private final int port;
    private final int packetSize;

    private Task senderThread;
    private Task receiverThread;
    private boolean started = false;

    /**
     * Constructs a udp rpc.
     * 
     * @param serialization The serialization protocol.
     * @param threadPool A thread pool used to instantiate threads from.
     * @param port The port to listen on locally.
     * @param packetSize The size of the byte buffer to allocate locally while listening for messages.
     */
    public UdpRpc(
            Serialization serialization,
            ThreadPool threadPool, 
            int port,
            int packetSize) {

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

        this.senderThread = this.threadPool.onDaemonThread(this::sender);

        this.receiverThread = this.threadPool.onDaemonThread(this::receiver);

        this.started = true;

        logger.trace("Started");
    }

    /**
     * Stops the background network threads if they are not already stopped.
     */
    public synchronized void stop() {
        logger.trace("stop()");

        if (!this.started) {
            logger.trace("Not stopping because bridge is already stopped");
            return;
        }

        logger.trace("Stopping...");

        if (this.senderThread != null) {
            logger.trace("Canceling sender thread");
            this.senderThread.cancel();
        }

        if (this.receiverThread != null) {
            logger.trace("Canceling receiver thread");
            this.receiverThread.cancel();
        }

        this.started = false;

        logger.trace("Stopped.");
    }

    @Override
    public void send(Message message) throws InterruptedException {
        logger.trace("Writing message to channel");
        this.senderChannel.put(message);
        logger.trace("Message written to channel");
    }

    @Override
    public Message next() throws InterruptedException {
        logger.trace("Awaiting next message...");
        Message m = this.receiverChannel.take();
        logger.trace("Got message from channel");
        return m;
    }

    /**
     * Sender network thread.
     * Will continue to send forever, pulling messages from the sender channel.
     * @return null
     * @throws InterruptedException if interrupted.
     */
    private Void sender() throws InterruptedException {

        // TODO: some nicer way of making this stop other than interrupting it?
        while (true) {

            logger.trace("Waiting for next message on channel");
            Message message = this.senderChannel.take();

            logger.trace("Pulled message from sender channel: {}", message);

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            
            if (!this.serialization.serialize(message, byteArrayOutputStream)) {
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
                
                packet.setAddress(InetAddress.getByName(message.getTo().getHost()));
                packet.setPort(message.getTo().getPort());

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
     * Receiver network thread.
     * Will continue listening for messages forever, writing them to the receiver channel.
     * @return null.
     * @throws InterruptedException If interrupted.
     */
    private Void receiver() throws InterruptedException {

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

                    logger.trace("Sending de-serialized message to channel: {}", message);

                    this.receiverChannel.put(message);

                    logger.trace("Message sent to channel");
                }
            }

        } catch (SocketException se) {
            logger.error("Unable to bind to port, stopping", se);
            this.stop();
            return null;
        }
    }
}