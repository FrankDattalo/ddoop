package ddoop.raft.rpc.http;

import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.rpc.Rpc;
import ddoop.raft.rpc.Serialization;
import ddoop.raft.rpc.message.ClientCommandMessage;
import ddoop.raft.rpc.message.ClientCommandResultMessage;
import ddoop.raft.rpc.message.base.*;
import ddoop.util.ThreadPool;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

public class HttpRpc implements Rpc {

    private static final Logger logger = LoggerFactory.getLogger(HttpRpc.class);

    private final Serialization serialization;
    private final ThreadPool threadPool;
    private final int port;

    private final BlockingQueue<Message> messages = new LinkedTransferQueue<>();

    private boolean started = false;

    public HttpRpc(Serialization serialization, ThreadPool threadPool, int port) {
        this.threadPool = threadPool;
        this.port = port;
        this.serialization = serialization;
    }

    public synchronized void start() {
        if (this.started) {
            logger.trace("Not starting http rpc because it is already started");
            return;
        }

        logger.trace("Starting http rpc...");

        this.threadPool.onDaemonThread(this::backgroundThread);

        this.started = true;

        logger.trace("Started http rpc");
    }

    private void backgroundThread() {
        Vertx vertx = Vertx.vertx();

        HttpServer httpServer = vertx.createHttpServer();

        httpServer.requestHandler(httpServerRequest -> {
            httpServerRequest.bodyHandler(buffer -> {
                logger.trace("Got message: {}", buffer);

                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer.getBytes());
                Message message = this.serialization.deSerialize(byteArrayInputStream);

                if (message == null) {
                    logger.error("Could not decode body");
                    httpServerRequest.response().setStatusCode(400);
                    httpServerRequest.response().end();
                    return;
                }

                logger.trace("de-serialized message: {}", message);

                if (!(message instanceof BindableMessage)) {
                    logger.trace("Got non-bindable message in router, ending response immediately");
                    httpServerRequest.response().end();
                } else {

                    logger.trace("Binding message to http rpc");
                    message = bind((BindableMessage) message, httpServerRequest.response());

                    if (message == null) {
                        logger.error("Could not bind message in http rpc");
                        httpServerRequest.response().setStatusCode(500);
                        httpServerRequest.response().end();
                        return;
                    }
                }

                logger.trace("Writing message to channel");

                try {
                    this.messages.put(message);

                } catch (InterruptedException e) {
                    logger.trace("Got interrupted with communicating message", e);
                }
            });
        });

        logger.trace("Listening on port: {}", this.port);

        httpServer.listen(this.port);
    }

    private ReplyableMessage<?> bind(BindableMessage message, HttpServerResponse response) {
        logger.trace("bind({})", message);

        switch (message.getType()) {
            case ClientCommand: return new BoundClientCommandMessage((BindableClientCommandMessage) message, response);
            // TODO: other bindable message types
            default:
                logger.error("Unknown bindable type: {}", message);
                return null;
        }
    }

    private void reply(Message message, HttpServerResponse response) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        if (!this.serialization.serialize(message, byteArrayOutputStream)) {
            logger.error("Could not serialize response");
            response.setStatusCode(500);
            response.end();
            return;
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Http rpc serialized output: {}", new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8));
        }

        response.end(Buffer.buffer(byteArrayOutputStream.toByteArray()));
    }

    @Override
    public void send(NodeIdentity.Location location, BindableAppendEntitiesMessage m) throws InterruptedException {
        logger.trace("send({}, {})", location, m);
    }

    @Override
    public void send(NodeIdentity.Location location, BindableClientCommandMessage m) throws InterruptedException {
        logger.trace("send({}, {})", location, m);
    }

    @Override
    public void send(NodeIdentity.Location location, BindableRequestVoteMessage m) throws InterruptedException {
        logger.trace("send({}, {})", location, m);
    }

    @Override
    public Message next() throws InterruptedException {
        logger.trace("Awaiting next message on http rpc message channel");
        Message m = this.messages.take();
        logger.trace("Next message received from channel");
        return m;
    }

    private class BoundClientCommandMessage extends ClientCommandMessage {

        private final BindableClientCommandMessage base;
        private final HttpServerResponse httpServerResponse;

        public BoundClientCommandMessage(BindableClientCommandMessage base, HttpServerResponse httpServerResponse) {
            this.base = base;
            this.httpServerResponse = httpServerResponse;
        }

        @Override
        public String getData() {
            return base.getData();
        }

        @Override
        public void reply(NodeIdentity.Location location, ClientCommandResultMessage response) throws InterruptedException {
            HttpRpc.this.reply(response, httpServerResponse);
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
                    "} " + super.toString();
        }
    }
}
