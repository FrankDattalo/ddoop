package ddoop.raft.rpc;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Rpc Message used to communicate amongst the cluster.
 */
public abstract class Message {

    public static enum MessageType {
        AppendEntities, AppendEntitiesResult, RequestVote, RequestVoteResult, ClientCommand, ClientCommandResult
    }

    private final NodeIdentity from;
    private final NodeIdentity to;
    private final MessageType  type;

    /**
     * Constructs a message.
     * @param from The node which is sending the message.
     * @param to The node to recieve the message.
     */
    public Message(NodeIdentity from, NodeIdentity to, MessageType messageType) {
        this.from = Objects.requireNonNull(from, "from cannot be null");
        this.to = Objects.requireNonNull(to, "to cannot be null");
        this.type = Objects.requireNonNull(messageType, "message type cannot be null");
    }

    public NodeIdentity getFrom() {
        return from;
    }

    public NodeIdentity getTo() {
        return to;
    }

    public MessageType getMessageType() {
        return type;
    }

    public void reply(Message replyMessage) {

    }

    private static abstract class Result extends Message {

        private final long term;
        private final boolean success;

        public Result(
                NodeIdentity from, 
                NodeIdentity to, 
                MessageType type,
                long term, 
                boolean success) {

            super(from, to, type);
            this.term = term;
            this.success = success;
        }

        public long getTerm() {
            return term;
        }

        public boolean getSuccess() {
            return success;
        }
    }

    private static class ClientMessage extends Message {

        private final String message;

        private ClientMessage(
                NodeIdentity from, 
                NodeIdentity to, 
                MessageType type,
                String message) {

            super(from, to, type);
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }

    public static class ClientCommand extends ClientMessage {

        public ClientCommand(
                NodeIdentity from, 
                NodeIdentity to, 
                String command) {

            super(from, to, MessageType.ClientCommand, command);
        }

        @Override
        public String toString() {
            return "ClientCommand{" +
                "from=" + getFrom() +
                ", to=" + getTo() +
                ", type=" + getMessageType() +
                ", message=" + getMessage() +
                '}';
        }
    }

    public static class ClientCommandResult extends ClientMessage {

        public ClientCommandResult(
                NodeIdentity from, 
                NodeIdentity to, 
                String message) {

            super(from, to, MessageType.ClientCommandResult, message);
        }

        @Override
        public String toString() {
            return "ClientCommandResult{" +
                "from=" + getFrom() +
                ", to=" + getTo() +
                ", type=" + getMessageType() +
                ", message=" + getMessage() +
                '}';
        }
    }

    /**
     * Message containing AppendEntities result
     */
    public static class AppendEntitiesResult extends Result {

        public AppendEntitiesResult(NodeIdentity from, NodeIdentity to, long term, boolean success) {
            super(from, to, MessageType.AppendEntitiesResult, term, success);
        }

        @Override
        public String toString() {
            return "AppendEntitiesResult{" +
                "from=" + getFrom() +
                ", to=" + getTo() +
                ", type=" + getMessageType() +
                ", term=" + getTerm() +
                ", success=" + getSuccess() +
                '}';
        }
    }

    /**
     * Message containing RequestVote result
     */
    public static class RequestVoteResult extends Result {

        public RequestVoteResult(NodeIdentity from, NodeIdentity to, long term, boolean success) {
            super(from, to, MessageType.RequestVoteResult, term, success);
        }

        @Override
        public String toString() {
        return "RequestVoteResult{" +
            "from=" + getFrom() +
            ", to=" + getTo() +
            ", type=" + getMessageType() +
            ", term=" + getTerm() +
            ", success=" + getSuccess() +
            '}';
        }
    }

    /**
     * Append entities message.
     */
    public static class AppendEntities extends Message {

        private final long term;
        private final NodeIdentity leaderId;
        private final long prevLogIndex;
        private final long prevLogTerm;
        private final List<String> entities;
        private final long leaderCommit;

        public AppendEntities(
                NodeIdentity from, 
                NodeIdentity to, 
                long term, 
                NodeIdentity leaderId, 
                long prevLogIndex, 
                long prevLogTerm, 
                List<String> entities, 
                long leaderCommit) {

            super(from, to, MessageType.AppendEntities);
            this.term = term;
            this.leaderId = Objects.requireNonNull(leaderId, "leaderId cannot be null");
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.entities = Collections.unmodifiableList(Objects.requireNonNull(entities, "entities cannot be null"));
            this.leaderCommit = leaderCommit;
        }

        public long getTerm() {
            return term;
        }

        public NodeIdentity getLeaderId() {
            return leaderId;
        }

        public long getPrevLogIndex() {
            return prevLogIndex;
        }

        public long getPrevLogTerm() {
            return prevLogTerm;
        }

        public List<String> getEntities() {
            return entities;
        }

        public long getLeaderCommit() {
            return leaderCommit;
        }

        @Override
        public String toString() {
        return "AppendEntities{" +
            "from=" + getFrom() +
            ", to=" + getTo() +
            ", type=" + getMessageType() +
            ", term=" + term +
            ", leaderId=" + leaderId +
            ", prevLogIndex=" + prevLogIndex +
            ", prevLogTerm=" + prevLogTerm +
            ", entities=" + entities +
            ", leaderCommit=" + leaderCommit +
            '}';
        }
    }

    /**
     * Request vote message.
     */
    public static class RequestVote extends Message {

        private final long term;
        private final NodeIdentity candidateId;
        private final long lastLogIndex;
        private final long lastLogTerm;

        public RequestVote(
                NodeIdentity from, 
                NodeIdentity to, 
                long term, 
                NodeIdentity candidateId, 
                long lastLogIndex, 
                long lastLogTerm) {

            super(from, to, MessageType.RequestVote);

            this.term = term;
            this.candidateId = Objects.requireNonNull(candidateId, "candidateId cannot be null");
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }

        public long getTerm() {
            return term;
        }

        public NodeIdentity getCandidateId() {
            return candidateId;
        }

        public long getLastLogIndex() {
            return lastLogIndex;
        }

        public long lastLogTerm() {
            return lastLogTerm;
        }

        @Override
        public String toString() {
        return "RequestVote{" +
            "from=" + getFrom() +
            ", to=" + getTo() +
            ", type=" + getMessageType() +
            ", term=" + term +
            ", candidateId=" + candidateId +
            ", lastLogIndex=" + lastLogIndex +
            ", lastLogTerm=" + lastLogTerm +
            '}';
        }
    }
}