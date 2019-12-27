package ddoop.raft.rpc.message.base;

import ddoop.raft.rpc.NodeIdentity;

import java.util.Objects;

public class BindableRequestVoteMessage
    implements AbstractRequestVoteMessage, BindableMessage {

    private final NodeIdentity from;
    private final NodeIdentity to;
    private final long term;
    private final NodeIdentity candidateId;
    private final long lastLogIndex;
    private final long lastLogTerm;

    public BindableRequestVoteMessage(
            NodeIdentity from,
            NodeIdentity to,
            long term,
            NodeIdentity candidateId,
            long lastLogIndex,
            long lastLogTerm) {

        this.from = Objects.requireNonNull(from, "from cannot be null");
        this.to = Objects.requireNonNull(to, "no cannot be null");
        this.term = term;
        this.candidateId = Objects.requireNonNull(candidateId, "candidateId cannot be null");
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    @Override
    public NodeIdentity getFrom() {
        return from;
    }

    @Override
    public NodeIdentity getTo() {
        return to;
    }

    @Override
    public long getTerm() {
        return term;
    }

    @Override
    public NodeIdentity getCandidateId() {
        return candidateId;
    }

    @Override
    public long getLastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public long getLastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public String toString() {
        return "BindableRequestVoteMessage{" +
                "from=" + from +
                ", to=" + to +
                ", term=" + term +
                ", candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}
