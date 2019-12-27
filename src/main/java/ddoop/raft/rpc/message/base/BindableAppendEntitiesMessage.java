package ddoop.raft.rpc.message.base;

import ddoop.raft.rpc.NodeIdentity;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class BindableAppendEntitiesMessage
        implements AbstractAppendEntitiesMessage, BindableMessage {

    private final NodeIdentity from;
    private final NodeIdentity to;
    private final long term;
    private final NodeIdentity leaderId;
    private final long prevLogIndex;
    private final long prevLogTerm;
    private final List<String> entities;
    private final long leaderCommit;

    public BindableAppendEntitiesMessage(
            NodeIdentity from,
            NodeIdentity to,
            long term,
            NodeIdentity leaderId,
            long prevLogIndex,
            long prevLogTerm,
            List<String> entities,
            long leaderCommit) {

        this.from = Objects.requireNonNull(from, "from cannot be null");
        this.to = Objects.requireNonNull(to, "to cannot be null");
        this.term = term;
        this.leaderId = Objects.requireNonNull(leaderId,"leaderId cannot be null");
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entities = Collections.unmodifiableList(Objects.requireNonNull(entities, "entities cannot be null"));
        this.leaderCommit = leaderCommit;
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
    public NodeIdentity getLeaderId() {
        return leaderId;
    }

    @Override
    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    @Override
    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    @Override
    public List<String> getEntities() {
        return entities;
    }

    @Override
    public long getLeaderCommit() {
        return leaderCommit;
    }

    @Override
    public String toString() {
        return "BindableAppendEntitiesMessage{" +
                "from=" + from +
                ", to=" + to +
                ", term=" + term +
                ", leaderId=" + leaderId +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", entities=" + entities +
                ", leaderCommit=" + leaderCommit +
                '}';
    }
}