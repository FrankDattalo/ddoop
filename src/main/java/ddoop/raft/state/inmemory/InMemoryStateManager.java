package ddoop.raft.state.inmemory;

import java.util.ArrayList;

import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.state.StateManager;

public class InMemoryStateManager implements StateManager {

    private long currentTerm;
    private NodeIdentity votedFor;

    private ArrayList<LogEntry> logs = new ArrayList<>();

    @Override
    public long getCurrentTerm() {
        return currentTerm;
    }

    @Override
    public void setCurrentTerm(long term) {
        this.votedFor = null;
        this.currentTerm = term;
    }

    @Override
    public NodeIdentity getVotedFor() {
        return votedFor;
    }

    @Override
    public void setVotedFor(NodeIdentity node) {
        this.votedFor = node;
    }

    @Override
    public LogEntry getLogEntry(long index) {
        index -= 1;

        if (index >= logs.size() || index < 0) {
            return null;
        }

        return logs.get((int) index);
    }

    @Override
    public long lastLogIndex() {
        return logs.size();
    }

    @Override
    public long lastLogTerm() {
        return logs.isEmpty() ? 0 : logs.get(logs.size() - 1).getTerm();
    }

    @Override
    public void dropLogs(long fromAndAfterIndex) {
        int index = (int) (fromAndAfterIndex - 1);
        
        while (this.logs.size() > index) {
            this.logs.remove(this.logs.size() - 1);
        }
    }

    @Override
    public void appendEntity(long index, long term, String entity) {
        int i = (int) (index - 1);

        if (i < this.logs.size()) {
            return;
        }

        LogEntry entry = new LogEntry(term, entity);
        this.logs.add(entry);
    }

}