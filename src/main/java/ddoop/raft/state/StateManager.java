package ddoop.raft.state;

import java.util.Objects;

import ddoop.raft.rpc.NodeIdentity;

/**
 * Interface for interacting with persistent state.
 */
public interface StateManager {

    /**
     * @return the current term for this node.
     */
    public long getCurrentTerm();

    /**
     * Sets the current term for this node.
     * @param term the term to set.
     */
    public void setCurrentTerm(long term);

    /**
     * @return who this node voted for in the current term or null if
     * no vote was made this term.
     */
    public NodeIdentity getVotedFor();

    /**
     * Votes for the given node this term.
     * @param node the node to vote for.
     */
    public void setVotedFor(NodeIdentity node);

    /**
     * @return the last log index (1 based indexing).
     */
    public long lastLogIndex();

    /**
     * @return the last log term.
     */
    public long lastLogTerm();

    /**
     * @param index the index of the log to get. (1 based indexing)
     * @return The log entry at that index at that index or null if one does not exist.
     */
    public LogEntry getLogEntry(long index);

    /**
     * Drops all logs on and after the provided index.
     * @param fromAndAfterIndex The index to drop from.
     */
    public void dropLogs(long fromAndAfterIndex);

    /**
     * Appends the entry at the given index with the given term and
     * entity if it is not already present.
     * 
     * @param index The index of the log.
     * @param term The term of the log at that index.
     * @param entity The entry of the log at that index.
     */
    public void appendEntity(long index, long term, String entity);

    /**
     * Represents a log entry at a given index.
     */
    public static class LogEntry {
        private final long term;
        private final String entity;

        /**
         * Constructs a log entry.
         * @param term The term of the log entry.
         * @param entity The value of the log entry.
         */
        public LogEntry(long term, String entity) {
            this.term = term;
            this.entity = Objects.requireNonNull(entity, "entity cannot be null");
        }

        public long getTerm() {
            return term;
        }

        public String getEntity() {
            return entity;
        }
    }
}