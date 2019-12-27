package ddoop.raft.node;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.stream.Collectors;

import ddoop.raft.rpc.message.*;
import ddoop.raft.rpc.message.base.BindableAppendEntitiesMessage;
import ddoop.raft.rpc.message.base.BindableRequestVoteMessage;
import ddoop.raft.rpc.message.base.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.rpc.Rpc;

import ddoop.raft.state.StateMachineApplier;
import ddoop.raft.state.StateManager;
import ddoop.raft.state.StateManager.LogEntry;
import ddoop.util.ThreadPool;
import ddoop.util.ThreadPool.Task;

/**
 * Raft node implementation.
 */
public class Node {

    private static final Logger logger = LoggerFactory.getLogger(Node.class);

    private final Rpc internalRpc;
    private final Rpc clientRpc;

    private final StateManager persisted;
    private final StateMachineApplier stateMachineApplier;
    private final ThreadPool threadPool;
    private final int timeout;
    private final NodeIdentity selfId;
    private final Collection<NodeIdentity> nodes;

    private Task electionThread;
    private Task heartbeatThread;

    private State FOLLOWER = new Follower();
    private State LEADER = new Leader();
    private State CANDIDATE = new Candidate();

    private final BlockingQueue<Message> messageChannel = new LinkedTransferQueue<>();

    private State state;

    // volatile state
    private long commitIndex;
    private long lastApplied;
    private Map<NodeIdentity, LogState> logState = new HashMap<>();

    /**
     * Constructs a raft node.
     * 
     * @param selfId What this nodes id is.
     * @param stateManager The state manager to manage this nodes persistent state.
     * @param stateMachineApplier The state machine applier which governs how to change the state of committed logs.
     * @param nodes A collection of nodes representing all nodes in the cluster.
     * @param threadPool A thread pool to construct threads upon.
     * @param timeout The timeout setting of this node.
     * @param internalRpc the internal rpc protocol
     * @param clientRpc  the rpc protocol for communicating with the client
     */
    public Node(
            NodeIdentity selfId,
            StateManager stateManager, 
            StateMachineApplier stateMachineApplier,
            Collection<NodeIdentity> nodes,
            ThreadPool threadPool, 
            int timeout,
            Rpc internalRpc,
            Rpc clientRpc) {

        this.internalRpc = internalRpc;
        this.clientRpc = clientRpc;
        this.threadPool = threadPool;
        this.timeout = timeout;
        this.selfId = selfId;
        this.nodes = nodes;
        this.persisted = stateManager;
        this.stateMachineApplier = stateMachineApplier;
    }

    /**
     * Starts the node on the current thread.
     * @throws InterruptedException If interrupted.
     */
    public void start() throws InterruptedException {

        logger.trace("Starting node...");

        this.threadPool.onDaemonThread(this::stateMachineApplierThread);
        this.threadPool.onDaemonThread(this::clientRpcThread);
        this.threadPool.onDaemonThread(this::internalRpcThread);

        this.changeState(FOLLOWER);

        while (true) {
            Message m = this.messageChannel.take();

            logger.trace("incoming message to node: {}", m);
            
            switch (m.getType()) {
                case AppendEntities: {
                    logger.trace("Handling append entities message");
                    onAppendEntitiesMessage((AppendEntitiesMessage) m);
                    break;
                }
                case AppendEntitiesResult: {
                    logger.trace("Handling append entities result message");
                    onAppendEntitiesResultMessage((AppendEntitiesResultMessage) m);
                    break;
                }
                case RequestVote: {
                    logger.trace("Handing request vote message");
                    onRequestVoteMessage((RequestVoteMessage) m);
                    break;
                }
                case RequestVoteResult: {
                    logger.trace("Handing request vote result message");
                    onRequestVoteResultMessage((RequestVoteResultMessage) m);
                    break;
                }
                case ClientCommand: {
                    logger.trace("Handing client command message");
                    onClientCommandMessage((ClientCommandMessage) m);
                    break;
                }
                case ClientCommandResult: {
                    logger.trace("Handling client command result message");
                    onClientCommandResultMessage((ClientCommandResultMessage) m);
                    break;
                }
                default:
                    logger.error("Invalid message type: {}", m.getType());
            }
        }
    }

    /**
     * Changes state from the previous state to the provided state.
     */
    private void changeState(State state) throws InterruptedException {

        logger.debug("changing state {} -> {}", this.state, state);

        if (this.state != null) {
            logger.trace("exiting previous state {}", this.state);
            this.state.onExitState();
        }
        
        this.state = state;
        
        if (this.state != null) {
            logger.trace("entering new state {}", this.state);
            this.state.onEnterState();
        }

        logger.trace("state changed");
    }

    /**
     * Cancels the election thread.
     */
    private void cancelElectionThread() {
        if (this.electionThread != null) {
            logger.trace("canceling previous election thread");
            this.electionThread.cancel();
        }
    }

    /**
     * Cancels the leader heartbeat thread.
     */
    private void cancelHeartbeatThread() {
        if (this.heartbeatThread != null) {
            logger.trace("canceling previous heartbeat thread");
            this.heartbeatThread.cancel();
        }
    }

    /**
     * Restarts the election thread.
     */
    private void restartHeartbeatThread() {
        cancelHeartbeatThread();
        this.heartbeatThread = this.threadPool.onDaemonThread(this::leaderHeartbeatThread);
        logger.trace("heartbeat thread restarted");
    }

    /**
     * Restarts the leader heartbeat thread.
     */
    private void restartElectionThread() {
        cancelElectionThread();
        this.electionThread = this.threadPool.onDaemonThread(this::electionTimeoutThread);
        logger.trace("election thread restarted");
    }

    /**
     * Given another term, steps down to follower if the other term is greater than our own.
     */
    private void checkTerm(long otherTerm) throws InterruptedException {
        if (this.persisted.getCurrentTerm() >= otherTerm) {
            logger.trace("term ok, not converting to follower");
            return;
        }

        logger.trace("discovered higher term, trying to convert to follower");

        this.persisted.setCurrentTerm(otherTerm);
        this.changeState(FOLLOWER);

        logger.trace("Converting to follower upon higher term");
    }

    /**
     * When command is issued from a client.
     */
    private synchronized void onClientCommandMessage(ClientCommandMessage m) throws InterruptedException {
        this.state.onClientCommandMessage(m);
    }

    /**
     * When an append entities message is issued from another node.
     */
    private synchronized void onAppendEntitiesMessage(AppendEntitiesMessage m) throws InterruptedException {
        this.checkTerm(m.getTerm());

        this.state.onAppendEntitiesMessage(m);
    }

    /**
     * When an append entities result message is issued from another node.
     */
    private synchronized void onAppendEntitiesResultMessage(AppendEntitiesResultMessage m) throws InterruptedException {
        this.checkTerm(m.getTerm());

        this.state.onAppendEntitiesResultMessage(m);
    }

    /**
     * When a request vote message is issued from another node.
     */
    private synchronized void onRequestVoteMessage(RequestVoteMessage m) throws InterruptedException {
        this.checkTerm(m.getTerm());

        this.state.onRequestVoteMessage(m);
    }

    /**
     * When a request vote result message is issued from another node.
     */
    private synchronized void onRequestVoteResultMessage(RequestVoteResultMessage m) throws InterruptedException {
        this.checkTerm(m.getTerm());
        
        this.state.onRequestVoteResultMessage(m);
    }

    /**
     * When a client command result is issued to this node.
     * This really shouldn't happen.
     */
    private synchronized void onClientCommandResultMessage(ClientCommandResultMessage m) throws InterruptedException {
        this.state.onClientCommandResultMessage(m);
    }

    /**
     * When the election timeout expires.
     */
    private synchronized void onElectionTimeout() throws InterruptedException {
        this.changeState(CANDIDATE);
    }

    /**
     * The leaders heartbeat thread.
     */
    private void leaderHeartbeatThread() throws InterruptedException {

        logger.trace("leader heartbeat thread started");

        while (true) {
            for (NodeIdentity other : this.nodes) {
                if (other.equals(this.selfId)) continue;

                LogState otherState = logState.get(other);

                LogEntry logEntry = this.persisted.getLogEntry(otherState.nextIndex);

                logger.trace("sending append entities to {} with log entry {}", other, logEntry);

                this.internalRpc.send(
                    other.getTransportLocation(),
                    new BindableAppendEntitiesMessage(
                        this.selfId, other,
                        this.persisted.getCurrentTerm(),
                        this.selfId,
                        otherState.nextIndex - 1,
                        logEntry == null || otherState.nextIndex == 1 ? 0 : this.persisted.getLogEntry(otherState.nextIndex - 1).getTerm(),
                        logEntry == null ? Collections.emptyList() : Collections.singletonList(logEntry.getEntity()),
                        this.commitIndex));

                logger.trace("after send to {}", other);
            }

            int sleep = this.timeout / this.nodes.size();
            logger.trace("sleeping for {}ms for heartbeat", sleep);
            
            try {
                Thread.sleep(sleep);

            } catch (InterruptedException e) {
                logger.trace("Observed interrupted exception leader heartbeat thread", e);
                throw e;
            }
        }
    }

    /**
     * The election timeout thread.
     */
    private Void electionTimeoutThread() throws InterruptedException {

        logger.trace("election timeout thread started");

        Random random = new Random();
        int timeout = random.nextInt(this.timeout * this.nodes.size()) + this.timeout;

        logger.trace("election timeout sleeping for {}ms", timeout);

        try {
            Thread.sleep(timeout);
            
        } catch (InterruptedException e) {
            logger.trace("Observed interrupted exception in election timeout thread", e);
            throw e;
        }

        logger.trace("Woke up from sleep, starting new election");

        this.threadPool.onDaemonThread(this::onElectionTimeout);

        return null;
    }

    /**
     * The state machine applier thread.
     */
    private Void stateMachineApplierThread() throws InterruptedException {

        while (true) {
            while (this.lastApplied < this.commitIndex) {
                String command = this.persisted.getLogEntry(this.lastApplied + 1).getEntity();
                logger.trace("Apply comamnd: {}", command);
                this.stateMachineApplier.apply(command);
                logger.trace("Command applied to state machine");
                this.lastApplied++;
            }

            try {
                Thread.sleep(Math.min(this.timeout / 100, 10));

            } catch (InterruptedException e) {
                logger.trace("observed interrupted exception in state machine applier thread", e);
                throw e;
            } 
        }

    }

    /**
     * Rpc thread which reads from an rpc and aggregates to the message channel.
     */
    private void rpcThread(Rpc readFrom) throws InterruptedException {
        while (true) {
            this.messageChannel.put(readFrom.next());
        }
    }

    /**
     * rpc thread which reads from the client rpc.
     */
    private void clientRpcThread() throws InterruptedException {
        rpcThread(this.clientRpc);
    }

    /**
     * rpc thread which reads from the internal rpc.
     */
    private void internalRpcThread() throws InterruptedException {
        rpcThread(this.internalRpc);
    }

    /**
     * @return A majority count of the cluster needed to achieve consensus.
     */
    private int majority() {
        return Node.this.nodes.size() / 2 + 1;
    }

    /**
     * State machine implementation of raft nodes internal 3 states:
     * follower, candidate, and leader
     */
    private static interface State {

        void onEnterState() throws InterruptedException;

        void onExitState() throws InterruptedException;

        void onAppendEntitiesMessage(AppendEntitiesMessage m) throws InterruptedException;

        void onAppendEntitiesResultMessage(AppendEntitiesResultMessage m) throws InterruptedException;

        void onRequestVoteMessage(RequestVoteMessage m) throws InterruptedException;

        void onRequestVoteResultMessage(RequestVoteResultMessage m) throws InterruptedException;

        void onClientCommandMessage(ClientCommandMessage m) throws InterruptedException;

        void onClientCommandResultMessage(ClientCommandResultMessage m) throws InterruptedException;

    }

    private class Follower implements State {

        @Override
        public void onEnterState() {
            Node.this.restartElectionThread();
        }

        @Override
        public void onExitState() {
            Node.this.cancelElectionThread();
        }

        @Override
        public void onAppendEntitiesMessage(AppendEntitiesMessage m) throws InterruptedException {
            Node.this.restartElectionThread();

            // handles the case when our term is greater than the message's term

            if (m.getTerm() < Node.this.persisted.getCurrentTerm()) {
                Node.logger.trace("on append entities term < current term, responding false");
                m.reply(m.getFrom().getTransportLocation(), new AppendEntitiesResultMessage(Node.this.selfId, m.getFrom(), Node.this.persisted.getCurrentTerm(), false));
                return;
            }

            // next two cases handle when the log at previous log entry does not match expected
            // (we're trying to find the last log which we agree upon)

            LogEntry logEntry = Node.this.persisted.getLogEntry(m.getPrevLogIndex());

            if (logEntry == null && m.getPrevLogIndex() != 0) {
                Node.logger.trace("on append entities log entry out of bounds, replying false");
                m.reply(m.getFrom().getTransportLocation(), new AppendEntitiesResultMessage(Node.this.selfId, m.getFrom(), Node.this.persisted.getCurrentTerm(), false));
                return;
            }

            if (logEntry != null && logEntry.getTerm() != m.getPrevLogTerm()) {
                Node.logger.trace("on append entities log entry prev log term does not match, dropping logs");
                m.reply(m.getFrom().getTransportLocation(), new AppendEntitiesResultMessage(Node.this.selfId, m.getFrom(), Node.this.persisted.getCurrentTerm(), false));
                return;
            }

            // handles the case where our logs differ from those of the sender
            // at this point we agree upon all other logs
            
            long logIndex = m.getPrevLogIndex() + 1;

            // if we discover an inconsistency, we drop all logs on and after that point

            logEntry = Node.this.persisted.getLogEntry(logIndex);
            if (logEntry != null && logEntry.getTerm() != m.getTerm()) {
                Node.logger.trace("detected log inconsistency, dropping all logs on and after {}", logIndex);
                Node.this.persisted.dropLogs(logIndex);
            }

            Node.logger.trace("appending entities");

            for (int i = 0; i < m.getEntities().size(); i++) {
                Node.this.persisted.appendEntity(logIndex, m.getTerm(), m.getEntities().get(i));
                logIndex++;
            }

            if (m.getLeaderCommit() > Node.this.commitIndex) {
                Node.logger.trace("advancing commit index");
                Node.this.commitIndex = Math.min(m.getLeaderCommit(), logIndex);
            }

            m.reply(m.getFrom().getTransportLocation(), new AppendEntitiesResultMessage(Node.this.selfId, m.getFrom(), Node.this.persisted.getCurrentTerm(), true));
        }

        @Override
        public void onAppendEntitiesResultMessage(AppendEntitiesResultMessage m) {
            Node.logger.trace("got append entities result as follower, ignoring");
        }

        @Override
        public void onRequestVoteMessage(RequestVoteMessage m) throws InterruptedException {
            Node.logger.trace("got request vote from {} as follower", m.getFrom());
            
            if (Node.this.persisted.getVotedFor() != null) {
                Node.logger.trace("Not granting vote to {} because we've already voted this term", m);
                m.reply(m.getFrom().getTransportLocation(), new RequestVoteResultMessage(Node.this.selfId, m.getFrom(), Node.this.persisted.getCurrentTerm(), false));
                return;
            }

            if (m.getTerm() < Node.this.persisted.getCurrentTerm()) {
                Node.logger.trace("Not granting vote to {} because our term is higher", m);
                m.reply(m.getFrom().getTransportLocation(), new RequestVoteResultMessage(Node.this.selfId, m.getFrom(), Node.this.persisted.getCurrentTerm(), false));
                return;
            }

            if (m.getTerm() < Node.this.persisted.lastLogTerm()) {
                Node.logger.trace("Not granting vote to {} because our log is more complete (our term is greater)", m);
                m.reply(m.getFrom().getTransportLocation(), new RequestVoteResultMessage(Node.this.selfId, m.getFrom(), Node.this.persisted.getCurrentTerm(), false));
                return;
            }

            if (m.getLastLogIndex() < Node.this.persisted.lastLogIndex()) {
                Node.logger.trace("Not granting vote to {} because our log is more complete (our index is greater)", m);
                m.reply(m.getFrom().getTransportLocation(), new RequestVoteResultMessage(Node.this.selfId, m.getFrom(), Node.this.persisted.getCurrentTerm(), false));
                return;
            }

            Node.logger.trace("Granting vote to {}", m);
            m.reply(m.getFrom().getTransportLocation(), new RequestVoteResultMessage(Node.this.selfId, m.getFrom(), Node.this.persisted.getCurrentTerm(), true));
        }

        @Override
        public void onRequestVoteResultMessage(RequestVoteResultMessage m) {
            Node.logger.trace("got request vote result as follower, ignoring");
        }

        @Override
        public String toString() {
            return "Follower";
        }

        @Override
        public void onClientCommandMessage(ClientCommandMessage m) throws InterruptedException {
            logger.trace("ignoring client command as follower");
            // TODO forward this to the proper node
        }

        @Override
        public void onClientCommandResultMessage(ClientCommandResultMessage m) throws InterruptedException {
            logger.trace("ignoring client command result as follower");
            // TODO Auto-generated method stub

        }
    }

    private class Leader implements State {

        @Override
        public void onEnterState() {
            Node.this.logState.clear();

            for (NodeIdentity other : Node.this.nodes) {
                if (other.equals(Node.this.selfId)) continue;

                LogState logState = new LogState();
                logState.nextIndex = Node.this.persisted.lastLogIndex() + 1;
                logState.matchIndex = 0;

                Node.this.logState.put(other, logState);
            }

            Node.this.restartHeartbeatThread();
        }

        @Override
        public void onExitState() {
            Node.this.cancelHeartbeatThread();
        }

        @Override
        public void onAppendEntitiesMessage(AppendEntitiesMessage m) {
            Node.logger.trace("got append entities as leader, ignoring");
        }

        @Override
        public void onAppendEntitiesResultMessage(AppendEntitiesResultMessage m) {
            Node.logger.trace("got append entities result as leader: {}", m);
            
            int delta = m.isSuccess() ? 1 : -1;

            LogState logState = Node.this.logState.get(m.getFrom());
            logState.nextIndex = Math.max(0, logState.nextIndex + delta);

            Node.logger.trace("advanced nextIndex to {} for {}", logState.nextIndex, m.getFrom());

            if (m.isSuccess()) {
                logState.matchIndex = Long.min(logState.matchIndex + 1, Node.this.persisted.lastLogIndex());
                Node.logger.trace("match index for node {} updated to {}", m.getFrom(), logState.matchIndex);
            }

            tryUpdateCommitIndex();
        }

        @Override
        public void onRequestVoteMessage(RequestVoteMessage m) {
            Node.logger.trace("got request vote as leader, ignoring");
        }

        @Override
        public void onRequestVoteResultMessage(RequestVoteResultMessage m) {
            Node.logger.trace("got request vote result as leader, ignoring");
        }

        @Override
        public String toString() {
            return "Leader";
        }

        private void tryUpdateCommitIndex() {

            Node.logger.trace("trying to update commit index on leader");

            List<Long> commitIndices = 
                Node.this.logState.values().stream()
                .map(ls -> ls.matchIndex)
                .sorted((a, b) -> (int) (b - a))
                .collect(Collectors.toList());

            Node.logger.trace("commit indices: {}", commitIndices);

            long maxCommitedByMajority = Long.MAX_VALUE;

            for (int i = 0; i < majority(); i++) {
                long commitValue = commitIndices.get(i);
                maxCommitedByMajority = Long.min(commitValue, maxCommitedByMajority);
            }

            if (maxCommitedByMajority == Long.MAX_VALUE) {
                Node.logger.trace("No max commited by majority");
                return;
            }

            long prevCommit = Node.this.commitIndex;
            Node.this.commitIndex = Long.max(Node.this.commitIndex, maxCommitedByMajority);

            if (prevCommit != Node.this.commitIndex) {
                Node.logger.debug("Updated commit index {} -> {}", prevCommit, Node.this.commitIndex);
            }
        }

        @Override
        public void onClientCommandMessage(ClientCommandMessage m) throws InterruptedException {

            logger.trace("adhering to client command as leader");

            Node.this.persisted.appendEntity(
                Node.this.persisted.lastLogIndex() + 1, 
                Node.this.persisted.getCurrentTerm(), 
                m.getData());

            // TODO respond once the message is applied
           m.reply(m.getFrom().getTransportLocation(), new ClientCommandResultMessage(Node.this.selfId, m.getFrom(), "ok", true));
        }

        @Override
        public void onClientCommandResultMessage(ClientCommandResultMessage m) throws InterruptedException {
            Node.logger.trace("got client command result as leader ignoring");
        }
    }

    private class Candidate implements State {

        private int votes;
        private Set<NodeIdentity> voted = new HashSet<NodeIdentity>();

        @Override
        public void onEnterState() throws InterruptedException {
            voted.clear();
            voted.add(Node.this.selfId);
            votes = 1;

            Node.this.persisted.setCurrentTerm(Node.this.persisted.getCurrentTerm() + 1);
            Node.this.persisted.setVotedFor(Node.this.selfId);

            Node.logger.debug("Vote progress, {}/{} to become leader", votes, Node.this.nodes.size());

            for (NodeIdentity other : Node.this.nodes) {
                if (other.equals(Node.this.selfId)) continue;

                logger.trace("Sending vote request to {} from {}", Node.this.selfId, other);

                Node.this.internalRpc.send(
                    other.getTransportLocation(),
                    new BindableRequestVoteMessage(
                        Node.this.selfId, 
                        other, 
                        Node.this.persisted.getCurrentTerm(), 
                        Node.this.selfId, 
                        Node.this.persisted.lastLogIndex(), 
                        Node.this.persisted.lastLogTerm()));
            }

            Node.this.restartElectionThread();
        }

        @Override
        public void onExitState() {
            Node.this.cancelElectionThread();
        }

        @Override
        public void onAppendEntitiesMessage(AppendEntitiesMessage m) throws InterruptedException {
            Node.logger.trace("got append entities as candidate, converting to follower");
            Node.this.changeState(FOLLOWER);
        }

        @Override
        public void onAppendEntitiesResultMessage(AppendEntitiesResultMessage m) {
            Node.logger.trace("got append entities result as candidate, ignoring");
        }

        @Override
        public void onRequestVoteMessage(RequestVoteMessage m) {
            Node.logger.trace("got request vote as candidate, ignoring");
        }

        @Override
        public void onRequestVoteResultMessage(RequestVoteResultMessage m) throws InterruptedException {

            if (!m.isSuccess()) {
                Node.logger.trace("request vote from {} was not successful", m.getFrom());
            }

            if (this.voted.contains(m.getFrom())) {
                Node.logger.trace("stopping duplicate vote from going through from {}", m.getFrom());
                return;
            }

            Node.logger.trace("request vote from {} was successful", m.getFrom());

            this.voted.add(m.getFrom());

            this.votes += 1;

            Node.logger.debug("Vote progress, {}/{} to become leader", votes, Node.this.nodes.size());

            if (this.votes >= majority()) {
                Node.logger.debug("got majority vote, converting to leader");
                Node.this.changeState(LEADER);
            }
        }

        @Override
        public String toString() {
            return "Candidate";
        }

        @Override
        public void onClientCommandMessage(ClientCommandMessage m) throws InterruptedException {
            logger.trace("ignoring client command as candidate");
        }

        @Override
        public void onClientCommandResultMessage(ClientCommandResultMessage m) throws InterruptedException {
            logger.trace("ignoring client command result as candidate");
        }
    }

    private static class LogState {
        long nextIndex;
        long matchIndex;
    }
}