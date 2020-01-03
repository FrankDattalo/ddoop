package ddoop.raft.state.mapdb;

import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.state.StateManager;
import ddoop.state.State;
import ddoop.util.MapDbUtils;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class MapDbStateManager implements StateManager {

  private final Logger logger = LoggerFactory.getLogger(State.class);

  private final DB db;

  private List<String> logValues;
  private List<Long> logTerm;
  private Atomic.String votedFor;
  private Atomic.Long currentTerm;

  public MapDbStateManager(DB db) {
    this.db = Objects.requireNonNull(db, "db cannot be null");
  }

  public void init() {
    logger.trace("init()");

    this.logValues = this.db.indexTreeList(
        MapDbUtils.nameForElement(this.getClass(), "logValues"), Serializer.STRING).createOrOpen();

    this.logTerm = this.db.indexTreeList(
        MapDbUtils.nameForElement(this.getClass(), "logTerm"), Serializer.LONG).createOrOpen();

    this.votedFor = this.db.atomicString(
        MapDbUtils.nameForElement(this.getClass(), "votedFor")).createOrOpen();

    this.currentTerm = this.db.atomicLong(
        MapDbUtils.nameForElement(this.getClass(), "currentTerm")).createOrOpen();

    this.db.commit();

    logger.trace("init done");
  }

  @Override
  public long getCurrentTerm() {
    return this.currentTerm.get();
  }

  @Override
  public void setCurrentTerm(long term) {

    this.setVotedFor(null);

    this.currentTerm.set(term);

    this.db.commit();
  }

  @Override
  public NodeIdentity getVotedFor() {
    String votedFor = this.votedFor.get();

    if (votedFor == null) {
      return null;
    }

    return NodeIdentity.parse(votedFor);
  }

  @Override
  public void setVotedFor(NodeIdentity node) {
    if (node == null) {
      this.votedFor.set(null);
      this.db.commit();
      return;
    }

    this.votedFor.set(node.toString());

    this.db.commit();
  }

  @Override
  public long lastLogIndex() {
    return this.logTerm.size();
  }

  @Override
  public long lastLogTerm() {
    return this.logTerm.isEmpty() ? 0 : this.logTerm.get(this.logTerm.size() - 1);
  }

  @Override
  public LogEntry getLogEntry(long index) {
    index -= 1;

    if (index >= this.logTerm.size() || index < 0) {
      return null;
    }

    return new LogEntry(
        this.logTerm.get((int) index),
        this.logValues.get((int) index)
    );
  }

  @Override
  public void dropLogs(long fromAndAfterIndex) {

    int index = (int) (fromAndAfterIndex - 1);

    while (this.logValues.size() > index) {
      int indexToRemove = this.logValues.size() - 1;

      this.logValues.remove(indexToRemove);
      this.logTerm.remove(indexToRemove);
    }

    this.db.commit();
  }

  @Override
  public void appendEntity(long index, long term, String entity) {
    int i = (int) (index - 1);

    if (i < this.logTerm.size()) {
      return;
    }

    this.logTerm.add(term);
    this.logValues.add(entity);

    this.db.commit();
  }
}
