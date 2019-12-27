package ddoop.raft.rpc.message;

public enum MessageType {
    AppendEntities, AppendEntitiesResult,
    RequestVote, RequestVoteResult,
    ClientCommand, ClientCommandResult
}