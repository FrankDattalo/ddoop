package ddoop.raft.rpc.message;

import ddoop.raft.rpc.NodeIdentity;
import ddoop.raft.rpc.message.base.AbstractResultMessage;

public class ClientCommandResultMessage extends AbstractResultMessage<String> {

    public ClientCommandResultMessage(NodeIdentity from, NodeIdentity to, String data, boolean success) {
        super(from, to, MessageType.ClientCommandResult, data, success);
    }

}
