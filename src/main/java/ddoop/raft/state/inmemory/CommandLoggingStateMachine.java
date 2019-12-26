package ddoop.raft.state.inmemory;

import java.util.ArrayList;

import ddoop.raft.state.StateMachineApplier;

public class CommandLoggingStateMachine implements StateMachineApplier {

    private final ArrayList<String> commands = new ArrayList<>();

    @Override
    public void apply(String command) {
        this.commands.add(command);
        
        System.out.println("> " + this.commands);
    }

}