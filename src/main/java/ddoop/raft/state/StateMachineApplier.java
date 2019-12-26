package ddoop.raft.state;

/**
 * Interface for applying commands to the state machine once they are 
 * replicated.
 */
public interface StateMachineApplier {

    /**
     * Applies the command to the state machine.
     * @param command The command to apply.
     */
    public void apply(String command);
    
}