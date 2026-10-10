package transitsims;

/** Statuses of the task of a simulation. */
public final class SimStatus {
    public static final String READY = "Sim ready";
    public static final String RUNNING = "Sim running";
    public static final String PAUSED = "Sim paused";
    public static final String COMPLETED = "Sim completed";

    private SimStatus() {
    }
}
