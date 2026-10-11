package transitsims;

/** Statuses of the task of a simulation while Doer runs it; a ready or completed simulation has none. */
public final class SimStatus {
    public static final String RUNNING = "Sim running";
    public static final String PAUSED = "Sim paused";

    private SimStatus() {
    }
}
