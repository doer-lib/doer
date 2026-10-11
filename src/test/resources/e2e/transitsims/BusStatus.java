package transitsims;

/**
 * Statuses of the task of a bus: whether Doer runs it. Where the bus is, is in {@link Bus#state}. A bus before the
 * start, paused or parked has no status.
 */
public final class BusStatus {
    /** Set to all buses when the simulation starts or resumes. */
    public static final String RESUME = "Bus resume";
    /** The bus is simulated step by step. */
    public static final String ON_ROUTE = "Bus on route";

    private BusStatus() {
    }
}
