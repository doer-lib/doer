package transitsims;

import com.doer.AcceptStatus;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;

/**
 * Drives a bus in steps of one second: each step either leaves the status as it is, or moves the bus on. Nothing
 * moves while the clock of the simulation stands.
 */
@ApplicationScoped
public class BusDriver {

    /** Departs to the next stop after the dwell; at a terminal turns back, or parks when nobody needs buses. */
    @AcceptStatus(value = BusStatus.AT_STOP, delay = "1s")
    @AcceptStatus(value = BusStatus.AT_TERMINAL, delay = "1s")
    public void stand(Task task, Bus bus) {
        Simulation simulation = bus.simulation;
        int dwell = bus.atTerminal() ? simulation.terminalDwell : simulation.stopDwell;
        if (!simulation.clockRunning() || bus.time() - bus.arrivedAt < dwell * 1000L) {
            return;
        }
        if (bus.atTerminal()) {
            if (simulation.allArrived() && bus.passengers.isEmpty()) {
                task.setStatus(BusStatus.PARKED);
                return;
            }
            bus.direction = bus.stopIndex == 0 ? 1 : -1;
        }
        List<String> stops = bus.route().stops();
        bus.path = simulation.path(stops.get(bus.stopIndex), stops.get(bus.stopIndex + bus.direction));
        bus.departedAt = bus.time();
        task.setStatus(BusStatus.DRIVING);
    }

    /** Arrives at the next stop when the bus has covered its path at the speed of the simulation. */
    @AcceptStatus(value = BusStatus.DRIVING, delay = "1s")
    public void drive(Task task, Bus bus) {
        double driven = (bus.time() - bus.departedAt) * bus.simulation.speed / 1000;
        if (driven < Simulation.length(bus.path)) {
            return;
        }
        bus.stopIndex += bus.direction;
        bus.arrivedAt = bus.time();
        bus.path = List.of();
        task.setStatus(bus.atTerminal() ? BusStatus.AT_TERMINAL : BusStatus.AT_STOP);
    }
}
