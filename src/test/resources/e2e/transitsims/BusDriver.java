package transitsims;

import com.doer.AcceptStatus;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;

/**
 * Drives a bus in steps of one second. A step changes the {@link Bus}: it departs, arrives or parks; the status of
 * the task stays {@code Bus on route} until the bus is parked.
 */
@ApplicationScoped
public class BusDriver {

    /** After a start or a resume: back on the route, unless the bus is parked. */
    @AcceptStatus(BusStatus.RESUME)
    public void resume(Task task, Bus bus) {
        task.setStatus(bus.state == Bus.State.PARKED ? null : BusStatus.ON_ROUTE);
    }

    @AcceptStatus(value = BusStatus.ON_ROUTE, delay = "1s")
    public void step(Task task, Bus bus) {
        switch (bus.state) {
            case STANDING -> stand(task, bus);
            case DRIVING -> drive(bus);
            case PARKED -> task.setStatus(null);
        }
    }

    /** Departs to the next stop after the dwell; at a terminal turns back, or parks when nobody needs buses. */
    private void stand(Task task, Bus bus) {
        Simulation simulation = bus.simulation;
        int dwell = bus.atTerminal() ? simulation.terminalDwell : simulation.stopDwell;
        if (bus.time() - bus.arrivedAt < dwell * 1000L) {
            return;
        }
        if (bus.atTerminal()) {
            if (simulation.allArrived() && bus.passengers.isEmpty()) {
                bus.state = Bus.State.PARKED;
                task.setStatus(null);
                return;
            }
            bus.direction = bus.stopIndex == 0 ? 1 : -1;
        }
        List<String> stops = bus.route().stops();
        bus.path = simulation.path(stops.get(bus.stopIndex), stops.get(bus.stopIndex + bus.direction));
        bus.departedAt = bus.time();
        bus.state = Bus.State.DRIVING;
    }

    /** Arrives at the next stop when the bus has covered its path at the speed of the simulation. */
    private void drive(Bus bus) {
        double driven = (bus.time() - bus.departedAt) * bus.simulation.speed / 1000;
        if (driven < Simulation.length(bus.path)) {
            return;
        }
        bus.stopIndex += bus.direction;
        bus.arrivedAt = bus.time();
        bus.path = List.of();
        bus.state = Bus.State.STANDING;
    }
}
