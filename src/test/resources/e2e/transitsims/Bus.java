package transitsims;

import jakarta.json.bind.annotation.JsonbPropertyOrder;
import jakarta.json.bind.annotation.JsonbTransient;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/** A bus as it is kept in buses.json_data: where it is on its route, and who is on board. */
@JsonbPropertyOrder({ "simulationId", "routeId", "state", "stopIndex", "direction", "arrivedAt", "departedAt",
        "path", "capacity", "passengers" })
public class Bus {

    public enum State {
        /** At the stop {@code stopIndex}. */
        STANDING,
        /** On the {@code path} from the stop {@code stopIndex} to the next one. */
        DRIVING,
        /** At a terminal for good. */
        PARKED
    }

    @JsonbTransient
    public UUID id;
    @JsonbTransient
    public long taskId;
    /** The simulation of the bus, loaded with it to be read; the bus never saves it. */
    @JsonbTransient
    public Simulation simulation;
    /** The database time when the bus was loaded. */
    @JsonbTransient
    public Instant now;

    public UUID simulationId;
    public String routeId;
    public State state = State.STANDING;
    /** The last stop of the route the bus stood at. */
    public int stopIndex;
    /** +1 or -1, along the stops of the route. */
    public int direction = 1;
    /** Simulation time, milliseconds. */
    public long arrivedAt;
    public long departedAt;
    /** The road to the next stop, while driving. */
    public List<Simulation.Vertex> path = new ArrayList<>();
    public int capacity;
    public Set<Integer> passengers = new TreeSet<>();

    public Simulation.Route route() {
        return simulation.route(routeId);
    }

    public String stopId() {
        return route().stops().get(stopIndex);
    }

    public boolean atTerminal() {
        return stopIndex == 0 || stopIndex == route().stops().size() - 1;
    }

    /** Simulation time when the bus was loaded. */
    public long time() {
        return simulation.time(now);
    }
}
