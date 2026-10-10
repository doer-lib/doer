package transitsims;

import jakarta.json.bind.annotation.JsonbPropertyOrder;
import jakarta.json.bind.annotation.JsonbTransient;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;

/**
 * A simulation as it is posted and kept in sims.json_data: the road, the stops, the routes, the buses on them, and the
 * clock. It also calculates on the road: the paths between stops and the buses of each route.
 */
@JsonbPropertyOrder({ "road", "stops", "routes", "buses", "capacity", "speed", "stopDwell", "terminalDwell",
        "startedAt", "pausedAt", "pausedMs", "passengers", "arrived" })
public class Simulation {

    public record Vertex(String id, double x, double y) {
    }

    public record Edge(String from, String to) {
    }

    public record Road(Set<Vertex> vertices, Set<Edge> edges) {
    }

    public record BusStop(String id, String name, double x, double y) {
    }

    public record Route(String id, String name, List<String> stops) {
    }

    @JsonbTransient
    public UUID id;
    @JsonbTransient
    public long taskId;

    public Road road;
    public List<BusStop> stops;
    public List<Route> routes;
    /** Number of buses on all routes. */
    public int buses;
    public int capacity;
    /** Units of length per second. */
    public double speed;
    /** Seconds a bus stands at a stop and at a terminal. */
    public int stopDwell;
    public int terminalDwell;
    /** Database time. */
    public Instant startedAt;
    public Instant pausedAt;
    public long pausedMs;
    /** Passengers registered, and arrived at their destinations. */
    public int passengers;
    public int arrived;

    /** Simulation time in milliseconds at the database time {@code now}: 0 before the start, frozen while paused. */
    public long time(Instant now) {
        if (startedAt == null) {
            return 0;
        }
        return Duration.between(startedAt, pausedAt != null ? pausedAt : now).toMillis() - pausedMs;
    }

    public boolean clockRunning() {
        return startedAt != null && pausedAt == null;
    }

    public boolean allArrived() {
        return arrived >= passengers;
    }

    public Route route(String routeId) {
        return routes.stream().filter(r -> r.id().equals(routeId)).findFirst().orElseThrow();
    }

    BusStop stop(String stopId) {
        return stops.stream().filter(s -> s.id().equals(stopId)).findFirst().orElseThrow();
    }

    /** The vertex of the road nearest to the stop. */
    Vertex nearestVertex(BusStop stop) {
        return road.vertices().stream()
                .min(Comparator.comparingDouble(v -> Math.hypot(v.x() - stop.x(), v.y() - stop.y())))
                .orElseThrow();
    }

    /** The shortest path on the road between the vertices nearest to the two stops (Dijkstra). */
    public List<Vertex> path(String fromStop, String toStop) {
        Vertex start = nearestVertex(stop(fromStop));
        Vertex end = nearestVertex(stop(toStop));
        Map<String, Vertex> vertices = new HashMap<>();
        road.vertices().forEach(v -> vertices.put(v.id(), v));
        Map<Vertex, List<Vertex>> neighbours = new HashMap<>();
        for (Edge edge : road.edges()) {
            Vertex from = vertices.get(edge.from());
            Vertex to = vertices.get(edge.to());
            neighbours.computeIfAbsent(from, v -> new ArrayList<>()).add(to);
            neighbours.computeIfAbsent(to, v -> new ArrayList<>()).add(from);
        }
        Map<Vertex, Double> distances = new HashMap<>(Map.of(start, 0.0));
        Map<Vertex, Vertex> previous = new HashMap<>();
        PriorityQueue<Vertex> queue = new PriorityQueue<>(Comparator.comparingDouble(distances::get));
        queue.add(start);
        while (!queue.isEmpty()) {
            Vertex vertex = queue.poll();
            if (vertex.equals(end)) {
                break;
            }
            for (Vertex next : neighbours.getOrDefault(vertex, List.of())) {
                double distance = distances.get(vertex) + length(List.of(vertex, next));
                if (distance < distances.getOrDefault(next, Double.MAX_VALUE)) {
                    queue.remove(next);
                    distances.put(next, distance);
                    previous.put(next, vertex);
                    queue.add(next);
                }
            }
        }
        if (!distances.containsKey(end)) {
            throw new IllegalArgumentException("No road from stop " + fromStop + " to stop " + toStop);
        }
        List<Vertex> path = new ArrayList<>();
        for (Vertex v = end; v != null; v = previous.get(v)) {
            path.add(v);
        }
        Collections.reverse(path);
        return path;
    }

    public static double length(List<Vertex> path) {
        double length = 0;
        for (int i = 1; i < path.size(); i++) {
            length += Math.hypot(path.get(i).x() - path.get(i - 1).x(), path.get(i).y() - path.get(i - 1).y());
        }
        return length;
    }

    double length(Route route) {
        double length = 0;
        for (int i = 1; i < route.stops().size(); i++) {
            length += length(path(route.stops().get(i - 1), route.stops().get(i)));
        }
        return length;
    }

    /** The number of buses of each route, proportional to its length (the largest remainders get the rest). */
    Map<Route, Integer> busesPerRoute() {
        Map<Route, Double> shares = new LinkedHashMap<>();
        double total = routes.stream().mapToDouble(this::length).sum();
        for (Route route : routes) {
            shares.put(route, total == 0 ? (double) buses / routes.size() : buses * length(route) / total);
        }
        Map<Route, Integer> result = new LinkedHashMap<>();
        shares.forEach((route, share) -> result.put(route, (int) Math.floor(share)));
        int rest = buses - result.values().stream().mapToInt(Integer::intValue).sum();
        shares.entrySet().stream()
                .sorted(Comparator.comparingDouble(e -> -(e.getValue() - Math.floor(e.getValue()))))
                .limit(rest)
                .forEach(e -> result.merge(e.getKey(), 1, Integer::sum));
        return result;
    }

    /** New buses of the simulation, spread over the stops of their routes. */
    List<Bus> newBuses() {
        List<Bus> result = new ArrayList<>();
        busesPerRoute().forEach((route, count) -> {
            if (route.stops().size() < 2) {
                throw new IllegalArgumentException("Route " + route.id() + " has less than 2 stops");
            }
            for (int i = 0; i < count; i++) {
                Bus bus = new Bus();
                bus.simulationId = id;
                bus.simulation = this;
                bus.routeId = route.id();
                bus.stopIndex = i * route.stops().size() / count;
                bus.direction = bus.stopIndex == route.stops().size() - 1 ? -1 : 1;
                bus.capacity = capacity;
                result.add(bus);
            }
        });
        return result;
    }
}
