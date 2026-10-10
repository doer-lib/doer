package com.doer.e2e;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Transit Sims as its clients use it, with Doer running its buses: a line of three stops s1 – s2 – s3, 30 units apart,
 * and one bus on the route s1, s2, s3.
 */
class TransitSimsE2E extends E2eTestBase {
    static final Duration TIMEOUT = Duration.ofSeconds(30);

    @BeforeEach
    void reset() {
        resetServer(true);
    }

    @Test
    void simulation_should_be_created_with_buses_at_stops() {
        String sim = createSim(30, 2);

        JsonPath view = get("/api/sims/" + sim);
        assertEquals("Sim ready", view.getString("status"));
        assertEquals(0L, view.getLong("time"));
        Map<String, Object> bus = buses(sim).get(0);
        assertEquals("Bus at terminal", bus.get("status"));
        assertEquals("s1", bus.get("stop"));
        // json_data is formatted, so that it can be read in psql
        String json = selectStringValue("SELECT json_data::text FROM sims WHERE id = '" + sim + "'");
        assertTrue(json.startsWith("{\n    \"road\": {"), json);
    }

    @Test
    void buses_should_go_from_stop_to_stop() {
        String sim = createSim(30, 2);
        // A passenger who never arrives keeps the buses going
        post("/api/sims/" + sim + "/passengers", null, 200);
        post("/api/sims/" + sim + "/start", null, 200);

        Map<String, Object> driving = waitBus(sim, b -> "Bus driving".equals(b.get("status")));
        assertEquals("s1", driving.get("stop"));
        assertEquals(List.of("a", "b"), pathOf(driving));
        waitBus(sim, b -> "Bus at stop".equals(b.get("status")) && "s2".equals(b.get("stop")));
        waitBus(sim, b -> "Bus at terminal".equals(b.get("status")) && "s3".equals(b.get("stop")));
    }

    @Test
    void concurrent_boarding_should_respect_capacity_and_stop() {
        String sim = createSim(30, 2);
        String bus = (String) buses(sim).get(0).get("id");

        List<Integer> statuses = IntStream.rangeClosed(1, 10)
                .mapToObj(p -> CompletableFuture.supplyAsync(() -> board(sim, bus, p, "s1")))
                .toList().stream()
                .map(CompletableFuture::join)
                .toList();

        assertEquals(2, statuses.stream().filter(s -> s == 200).count(), statuses::toString);
        assertEquals(8, statuses.stream().filter(s -> s == 409).count(), statuses::toString);
        assertEquals(409, board(sim, bus, 11, "s2"), "boarding at another stop");
        assertEquals(2, passengersOf(buses(sim).get(0)).size());
    }

    @Test
    void pause_should_stop_time_and_buses() throws Exception {
        // 3 s from stop to stop
        String sim = createSim(10, 2);
        post("/api/sims/" + sim + "/passengers", null, 200);
        post("/api/sims/" + sim + "/start", null, 200);
        waitBus(sim, b -> "Bus driving".equals(b.get("status")));

        post("/api/sims/" + sim + "/pause", null, 200);
        long pausedTime = get("/api/sims/" + sim).getLong("time");
        Thread.sleep(3000);

        assertEquals(pausedTime, get("/api/sims/" + sim).getLong("time"));
        assertEquals("Bus driving", buses(sim).get(0).get("status"));
        post("/api/sims/" + sim + "/resume", null, 200);
        waitBus(sim, b -> "Bus at stop".equals(b.get("status")) && "s2".equals(b.get("stop")));
    }

    @Test
    void simulation_should_complete_when_passengers_have_arrived() {
        String sim = createSim(30, 3);
        int passenger = post("/api/sims/" + sim + "/passengers", null, 200).getInt("passenger");
        String bus = (String) buses(sim).get(0).get("id");
        assertEquals(200, board(sim, bus, passenger, "s1"));
        post("/api/sims/" + sim + "/start", null, 200);

        waitBus(sim, b -> "Bus at stop".equals(b.get("status")) && "s2".equals(b.get("stop")));
        post("/api/sims/" + sim + "/buses/" + bus + "/alight", boarding(passenger, "s2"), 200);
        post("/api/sims/" + sim + "/passengers/" + passenger + "/arrived", null, 200);

        waitSim(sim, "Sim completed");
        Map<String, Object> parked = buses(sim).get(0);
        assertEquals("Bus parked", parked.get("status"));
        assertEquals("s3", parked.get("stop"));
        assertFalse(passengersOf(parked).contains(passenger));
    }

    /** A simulation with one bus; {@code speed} in units per second, {@code dwell} in seconds at any stop. */
    static String createSim(int speed, int dwell) {
        String body = """
                {
                    "road": {
                        "vertices": [{"id": "a", "x": 0, "y": 0}, {"id": "b", "x": 30, "y": 0}, {"id": "c", "x": 60, "y": 0}],
                        "edges": [{"from": "a", "to": "b"}, {"from": "b", "to": "c"}]
                    },
                    "stops": [{"id": "s1", "name": "West", "x": 0, "y": 1}, {"id": "s2", "name": "Center", "x": 30, "y": 1},
                        {"id": "s3", "name": "East", "x": 60, "y": 1}],
                    "routes": [{"id": "r1", "name": "1", "stops": ["s1", "s2", "s3"]}],
                    "buses": 1, "capacity": 2, "speed": %d, "stopDwell": %d, "terminalDwell": %d
                }
                """.formatted(speed, dwell, dwell);
        return post("/api/sims", body, 200).getString("id");
    }

    static JsonPath get(String path) {
        return given().get(path).then().statusCode(200).extract().jsonPath();
    }

    static JsonPath post(String path, String body, int expectedStatus) {
        var request = given().contentType(ContentType.JSON);
        if (body != null) {
            request.body(body);
        }
        return request.post(path).then().statusCode(expectedStatus).extract().jsonPath();
    }

    static int board(String sim, String bus, int passenger, String stop) {
        return given().contentType(ContentType.JSON)
                .body(boarding(passenger, stop))
                .post("/api/sims/" + sim + "/buses/" + bus + "/board")
                .statusCode();
    }

    static String boarding(int passenger, String stop) {
        return "{\"passenger\": " + passenger + ", \"stop\": \"" + stop + "\"}";
    }

    static List<Map<String, Object>> buses(String sim) {
        return get("/api/sims/" + sim + "/buses").getList("$");
    }

    @SuppressWarnings("unchecked")
    static List<Integer> passengersOf(Map<String, Object> busView) {
        return (List<Integer>) ((Map<String, Object>) busView.get("bus")).get("passengers");
    }

    @SuppressWarnings("unchecked")
    static List<String> pathOf(Map<String, Object> busView) {
        Map<String, Object> bus = (Map<String, Object>) busView.get("bus");
        List<Map<String, Object>> path = (List<Map<String, Object>>) bus.get("path");
        return path.stream().map(v -> (String) v.get("id")).toList();
    }

    /** Polls the first bus of the simulation until it matches; returns it. */
    static Map<String, Object> waitBus(String sim, Predicate<Map<String, Object>> condition) {
        Instant deadline = Instant.now().plus(TIMEOUT);
        Map<String, Object> bus = null;
        while (Instant.now().isBefore(deadline)) {
            bus = buses(sim).get(0);
            if (condition.test(bus)) {
                return bus;
            }
            sleep();
        }
        return fail("The bus did not get there in " + TIMEOUT + ", last: " + bus);
    }

    static void waitSim(String sim, String status) {
        Instant deadline = Instant.now().plus(TIMEOUT);
        String last = null;
        while (Instant.now().isBefore(deadline)) {
            last = get("/api/sims/" + sim).getString("status");
            if (status.equals(last)) {
                return;
            }
            sleep();
        }
        fail("The simulation is not " + status + " in " + TIMEOUT + ", last: " + last);
    }

    private static void sleep() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
