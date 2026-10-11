package com.doer.e2e;

import static com.doer.e2e.TransitSimsE2E.boarding;
import static com.doer.e2e.TransitSimsE2E.buses;
import static com.doer.e2e.TransitSimsE2E.createSim;
import static com.doer.e2e.TransitSimsE2E.standsAt;
import static com.doer.e2e.TransitSimsE2E.stateOf;
import static com.doer.e2e.TransitSimsE2E.waitBus;
import static com.doer.e2e.TransitSimsE2E.waitSim;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Transit Sims across restarts and on several nodes: a running simulation completes after its node is stopped and
 * started again, and two nodes on one database run one simulation together.
 */
class LifecycleE2E extends E2eTestBase {

    @BeforeEach
    void reset() {
        assumeFalse(E2eEnvironment.isExternal(), "no restarts and no second node of an external application");
        resetServer(true);
    }

    @Test
    void simulation_should_complete_after_node_restart() throws Exception {
        String sim = createSim(30, 2);
        String bus = (String) buses(sim).get(0).get("id");
        int passenger = post(1, "/api/sims/" + sim + "/passengers", null).getInt("passenger");
        post(1, "/api/sims/" + sim + "/buses/" + bus + "/board", boarding(passenger, "s1"));
        post(1, "/api/sims/" + sim + "/start", null);
        waitBus(sim, b -> "DRIVING".equals(stateOf(b)));

        assertEquals(143, E2eEnvironment.stopNode(1), "exit code after SIGTERM");
        E2eEnvironment.startNode(1);

        waitBus(sim, b -> standsAt(b, "s2"));
        post(1, "/api/sims/" + sim + "/buses/" + bus + "/alight", boarding(passenger, "s2"));
        post(1, "/api/sims/" + sim + "/passengers/" + passenger + "/arrived", null);
        waitSim(sim, "COMPLETED");
        assertEquals(0L, selectLongValue("SELECT count(*) FROM tasks WHERE in_progress"));
    }

    @Test
    void two_nodes_should_run_one_simulation() throws Exception {
        E2eEnvironment.startNode(2);
        try {
            // A new node starts without Doer, see E2eEnvironment
            node(2).queryParam("m", true).get("/api/validation/start").then().statusCode(200);
            String sim = createSim(30, 2);
            String bus = (String) buses(sim).get(0).get("id");
            int first = post(1, "/api/sims/" + sim + "/passengers", null).getInt("passenger");
            int second = post(2, "/api/sims/" + sim + "/passengers", null).getInt("passenger");
            post(1, "/api/sims/" + sim + "/buses/" + bus + "/board", boarding(first, "s1"));
            post(2, "/api/sims/" + sim + "/buses/" + bus + "/board", boarding(second, "s1"));
            post(2, "/api/sims/" + sim + "/start", null);

            waitBus(sim, b -> standsAt(b, "s2"));
            post(2, "/api/sims/" + sim + "/buses/" + bus + "/alight", boarding(first, "s2"));
            post(1, "/api/sims/" + sim + "/buses/" + bus + "/alight", boarding(second, "s2"));
            post(1, "/api/sims/" + sim + "/passengers/" + first + "/arrived", null);
            post(2, "/api/sims/" + sim + "/passengers/" + second + "/arrived", null);
            waitSim(sim, "COMPLETED");
            assertEquals(0L, selectLongValue("SELECT count(*) FROM tasks WHERE in_progress"));
        } finally {
            // Doer of node 2 must not run the tasks of the next tests
            E2eEnvironment.stopNode(2);
        }
    }

    private static RequestSpecification node(int node) {
        return given().baseUri(E2eEnvironment.baseUrl(node));
    }

    private static JsonPath post(int node, String path, String body) {
        RequestSpecification request = node(node).contentType(ContentType.JSON);
        if (body != null) {
            request.body(body);
        }
        return request.post(path).then().statusCode(200).extract().jsonPath();
    }
}
