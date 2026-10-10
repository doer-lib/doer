package com.doer.e2e;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/** Transit Sims starts in the runtime, migrates the database and restarts. */
@TestMethodOrder(OrderAnnotation.class)
public class SmokeE2E extends E2eTestBase {

    @Test
    @Order(1)
    void application_should_answer_with_runtime() {
        var response = given().get("/api/validation/info").then().statusCode(200);
        // An external application, run from the IDE, may have no E2E_RUNTIME
        if (!E2eEnvironment.isExternal()) {
            response.body("runtime", equalTo(E2eEnvironment.runtime));
        }
    }

    @Test
    @Order(2)
    void application_should_migrate_database() throws Exception {
        List<String> versions = new ArrayList<>();
        try (Connection con = E2eEnvironment.dataSource().getConnection();
                PreparedStatement pst = con.prepareStatement(
                        "SELECT version FROM flyway_schema_history WHERE success ORDER BY installed_rank");
                ResultSet rs = pst.executeQuery()) {
            while (rs.next()) {
                versions.add(rs.getString(1));
            }
        }
        assertEquals(List.of("1", "2", "3"), versions);
    }

    @Test
    @Order(3)
    void node_should_restart() throws Exception {
        assumeFalse(E2eEnvironment.isExternal(), "no restarts of an external application");

        assertEquals(143, E2eEnvironment.stopNode(1), "exit code after SIGTERM");
        E2eEnvironment.startNode(1);

        given().get("/api/validation/info").then().statusCode(200);
        List<String> log = E2eEnvironment.appLog(1).lines().toList();
        List<String> separators = log.stream().filter(line -> line.startsWith("==== ")).toList();
        assertEquals(3, separators.size(), () -> "separators: " + separators);
        assertEquals(List.of(true, false, true), separators.stream().map(s -> s.contains(" started: ")).toList(),
                () -> "separators: " + separators);
        int secondStart = log.lastIndexOf(separators.get(2));
        assertFalse(log.subList(secondStart + 1, log.size()).isEmpty(), "log of the second start");
        assertFalse(log.subList(1, log.indexOf(separators.get(1))).isEmpty(), "log of the first start");

        // The restarted node runs Doer as the application starts it, with the monitor: a delayed task is processed
        // without /api/validation/check
        long taskId = pushTask("Doer method delayed");
        Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
        while (!"Doer method delayed done".equals(restGetTask(taskId).status()) && Instant.now().isBefore(deadline)) {
            Thread.sleep(200);
        }
        assertEquals("Doer method delayed done", restGetTask(taskId).status(), "delayed task after the restart");
    }
}
