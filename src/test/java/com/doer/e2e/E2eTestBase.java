package com.doer.e2e;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;

import com.doer.testkit.Sql;
import io.restassured.RestAssured;
import io.restassured.path.json.JsonPath;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base for the E2E tests of Transit Sims: helpers for the validation endpoints ({@code /api/validation/...}) and for the
 * database of {@link E2eEnvironment}.
 */
@ExtendWith(E2eEnvironment.class)
abstract class E2eTestBase {

    @BeforeEach
    void enableRestLogging() {
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    }

    /** Task as the validation endpoint returns it. */
    record RestTask(long id, Instant created, Instant modified, Instant failingSince, String status,
            boolean inProgress, int version) {
    }

    /** Row of {@code demo_log_tasks}: what was loaded, saved or updated, and in which transaction. */
    record DemoLogRow(long id, String type, Long taskId, Boolean inProgress, String txId) {
    }

    static void pauseServer() {
        given()
                .when()
                .get("/api/validation/stop")
                .then()
                .statusCode(200)
                .body("status", equalTo("Doer Stopped"));
    }

    static void resumeServer(boolean enableMonitor) {
        given()
                .when()
                .queryParam("m", enableMonitor)
                .get("/api/validation/start")
                .then()
                .statusCode(200)
                .body("status", equalTo("Doer Started"));
    }

    static void resetServer() {
        given()
                .when()
                .get("/api/validation/reset")
                .then()
                .statusCode(200)
                .body("status", equalTo("Doer Reset"));
    }

    static void checkReadyTasks() {
        given()
                .when()
                .get("/api/validation/check")
                .then()
                .statusCode(200)
                .body("status", equalTo("check"));
    }

    static void reloadQueues() {
        given()
                .when()
                .get("/api/validation/load")
                .then()
                .statusCode(200)
                .body("status", equalTo("Doer Reloaded"));
    }

    static long pushTask(String status) {
        return given()
                .when()
                .queryParam("s", status)
                .get("/api/validation/add_task")
                .then()
                .statusCode(200)
                .body("status", equalTo(status))
                .extract()
                .jsonPath()
                .getLong("id");
    }

    /** Waits until the task gets the status, or is not modified for 2 seconds; returns the last status. */
    static String waitTaskStatus(long id, String status) {
        while (true) {
            RestTask task = restGetTask(id);
            boolean idle = task.modified().plus(Duration.ofSeconds(2)).isBefore(Instant.now());
            if (Objects.equals(status, task.status()) || idle) {
                return task.status();
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return task.status();
            }
        }
    }

    static RestTask restGetTask(long taskId) {
        JsonPath json = given()
                .when()
                .queryParam("id", taskId)
                .get("/api/validation/task")
                .then()
                .statusCode(200)
                .extract()
                .jsonPath();
        String failingSince = json.getString("failingSince");
        return new RestTask(json.getLong("id"),
                Instant.parse(json.getString("created")),
                Instant.parse(json.getString("modified")),
                failingSince == null ? null : Instant.parse(failingSince),
                json.getString("status"),
                json.getBoolean("inProgress"),
                json.getInt("version"));
    }

    static List<DemoLogRow> loadDemoLogs(long taskId) {
        List<DemoLogRow> list = new ArrayList<>();
        try (Connection con = E2eEnvironment.dataSource().getConnection();
                PreparedStatement pst = con
                        .prepareStatement("SELECT * FROM demo_log_tasks WHERE task_id = ? ORDER BY id")) {
            pst.setLong(1, taskId);
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    list.add(new DemoLogRow(rs.getLong("id"), rs.getString("object_type"), rs.getLong("task_id"),
                            rs.getBoolean("in_progress"), rs.getString("tx_id")));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    static void sqlUpdate(String sql) {
        Sql.update(E2eEnvironment.dataSource(), sql);
    }

    static Long selectLongValue(String sql) {
        return Sql.selectLong(E2eEnvironment.dataSource(), sql);
    }

    static String selectStringValue(String sql) {
        return Sql.selectString(E2eEnvironment.dataSource(), sql);
    }
}
