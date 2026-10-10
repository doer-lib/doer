package com.doer.e2e;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/** DoerService.facilitateCoordinatedUpdate called from a REST endpoint. */
class CoordinatedUpdateE2E extends E2eTestBase {

    @Test
    void coordinated_update_should_change_status_and_write_log() {
        resetServer();
        long taskId = pushTask("Parked");

        given().queryParam("id", taskId).queryParam("s", "Unparked")
                .get("/api/validation/coordinated_update")
                .then()
                .statusCode(200)
                .body("status", equalTo("Unparked"));

        assertEquals("Unparked", restGetTask(taskId).status());
        assertEquals("Parked>Unparked>ValidationResource>coordinatedUpdate", selectStringValue(
                "SELECT initial_status || '>' || final_status || '>' || class_name || '>' || method_name "
                        + "FROM task_logs WHERE task_id = " + taskId));
    }

    @Test
    void coordinated_update_should_fail_for_in_progress_task_without_hijack() {
        resetServer();
        long taskId = pushTask("Parked");
        sqlUpdate("UPDATE tasks SET in_progress = TRUE WHERE id = " + taskId);

        given().queryParam("id", taskId).queryParam("s", "Unparked")
                .get("/api/validation/coordinated_update")
                .then()
                .statusCode(500);

        RestTask task = restGetTask(taskId);
        assertEquals("Parked", task.status());
        assertTrue(task.inProgress());
    }

    @Test
    void coordinated_update_should_hijack_in_progress_task() {
        resetServer();
        long taskId = pushTask("Parked");
        sqlUpdate("UPDATE tasks SET in_progress = TRUE WHERE id = " + taskId);

        given().queryParam("id", taskId).queryParam("s", "Unparked").queryParam("hijack", true)
                .get("/api/validation/coordinated_update")
                .then()
                .statusCode(200)
                .body("status", equalTo("Unparked"));

        RestTask task = restGetTask(taskId);
        assertEquals("Unparked", task.status());
        assertFalse(task.inProgress());
        assertEquals(1L, selectLongValue(
                "SELECT count(*) FROM task_logs WHERE exception_type = 'TaskHijacked' AND task_id = " + taskId));
    }

    @Test
    void coordinated_update_should_rollback_when_updater_throws() {
        resetServer();
        long taskId = pushTask("Parked");

        given().queryParam("id", taskId).queryParam("s", "Unparked").queryParam("fail", true)
                .get("/api/validation/coordinated_update")
                .then()
                .statusCode(500)
                .body("exception", equalTo("IllegalStateException"));

        RestTask task = restGetTask(taskId);
        assertEquals("Parked", task.status());
        assertEquals(0, task.version());
        assertEquals(0L, selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = " + taskId));
    }

    @Test
    void coordinated_update_should_fail_when_task_not_found() {
        resetServer();

        given().queryParam("id", 9839893L).queryParam("s", "Unparked")
                .get("/api/validation/coordinated_update")
                .then()
                .statusCode(500)
                .body("exception", equalTo("TaskNotFoundException"));
    }

    @Test
    void coordinated_update_should_wait_until_task_is_not_in_progress() throws Exception {
        resetServer();
        long taskId = pushTask("Parked");
        sqlUpdate("UPDATE tasks SET in_progress = TRUE WHERE id = " + taskId);
        CompletableFuture<Void> doerCompletion = CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            sqlUpdate("UPDATE tasks SET in_progress = FALSE, status = 'Waited', version = version + 1 WHERE id = "
                    + taskId);
        });

        given().queryParam("id", taskId).queryParam("s", "Unparked").queryParam("wait", 10000)
                .get("/api/validation/coordinated_update")
                .then()
                .statusCode(200)
                .body("status", equalTo("Unparked"));
        doerCompletion.get();

        RestTask task = restGetTask(taskId);
        assertEquals("Unparked", task.status());
        assertFalse(task.inProgress());
        assertEquals("Waited>Unparked", selectStringValue(
                "SELECT string_agg(initial_status || '>' || final_status, ',') FROM task_logs WHERE task_id = "
                        + taskId));
        assertEquals(0L, selectLongValue(
                "SELECT count(*) FROM task_logs WHERE exception_type = 'TaskHijacked' AND task_id = " + taskId));
    }

    @Test
    void coordinated_update_should_rollback_hijack_when_updater_throws() {
        resetServer();
        long taskId = pushTask("Parked");
        sqlUpdate("UPDATE tasks SET in_progress = TRUE WHERE id = " + taskId);

        given().queryParam("id", taskId).queryParam("s", "Unparked").queryParam("hijack", true)
                .queryParam("fail", true)
                .get("/api/validation/coordinated_update")
                .then()
                .statusCode(500)
                .body("exception", equalTo("IllegalStateException"));

        RestTask task = restGetTask(taskId);
        assertEquals("Parked", task.status());
        assertTrue(task.inProgress());
        assertEquals(0, task.version());
        assertEquals(0L, selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = " + taskId));
    }

    @Test
    void coordinated_update_should_fail_without_loader() {
        resetServer();
        long taskId = pushTask("Parked");

        given().queryParam("id", taskId).queryParam("s", "Unparked")
                .get("/api/validation/coordinated_update_without_loader")
                .then()
                .statusCode(500)
                .body("exception", equalTo("IllegalArgumentException"));

        RestTask task = restGetTask(taskId);
        assertEquals("Parked", task.status());
        assertEquals(0, task.version());
        assertEquals(0L, selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = " + taskId));
    }

    @Test
    void coordinated_update_should_load_and_save_in_the_same_transaction() {
        resetServer();
        long taskId = pushTask("Parked");

        given().queryParam("id", taskId).queryParam("s", "Unparked")
                .get("/api/validation/coordinated_data_update")
                .then()
                .statusCode(200)
                .body("status", equalTo("Unparked"));

        // Data loaded, task updated (trigger), data saved
        List<DemoLogRow> logs = loadDemoLogs(taskId);
        List<String> types = logs.stream().map(DemoLogRow::type).toList();
        assertEquals(List.of("TransactionData", "task", "TransactionData"), types);
        assertEquals(logs.get(0).txId(), logs.get(1).txId());
        assertEquals(logs.get(1).txId(), logs.get(2).txId());
    }

    @Test
    void coordinated_update_should_not_be_called_in_transaction() {
        resetServer();
        long taskId = pushTask("Parked");

        given().queryParam("id", taskId).queryParam("s", "Unparked")
                .get("/api/validation/coordinated_update_in_transaction")
                .then()
                .statusCode(500);

        assertEquals("Parked", restGetTask(taskId).status());
    }
}
