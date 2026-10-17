package com.doer.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.restassured.path.json.JsonPath;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/** Exceptions of doer methods: {@code @ExceptionDescriber}, retries and {@code @RetryPolicy}. */
class ErrorsE2E extends E2eTestBase {

    @Test
    void exception_describer_should_add_details_to_extra_json() throws Exception {
        resetServer();
        long taskId1 = pushTask("Error checked exception");
        long taskId2 = pushTask("Error retry policy");
        Thread.sleep(100);

        // taskId1 - throws Exception - extra_json should not have "e2": "RuntimeException" value
        String extraJson1 = selectStringValue("SELECT extra_json::VARCHAR FROM task_logs WHERE task_id = " + taskId1);
        assertTrue(extraJson1.contains("\"e1\": \"Exception\""), extraJson1);
        assertFalse(extraJson1.contains("e2"), extraJson1);

        // taskId2 - throws RuntimeException - extra_json should have both "Exception" and "RuntimeException" lines
        String extraJson2 = selectStringValue("SELECT extra_json::VARCHAR FROM task_logs WHERE task_id = " + taskId2);
        assertTrue(extraJson2.contains("\"e1\": \"Exception\""), extraJson2);
        assertTrue(extraJson2.contains("\"e2\": \"RuntimeException\""), extraJson2);
    }

    @Test
    void no_onException_method_should_be_retried_in_5_min() throws Exception {
        resetServer();
        pushTask("Error checked exception");
        Thread.sleep(200);
        assertEquals(1, selectLongValue("SELECT count(*) FROM task_logs"));

        // Pretending task was updated long ago
        sqlUpdate("UPDATE tasks SET modified = modified - '5min'::INTERVAL");
        reloadQueues();

        Thread.sleep(200);
        assertEquals(2, selectLongValue("SELECT count(*) FROM task_logs"));
    }

    @Test
    void retryPolicy_should_set_fallbackStatus_when_duration_elapsed() throws Exception {
        resetServer();
        long taskId = pushTask("Error retry policy");
        Instant deadline = Instant.now().plus(Duration.ofSeconds(11));
        while (deadline.isAfter(Instant.now())) {
            Thread.sleep(200);
            checkReadyTasks();
        }
        assertEquals("Error retry policy fallback", waitTaskStatus(taskId, "Error retry policy fallback"));
        assertEquals(6, selectLongValue("SELECT count(*) FROM task_logs"));
    }

    @Test
    void retryPolicy_without_duration_should_retry_forever() throws Exception {
        resetServer();
        long taskId = pushTask("Error retry forever");
        waitTaskLogs(taskId, 1);

        // Failing for 2 days: without @RetryPolicy the task would get the status null after 1 day
        sqlUpdate("UPDATE tasks SET failing_since = now() - '2 days'::INTERVAL, modified = now() - '1 min'::INTERVAL "
                + "WHERE id = " + taskId);
        reloadQueues();
        waitTaskLogs(taskId, 2);

        RestTask task = restGetTask(taskId);
        assertEquals("Error retry forever", task.status());
        assertNotNull(task.failingSince());
        assertEquals("Error retry forever", selectStringValue(
                "SELECT final_status FROM task_logs WHERE task_id = " + taskId + " ORDER BY id DESC LIMIT 1"));
    }

    @Test
    void cause_and_suppressed_exceptions_should_be_described() throws Exception {
        resetServer();
        long taskId = pushTask("Error cause and suppressed");
        waitTaskLogs(taskId, 1);

        JsonPath extraJson = JsonPath.from(
                selectStringValue("SELECT extra_json::VARCHAR FROM task_logs WHERE task_id = " + taskId));
        assertEquals("Error with cause", extraJson.getString("message"));
        assertEquals("Exception", extraJson.getString("e1"));
        assertNull(extraJson.get("e2"));
        assertEquals("Error cause", extraJson.getString("cause.message"));
        assertEquals("RuntimeException", extraJson.getString("cause.e2"));
        assertEquals(1, extraJson.getList("suppressed").size());
        assertEquals("Error suppressed", extraJson.getString("suppressed[0].message"));
        assertEquals("RuntimeException", extraJson.getString("suppressed[0].e2"));
    }

    /** Waits until the task has the number of task logs. */
    private static void waitTaskLogs(long taskId, long count) throws InterruptedException {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
        while (selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = " + taskId) < count) {
            if (Instant.now().isAfter(deadline)) {
                throw new AssertionError("Task " + taskId + " has no " + count + " task logs in 10 s");
            }
            Thread.sleep(100);
        }
    }
}
