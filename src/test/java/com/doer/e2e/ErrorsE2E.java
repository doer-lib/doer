package com.doer.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/** Exceptions of doer methods: {@code @ExceptionDescriber}, retries and {@code @RetryPolicy}. */
class ErrorsE2E extends E2eTestBase {

    @Test
    void exception_describer_should_add_details_to_extra_json() throws Exception {
        resetServer();
        long taskId1 = pushTask("Should send email");
        long taskId2 = pushTask("Should check email");
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
        pushTask("Should send email");
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
        long taskId = pushTask("Should check email");
        Instant deadline = Instant.now().plus(Duration.ofSeconds(11));
        while (deadline.isAfter(Instant.now())) {
            Thread.sleep(200);
            checkReadyTasks();
        }
        assertEquals("Email check failed", waitTaskStatus(taskId, "Email check failed"));
        assertEquals(6, selectLongValue("SELECT count(*) FROM task_logs"));
    }
}
