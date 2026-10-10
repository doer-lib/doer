package com.doer.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/** Doer methods are called in the runtime: statuses go from method to method, across classes, and after a delay. */
class DoerMethodsE2E extends E2eTestBase {

    @Test
    void jaxrs_resource_can_have_doer_method() {
        resetServer();
        long id = pushTask("Validation resource first");
        assertNull(waitTaskStatus(id, null));
    }

    @Test
    void task_should_be_instantly_processed_by_different_classes() {
        resetServer();
        long taskId = pushTask("Doer method hand over");
        assertEquals("Doer method next class done", waitTaskStatus(taskId, "Doer method next class done"));
    }

    @Test
    void delayed_method_should_be_called_after_delay() throws Exception {
        resetServer();
        long taskId = pushTask("Doer method delayed");
        Instant deadLine = Instant.now().plus(Duration.ofSeconds(5));
        while (Instant.now().isBefore(deadLine)) {
            checkReadyTasks();
            Thread.sleep(200);
            RestTask task = restGetTask(taskId);
            if ("Doer method delayed done".equals(task.status())) {
                break;
            }
        }
        RestTask task = restGetTask(taskId);
        assertEquals("Doer method delayed done", task.status());
        Instant processed = Instant.ofEpochMilli(
                selectLongValue("SELECT (extract(EPOCH FROM min(created)) * 1000)::BIGINT FROM task_logs"));
        long actualDelay = Duration.between(task.created(), processed).toMillis();
        assertTrue(actualDelay >= 2000);
        // 200ms latency of calling checkReadyTasks(),
        // and more 200ms transaction toll
        assertTrue(actualDelay < 2400);
    }
}
