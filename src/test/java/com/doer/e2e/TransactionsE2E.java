package com.doer.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Transactions of the runtime around doer methods, {@code @TaskDataLoader} and {@code @TaskDataSaver}. */
class TransactionsE2E extends E2eTestBase {

    @Test
    void load_should_happen_in_the_same_transaction_with_task_start() {
        resetServer();
        long taskId = pushTask("Car need polishing");
        assertEquals("Car is polished", waitTaskStatus(taskId, "Car is polished"));
        List<DemoLogRow> logs = loadDemoLogs(taskId);
        assertEquals(4, logs.size());

        DemoLogRow taskStart = logs.get(0);
        DemoLogRow carLoaded = logs.get(1);
        DemoLogRow taskStop = logs.get(2);
        assertEquals("task", taskStart.type());
        assertEquals("Car", carLoaded.type());
        assertEquals("task", taskStop.type());
        assertEquals(taskStart.txId(), carLoaded.txId());
        assertNotEquals(carLoaded.txId(), taskStop.txId());
    }

    @Test
    void save_should_happen_in_the_same_transaction_with_task_stop() {
        resetServer();
        long taskId = pushTask("Car need polishing");
        assertEquals("Car is polished", waitTaskStatus(taskId, "Car is polished"));
        List<DemoLogRow> logs = loadDemoLogs(taskId);
        assertEquals(4, logs.size());

        DemoLogRow taskStart = logs.get(0);
        DemoLogRow taskStop = logs.get(2);
        DemoLogRow carUnloaded = logs.get(3);
        assertEquals("task", taskStart.type());
        assertEquals("task", taskStop.type());
        assertEquals("Car", carUnloaded.type());
        assertEquals(taskStop.txId(), carUnloaded.txId());
        assertNotEquals(carUnloaded.txId(), taskStart.txId());
    }

    @Test
    void task_updated_in_doer_method_should_run_in_its_own_transaction() {
        resetServer();
        long taskId = pushTask("Need wash hands");
        assertEquals("Washed", waitTaskStatus(taskId, "Washed"));

        List<DemoLogRow> logs = loadDemoLogs(taskId);
        assertEquals(3, logs.size());
        DemoLogRow taskStart = logs.get(0);
        DemoLogRow taskUpdate = logs.get(1);
        DemoLogRow taskStop = logs.get(2);
        assertEquals("task", taskStart.type());
        assertEquals("task", taskUpdate.type());
        assertEquals("task", taskStop.type());
        assertNotEquals(taskStart.txId(), taskUpdate.txId());
        assertNotEquals(taskUpdate.txId(), taskStop.txId());
        assertNotEquals(taskStop.txId(), taskStart.txId());
    }

    @Test
    void on_exception_data_should_not_be_saved() throws Exception {
        resetServer();
        long taskId = pushTask("Should send email");
        Instant deadLine = Instant.now().plus(Duration.ofSeconds(2));
        while (Instant.now().isBefore(deadLine)) {
            if (loadDemoLogs(taskId).size() > 2) {
                Thread.sleep(100); // letting transactions finish
                break;
            }
            Thread.sleep(100);
        }
        List<DemoLogRow> logs = loadDemoLogs(taskId);
        assertEquals(3, logs.size());
        DemoLogRow taskStart = logs.get(0);
        DemoLogRow carLoaded = logs.get(1);
        DemoLogRow taskStop = logs.get(2);
        assertEquals(taskStart.txId(), carLoaded.txId());
        assertNotEquals(taskStart.txId(), taskStop.txId());
    }
}
