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
        long taskId = pushTask("Transaction data");
        assertEquals("Transaction data done", waitTaskStatus(taskId, "Transaction data done"));
        List<DemoLogRow> logs = loadDemoLogs(taskId);
        assertEquals(4, logs.size());

        DemoLogRow taskStart = logs.get(0);
        DemoLogRow dataLoaded = logs.get(1);
        DemoLogRow taskStop = logs.get(2);
        assertEquals("task", taskStart.type());
        assertEquals("TransactionData", dataLoaded.type());
        assertEquals("task", taskStop.type());
        assertEquals(taskStart.txId(), dataLoaded.txId());
        assertNotEquals(dataLoaded.txId(), taskStop.txId());
    }

    @Test
    void save_should_happen_in_the_same_transaction_with_task_stop() {
        resetServer();
        long taskId = pushTask("Transaction data");
        assertEquals("Transaction data done", waitTaskStatus(taskId, "Transaction data done"));
        List<DemoLogRow> logs = loadDemoLogs(taskId);
        assertEquals(4, logs.size());

        DemoLogRow taskStart = logs.get(0);
        DemoLogRow taskStop = logs.get(2);
        DemoLogRow dataSaved = logs.get(3);
        assertEquals("task", taskStart.type());
        assertEquals("task", taskStop.type());
        assertEquals("TransactionData", dataSaved.type());
        assertEquals(taskStop.txId(), dataSaved.txId());
        assertNotEquals(dataSaved.txId(), taskStart.txId());
    }

    @Test
    void task_updated_in_doer_method_should_run_in_its_own_transaction() {
        resetServer();
        long taskId = pushTask("Transaction update in method");
        assertEquals("Transaction update in method done", waitTaskStatus(taskId, "Transaction update in method done"));

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
    void doer_method_with_own_transactional_should_run_in_its_own_transaction() {
        resetServer();
        long taskId = pushTask("Transaction own");
        assertEquals("Transaction own done", waitTaskStatus(taskId, "Transaction own done"));

        List<DemoLogRow> logs = loadDemoLogs(taskId);
        assertEquals(4, logs.size());
        DemoLogRow taskStart = logs.get(0);
        DemoLogRow firstInMethod = logs.get(1);
        DemoLogRow secondInMethod = logs.get(2);
        DemoLogRow taskStop = logs.get(3);
        assertEquals("TransactionOwn", firstInMethod.type());
        assertEquals("TransactionOwn", secondInMethod.type());
        assertEquals(firstInMethod.txId(), secondInMethod.txId(), "both rows in the transaction of the method");
        assertNotEquals(taskStart.txId(), firstInMethod.txId());
        assertNotEquals(taskStop.txId(), firstInMethod.txId());
    }

    @Test
    void on_exception_data_should_not_be_saved() throws Exception {
        resetServer();
        long taskId = pushTask("Transaction data failing");
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
        DemoLogRow dataLoaded = logs.get(1);
        DemoLogRow taskStop = logs.get(2);
        assertEquals(taskStart.txId(), dataLoaded.txId());
        assertNotEquals(taskStart.txId(), taskStop.txId());
    }
}
