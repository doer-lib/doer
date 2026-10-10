package com.doer.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** {@code @ConcurrencyLimit} and the queues of DoerService. */
class ConcurrencyE2E extends E2eTestBase {

    @Test
    void concurrency_1_should_run_only_1_method_at_a_time() {
        resetServer();
        long task1 = pushTask("Need call taxi");
        pushTask("Need order pizza");
        pushTask("Time to cleanup");
        pushTask("Need call taxi");
        pushTask("Need order pizza");
        long task2 = pushTask("Time to cleanup");
        assertEquals("Cleanup finished", waitTaskStatus(task2, "Cleanup finished"));

        RestTask firstTask = restGetTask(task1);
        RestTask lastTask = restGetTask(task2);
        Duration timeToFinish6TasksBy100msEach = Duration.between(firstTask.created(), lastTask.modified());
        assertTrue(timeToFinish6TasksBy100msEach.compareTo(Duration.ofMillis(600)) >= 0);
    }

    @Test
    void concurrency_10_should_run_10_methods_in_parallel() {
        resetServer();
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        long lastTaskId = pushTask("Customer wants to make an order");
        assertEquals("Order accepted", waitTaskStatus(lastTaskId, "Order accepted"));
        Long timeSpentMs = selectLongValue(
                "SELECT (extract(EPOCH FROM max(modified) - min(created)) * 1000)::INT FROM tasks");
        Long timeSleptMs = selectLongValue("SELECT sum(duration_ms) FROM task_logs");
        assertTrue(timeSpentMs < 800);
        assertTrue(timeSleptMs >= 1000);
    }

    @Test
    void queues__should_grow_and_shrink() throws Exception {
        resetServer();
        pauseServer();
        LinkedList<Long> idList1 = new LinkedList<>(); // Failing but ready for retry
        LinkedList<Long> idList2 = new LinkedList<>(); // Asap tasks
        LinkedList<Long> idList3 = new LinkedList<>(); // Failing but not ready for retry
        for (int i = 0; i < 150; i++) {
            idList1.add(pushTask("Should send email"));
        }
        sqlUpdate("UPDATE tasks SET modified = now() - '6 min'::INTERVAL, " +
                "failing_since = now() - '10 min'::INTERVAL, " +
                "created = now() - '12 min'::INTERVAL");
        for (int i = 0; i < 300; i++) {
            idList2.add(pushTask("Customer wants to make an order"));
        }
        sqlUpdate("UPDATE tasks SET modified = now() - '6 min'::INTERVAL, " +
                "created = now() - '11 min'::INTERVAL " +
                "WHERE id >= " + idList2.peekFirst());
        for (int i = 0; i < 150; i++) {
            idList3.add(pushTask("Should send email"));
        }
        sqlUpdate("UPDATE tasks SET modified = now() - '1 min'::INTERVAL, " +
                "failing_since = now() - '10 min'::INTERVAL, " +
                "created = now() - '10 min'::INTERVAL " +
                "WHERE id >= " + idList3.peekFirst());
        long linesToSkip = E2eEnvironment.appLog(1).lines().count();
        resumeServer(false);
        for (Long id : idList2) {
            waitTaskStatus(id, "Order accepted");
        }
        Thread.sleep(250);
        checkReadyTasks();
        Thread.sleep(250);
        checkReadyTasks();
        Thread.sleep(250);
        List<List<Integer>> limitsTable = E2eEnvironment.appLog(1).lines()
                .skip(linesToSkip)
                .filter(line -> line.contains(" Limits: "))
                .map(ConcurrencyE2E::parseLimitsLine)
                .toList();

        // All readyToRetry and asapTasks should be updated exactly 1 time
        // No one failed but not ready task should be updated
        for (Long id : idList1) {
            assertEquals(1L, selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = " + id));
        }
        for (Long id : idList2) {
            assertEquals(1L, selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = " + id));
        }
        for (Long id : idList3) {
            assertEquals(0L, selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = " + id));
        }
        // All 150 readyToRetry should be updated first, but few of ASAP tasks may also be updated
        // (When readyToRetry queue become empty and DoerService start loading bigger queue from DB
        // it continues processing other queues - in our case ASAP queue).
        Long lastReadyToRetryLog = selectLongValue(
                "SELECT max(id) FROM task_logs WHERE task_id <= " + idList1.peekLast());
        Long aheadOfTime = selectLongValue("SELECT count(*) FROM task_logs WHERE id < " + lastReadyToRetryLog
                + " AND task_id >= " + idList2.peekFirst());
        int maxAheadOfTime = 50;
        // DUMP logs just for debug purpose
        if (aheadOfTime > maxAheadOfTime) {
            System.out.println(selectStringValue("select json_agg(row_to_json(x)) from (select task_id, created, "
                    + "final_status, duration_ms from task_logs order by created) x;"));
        }
        System.out.println("Limits");
        for (List<Integer> limits : limitsTable) {
            System.out.println(limits);
        }
        assertTrue(aheadOfTime <= maxAheadOfTime,
                "Expected only few ASAP task updated ahead of time " + aheadOfTime + " <= " + maxAheadOfTime);

        List<Integer> firstRow = limitsTable.get(0);
        List<Integer> middleRow = limitsTable.get(limitsTable.size() / 2);
        List<Integer> lastRow = limitsTable.get(limitsTable.size() - 1);
        for (int i = 0; i < firstRow.size(); i++) {
            assertEquals(10, firstRow.get(i), "Test should start with minimal limits for all queues");
        }
        int retryQueueIndex = maxValueIndex(middleRow);
        int asapQueueIndex = maxValueIndex(lastRow);
        for (List<Integer> row : limitsTable) {
            for (int i = 0; i < row.size(); i++) {
                int value = row.get(i);
                assertTrue(value >= 10, "Minimal limit value should be 10. But found " + value);
                if (i != retryQueueIndex && i != asapQueueIndex) {
                    assertEquals(10, value,
                            "Queues, not affected by the test, should have limit = 10. But found " + value);
                }
            }
        }
        assertTrue(firstRow.get(retryQueueIndex) < middleRow.get(retryQueueIndex),
                "ReadyToRetry queue limits should grow till middleRow");
        assertTrue(middleRow.get(retryQueueIndex) > lastRow.get(retryQueueIndex),
                "ReadyToRetry queue limits should shrink from middleRow till lastRow");
        assertEquals(firstRow.get(asapQueueIndex), middleRow.get(asapQueueIndex),
                "Asap queue should remain minimal till middleRow");
        assertTrue(middleRow.get(asapQueueIndex) < lastRow.get(asapQueueIndex),
                "Asap queue should grow from middleRow till lastRow");
    }

    private static int maxValueIndex(List<Integer> row) {
        int maxValue = row.get(0);
        int indexOfMaxValue = 0;
        for (int i = 1; i < row.size(); i++) {
            int value = row.get(i);
            if (value > maxValue) {
                maxValue = value;
                indexOfMaxValue = i;
            }
        }
        return indexOfMaxValue;
    }

    /** The limits of the queues in a log line {@code ... Limits: [10, 12, 10]}. */
    private static List<Integer> parseLimitsLine(String line) {
        String limits = line.split("Limits: \\[")[1].split("]")[0];
        return Arrays.stream(limits.split(",\\s*")).map(Integer::valueOf).toList();
    }
}
