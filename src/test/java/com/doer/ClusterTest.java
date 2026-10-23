package com.doer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;

/**
 * Two nodes on one database, faked in memory. The jobs of the executors of the nodes run in turn, one at a time, so
 * the nodes work on the same tasks at the same time, as in a cluster, but deterministically.
 */
public class ClusterTest {

    static final int TASKS = 5;
    /** A -> B -> C -> null: 3 steps of each task. */
    static final int STEPS = 3;

    FakeDb db = new FakeDb();
    Node node1 = new Node("node1", db);
    Node node2 = new Node("node2", db);

    @Test
    void nodes__should_run_each_step_once_and_drop_task_taken_by_other_node() {
        run(false);
    }

    @Test
    void nodes__should_drop_task_taken_by_other_node_while_it_runs() {
        run(true);
    }

    @Test
    void node__should_reload_task_changed_by_coordinated_update_after_taking_it_from_queue() {
        db.insert("A");
        node1.beforeRun = task -> {
            node1.beforeRun = null;
            // facilitateCoordinatedUpdate on this node: changes the task and asks to reload it, after the task was
            // taken from the queue; the reload runs at once, while the task is in progress here
            Task row = db.rows.get(task.getId());
            row.setStatus("B");
            row.setVersion(row.getVersion() + 1);
            node1.triggerTaskReloadFromDb(task.getId());
            while (node1.runJob()) {
            }
        };
        node1.start();

        while (node1.runJob()) {
        }

        assertEquals(1, node1.conflicts, "the start of the taken task conflicts");
        assertEquals(2, db.logs, "the steps B and C run after the reload");
        assertNull(db.rows.get(1L).getStatus());
    }

    /** With {@code overlap}, a doer method of one node runs a job of the other node, as if they ran at once. */
    private void run(boolean overlap) {
        if (overlap) {
            node1.other = node2;
            node2.other = node1;
        }
        for (int i = 0; i < TASKS; i++) {
            db.insert("A");
        }
        node1.start();
        node2.start();

        runJobsInTurn();

        assertEquals(TASKS * STEPS, db.logs, "every step of every task runs once");
        db.rows.values().forEach(row -> assertNull(row.getStatus(), "all tasks are done"));
        System.out.printf("overlap %s: conflicts: node1 %d, node2 %d; single task loads: node1 %d, node2 %d%n",
                overlap, node1.conflicts, node2.conflicts, node1.taskLoads, node2.taskLoads);
        db.trace.forEach(System.out::println);
        // Both nodes load all tasks at the start: a task is taken by one node, the other one gets a conflict once
        assertTrue(node1.conflicts + node2.conflicts <= TASKS,
                "conflicts: node1 " + node1.conflicts + ", node2 " + node2.conflicts + " for " + TASKS + " tasks");
    }

    private void runJobsInTurn() {
        for (int i = 0; i < 10_000; i++) {
            boolean ran = node1.runJob() | node2.runJob();
            if (!ran) {
                return;
            }
        }
        fail("the nodes do not stop");
    }

    /** Tables in memory: tasks and the count of task logs. Transactions roll back the tasks when they throw. */
    static class FakeDb {
        final Map<Long, Task> rows = new HashMap<>();
        final List<String> trace = new ArrayList<>();
        int logs;
        /** A doer method runs a job of the other node, but not the other way at the same time. */
        boolean overlapping;
        long lastId;

        void insert(String status) {
            Task row = new Task();
            row.setId(++lastId);
            row.setStatus(status);
            row.setCreated(Instant.now());
            row.setModified(Instant.now());
            rows.put(row.getId(), row);
        }

        Task load(long id) {
            Task row = rows.get(id);
            return row == null ? null : copy(row);
        }

        void inTransaction(Callable<Object> code) throws Exception {
            Map<Long, Task> before = new HashMap<>();
            rows.forEach((id, row) -> before.put(id, copy(row)));
            int logsBefore = logs;
            try {
                code.call();
            } catch (Throwable e) {
                rows.clear();
                rows.putAll(before);
                logs = logsBefore;
                throw e;
            }
        }

        boolean update(Task task) {
            Task row = rows.get(task.getId());
            if (row == null || row.getVersion() != task.getVersion()) {
                return false;
            }
            Task updated = copy(task);
            updated.setVersion(task.getVersion() + 1);
            updated.setModified(Instant.now());
            rows.put(task.getId(), updated);
            task.assignFieldsFrom(updated);
            return true;
        }

        static Task copy(Task task) {
            Task copy = new Task();
            copy.assignFieldsFrom(task);
            return copy;
        }
    }

    /** DoerService of one node, with the doer methods A -> B -> C -> null and its own executor. */
    static class Node extends DoerService {
        final String name;
        final FakeDb db;
        final LinkedList<Runnable> jobs = new LinkedList<>();
        int conflicts;
        int taskLoads;
        Node other;
        /** Called by runTask before the doer method, when set. */
        java.util.function.Consumer<Task> beforeRun;

        Node(String name, FakeDb db) {
            this.name = name;
            this.db = db;
            setExecutor(jobs::add);
            beanProviders.put(DoerService.class, () -> this);
            // The timer is never fired: the test runs the jobs in turn
            wakeUpTimerFactory = onWakeUp -> new ManualWakeUpTimer(onWakeUp, clock);
            setupConcurrencyDomain("D1", 2, Map.of("A", Duration.ZERO, "B", Duration.ZERO, "C", Duration.ZERO),
                    Map.of());
        }

        boolean runJob() {
            Runnable job = jobs.pollFirst();
            if (job == null) {
                return false;
            }
            job.run();
            return true;
        }

        @Override
        public void runTask(Task task) throws Exception {
            if (beforeRun != null) {
                beforeRun.accept(task);
            }
            callDoerMethod(task, () -> null, () -> {
                db.trace.add(name + " runs task " + task.getId() + " " + task.getStatus() + " v" + task.getVersion());
                if (other != null && !db.overlapping) {
                    db.overlapping = true;
                    other.runJob();
                    db.overlapping = false;
                }
                task.setStatus(switch (task.getStatus()) {
                    case "A" -> "B";
                    case "B" -> "C";
                    default -> null;
                });
                return null;
            }, () -> null, "Doer", "step", Duration.ofDays(1), null);
        }

        @Override
        public void runInTransaction(Callable<Object> code) throws Exception {
            db.inTransaction(code);
        }

        @Override
        public boolean updateAndBumpVersion(Task task) {
            long version = task.getVersion();
            boolean updated = db.update(task);
            if (!updated) {
                conflicts++;
                db.trace.add(name + " conflict on task " + task.getId() + " v" + version + ", in db v"
                        + db.rows.get(task.getId()).getVersion());
            }
            return updated;
        }

        @Override
        protected void insertTaskLog(long taskId, String initialStatus, String finalStatus, String className,
                String methodName, Integer durationMs, String exceptionType, String extraJson) {
            db.logs++;
        }

        @Override
        public Task loadTask(long id) {
            taskLoads++;
            Task task = db.load(id);
            db.trace.add(name + " reloads task " + id + " " + task.getStatus() + " v" + task.getVersion()
                    + (task.isInProgress() ? " in progress" : ""));
            return task;
        }

        @Override
        public List<Task> loadTasksFromDatabase(List<Integer> limits) {
            List<Task> result = new ArrayList<>();
            db.rows.values().forEach(row -> result.add(FakeDb.copy(row)));
            return result;
        }

        @Override
        public String createExtraJson(TaskSnapshot task, Throwable exception) {
            return null;
        }

        @Override
        protected Object _load(TaskSnapshot task, Class<?> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void _save(TaskSnapshot task, Class<?> type, Object data) {
            throw new UnsupportedOperationException();
        }
    }
}
