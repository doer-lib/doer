package com.doer;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DoerServiceITCase {

    TstDoerService service;
    LinkedList<Runnable> executorJobs;

    @BeforeAll
    static void createdDb() throws Exception {
        try (Connection con = Utils.getPostgresDataSource().getConnection()) {
            Utils.createDbSchema(con);
        }
    }

    @BeforeEach
    void init() throws Exception {
        executorJobs = new LinkedList<>();
        service = new TstDoerService();
        service.setExecutor(executorJobs::add);
        service.setSelfReference(service);
        Utils.sqlUpdate("DELETE FROM task_logs");
        Utils.sqlUpdate("DELETE FROM tasks");
    }
    void runAllExecutorJobs() {
        while(!executorJobs.isEmpty()) {
            Runnable runnable = executorJobs.pollFirst();
            runnable.run();
        }
    }

    @Test
    void loadTask__should_read_db_values() throws Exception {
        Utils.sqlUpdate(
                "INSERT INTO tasks (id, status, failing_since, in_progress) VALUES (743, 'test status', now(), TRUE)");

        Task task = service.loadTask(743);

        assertEquals(743L, task.getId());
        assertEquals("test status", task.getStatus());
        assertTrue(task.isInProgress());
        assertNotNull(task.getCreated());
        assertNotNull(task.getModified());
        assertNotNull(task.getFailingSince());
        assertEquals(0, task.getVersion());
    }

    @Test
    void insertTask__should_write_db() throws Exception {
        Task task = new Task();
        task.setStatus("test status 3");
        Instant failingSince = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        task.setFailingSince(failingSince);

        service.insert(task);

        assertNotNull(task.getId());
        assertNotNull(task.getCreated());
        assertEquals(task.getCreated(), task.getModified());
        assertEquals(failingSince, task.getFailingSince());
        assertFalse(task.isInProgress());
    }

    @Test
    void generateId__should_return_new_id() throws Exception {
        long id1 = service.generateId();
        long id2 = service.generateId();
        assertTrue(id1 >= 1000);
        assertTrue(id2 > id1);
    }

    @Test
    void updateAndBumpVersion__should_update_task() throws Exception {
        Utils.sqlUpdate("INSERT INTO tasks (id, status, version) VALUES (744, 'test status 744', 3)");
        Task task = new Task();
        task.setId(744L);
        task.setVersion(3);
        task.setStatus("Updated test status 744");

        service.updateAndBumpVersion(task);

        assertEquals(4, task.getVersion());
        assertNotNull(task.getCreated());
        assertNotNull(task.getModified());
        Task dbTask = service.loadTask(744L);
        assertEquals("Updated test status 744", dbTask.getStatus());
        assertEquals(4, dbTask.getVersion());
    }

    @Test
    void reloadTasksFromDb__should_calculate_limits() {
        {
            HashMap<String, Duration> delays = new HashMap<>();
            delays.put("S1", Duration.ZERO);
            service.setupConcurrencyDomain("D1", 2, delays, new HashMap<>());
        }
        service.start(false);

        service.reloadTasksFromDb();

        assertEquals(Arrays.asList(10, 10), service.tst_limits);
    }

    @Test
    void start__should_reloadTasksFromDb_and_start_processing_loaded_tasks() throws Exception {
        service.setMinSingleQueueSize(5);
        {
            // 3 queues (asap, delayed 2 min, retryed 5 min)
            HashMap<String, Duration> delays = new HashMap<>();
            delays.put("A", Duration.ZERO);
            delays.put("B", Duration.ZERO);
            delays.put("B_delayed", Duration.ofMinutes(2));
            service.setupConcurrencyDomain("D1", 2, delays, new HashMap<>());
        }
        {
            // 4 quees (asap, delayed 20 sec, retry 20 sec, retry 5 min)
            HashMap<String, Duration> delays = new HashMap<>();
            delays.put("C", Duration.ZERO);
            delays.put("D_Delayed", Duration.ofSeconds(20));
            HashMap<String, Duration> retryDelays = new HashMap<>();
            retryDelays.put("C", Duration.ofSeconds(20));
            service.setupConcurrencyDomain("D2", 2, delays, retryDelays);
        }
        Task task = createNewTask("A");
        service.tst_task_from_db.add(task);

        service.start(false);
        runAllExecutorJobs();

        assertEquals(Arrays.asList(5, 5, 5, 5, 5, 5, 5), service.tst_limits);
        assertNull(task.getStatus());
    }

    @Test
    void reloadTasksFromDb__should_increase_limit() throws Exception {
        service.setMinSingleQueueSize(2);
        {
            HashMap<String, Duration> delays = new HashMap<>();
            delays.put("A", Duration.ZERO);
            service.setupConcurrencyDomain("D1", 2, delays, new HashMap<>());
        }
        {
            HashMap<String, Duration> delays = new HashMap<>();
            delays.put("C", Duration.ZERO);
            service.setupConcurrencyDomain("D2", 2, delays, new HashMap<>());
        }
        service.tst_task_from_db.addAll(Arrays.asList(createNewTask("A"), createNewTask("A")));
        service.start(false);
        runAllExecutorJobs();

        assertEquals(Arrays.asList(4, 2, 2, 2), service.tst_limits); // actually it is second reloadTasksFromDb call
    }

    @Test
    void loadTasks__should_skip_nulls() throws Exception {
        assertTrue(service.loadTasks(Collections.emptyList()).isEmpty());
        assertTrue(service.loadTasks(Collections.singleton(null)).isEmpty());
    }

    @Test
    void loadTasks__should_skip_duplicates() throws Exception {
        Task task = createNewTask("Test for load tasks");
        Long taskId = task.getId();

        Map<Long, Task> result1 = service.loadTasks(Collections.singleton(taskId));
        Map<Long, Task> result2 = service.loadTasks(Arrays.asList(taskId, taskId));
        Map<Long, Task> result3 = service.loadTasks(Arrays.asList(taskId, taskId, taskId));

        assertEquals(1, result1.size());
        assertEquals(1, result2.size());
        assertEquals(1, result3.size());

        assertEquals(taskId, result1.get(taskId).getId());
        assertEquals(taskId, result2.get(taskId).getId());
        assertEquals(taskId, result3.get(taskId).getId());
    }

    @Test
    void loadTasks__should_skip_missing() throws Exception {
        Long missingTask = 9839892L;

        Map<Long, Task> result = service.loadTasks(Collections.singleton(missingTask));

        assertEquals(0, result.size());
    }

    @Test
    void facilitateCoordinatedUpdate__should_update_task_and_write_log() throws Exception {
        Utils.sqlUpdate("INSERT INTO tasks (id, status, failing_since, version) VALUES (801, 'A', now(), 5)");

        Task task = service.facilitateCoordinatedUpdate(801, Duration.ZERO, false, t -> t.setStatus("B"));

        assertEquals("B", task.getStatus());
        Task dbTask = service.loadTask(801);
        assertEquals("B", dbTask.getStatus());
        assertFalse(dbTask.isInProgress());
        assertNull(dbTask.getFailingSince());
        assertEquals(6, dbTask.getVersion());
        assertEquals(1L, Utils.selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = 801"));
        assertEquals("A>B>DoerServiceITCase>facilitateCoordinatedUpdate__should_update_task_and_write_log>",
                Utils.selectStringValue("SELECT initial_status || '>' || final_status || '>' || class_name || '>' || "
                        + "method_name || '>' || coalesce(exception_type, '') FROM task_logs WHERE task_id = 801"));
    }

    @Test
    void facilitateCoordinatedUpdate__should_rollback_when_updater_throws() throws Exception {
        Utils.sqlUpdate("INSERT INTO tasks (id, status) VALUES (802, 'A')");

        RuntimeException e = assertThrows(RuntimeException.class,
                () -> service.facilitateCoordinatedUpdate(802, Duration.ZERO, false, t -> {
                    t.setStatus("B");
                    throw new RuntimeException("updater failed");
                }));

        assertEquals("updater failed", e.getMessage());
        Task dbTask = service.loadTask(802);
        assertEquals("A", dbTask.getStatus());
        assertEquals(0, dbTask.getVersion());
        assertEquals(0L, Utils.selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = 802"));
    }

    @Test
    void facilitateCoordinatedUpdate__should_throw_when_task_not_found() {
        assertThrows(TaskNotFoundException.class,
                () -> service.facilitateCoordinatedUpdate(9839893L, Duration.ZERO, true, t -> fail()));
    }

    @Test
    void facilitateCoordinatedUpdate__should_throw_when_in_progress_and_hijack_not_allowed() throws Exception {
        Utils.sqlUpdate("INSERT INTO tasks (id, status, in_progress) VALUES (803, 'A', TRUE)");

        assertThrows(TaskInProgressException.class,
                () -> service.facilitateCoordinatedUpdate(803, Duration.ofMillis(200), false, t -> fail()));

        Task dbTask = service.loadTask(803);
        assertTrue(dbTask.isInProgress());
        assertEquals(0, dbTask.getVersion());
        assertEquals(0L, Utils.selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = 803"));
    }

    @Test
    void facilitateCoordinatedUpdate__should_wait_until_task_is_not_in_progress() throws Exception {
        Utils.sqlUpdate("INSERT INTO tasks (id, status, in_progress) VALUES (804, 'A', TRUE)");
        Thread doerCompletion = new Thread(() -> {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                return;
            }
            Utils.sqlUpdate("UPDATE tasks SET in_progress = FALSE, status = 'B', version = version + 1 WHERE id = 804");
        });
        doerCompletion.start();

        Task task = service.facilitateCoordinatedUpdate(804, Duration.ofSeconds(10), false,
                t -> t.setStatus(t.getStatus() + "C"));
        doerCompletion.join();

        assertEquals("BC", task.getStatus());
        assertEquals("BC", service.loadTask(804).getStatus());
        assertEquals(0L, Utils.selectLongValue(
                "SELECT count(*) FROM task_logs WHERE task_id = 804 AND exception_type = 'TaskHijacked'"));
    }

    @Test
    void facilitateCoordinatedUpdate__should_hijack_in_progress_task() throws Exception {
        Utils.sqlUpdate("INSERT INTO tasks (id, status, in_progress) VALUES (805, 'A', TRUE)");
        Task runningDoerCopy = service.loadTask(805);

        List<Boolean> inProgressSeen = new ArrayList<>();

        Task task = service.facilitateCoordinatedUpdate(805, Duration.ZERO, true, t -> {
            inProgressSeen.add(t.isInProgress());
            t.setStatus("B");
        });

        assertEquals(Arrays.asList(true), inProgressSeen);
        assertEquals("B", task.getStatus());
        Task dbTask = service.loadTask(805);
        assertFalse(dbTask.isInProgress());
        assertEquals(1, dbTask.getVersion());
        assertEquals("A>A>TaskHijacked", Utils.selectStringValue(
                "SELECT initial_status || '>' || final_status || '>' || exception_type FROM task_logs "
                        + "WHERE task_id = 805 AND exception_type IS NOT NULL"));
        assertEquals("A>B", Utils.selectStringValue(
                "SELECT initial_status || '>' || final_status FROM task_logs "
                        + "WHERE task_id = 805 AND exception_type IS NULL"));
        // the hijacked doer method can't save its result
        runningDoerCopy.setInProgress(false);
        runningDoerCopy.setStatus("Doer result");
        assertFalse(service.updateAndBumpVersion(runningDoerCopy));
        assertEquals("B", service.loadTask(805).getStatus());
    }

    @Test
    void facilitateCoordinatedUpdate__should_rollback_hijack_when_updater_throws() throws Exception {
        Utils.sqlUpdate("INSERT INTO tasks (id, status, in_progress) VALUES (806, 'A', TRUE)");

        assertThrows(IllegalStateException.class,
                () -> service.facilitateCoordinatedUpdate(806, Duration.ZERO, true, t -> {
                    throw new IllegalStateException();
                }));

        Task dbTask = service.loadTask(806);
        assertTrue(dbTask.isInProgress());
        assertEquals(0, dbTask.getVersion());
        assertEquals(0L, Utils.selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = 806"));
    }

    @Test
    void facilitateCoordinatedUpdate__should_load_and_unload_parameter() throws Exception {
        Utils.sqlUpdate("INSERT INTO tasks (id, status) VALUES (807, 'A')");
        List<String> received = new ArrayList<>();

        service.facilitateCoordinatedUpdate(807, Duration.ZERO, false, String.class, (t, data) -> {
            received.add(data);
            t.setStatus("B");
        });

        assertEquals(Arrays.asList("loaded-A"), received);
        // unloader runs after the task is written (version bumped from 0 to 1)
        assertEquals(Arrays.asList("loaded-A@v1"), service.tst_unloaded);
        assertEquals("B", service.loadTask(807).getStatus());
    }

    @Test
    void facilitateCoordinatedUpdate__should_fail_without_loader() throws Exception {
        Utils.sqlUpdate("INSERT INTO tasks (id, status) VALUES (808, 'A')");

        assertThrows(IllegalArgumentException.class,
                () -> service.facilitateCoordinatedUpdate(808, Duration.ZERO, false, Integer.class,
                        (t, data) -> t.setStatus("B")));

        assertEquals("A", service.loadTask(808).getStatus());
        assertEquals(0L, Utils.selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = 808"));
    }

    private Task createNewTask(String status) throws Exception {
        Task task = new Task();
        task.setStatus(status);
        service.insert(task);
        return task;
    }

    static class TstDoerService extends DoerService {

        List<Integer> tst_limits;
        ArrayList<Task> tst_task_from_db = new ArrayList<>();

        // Statements made through getConnection() inside runInTransaction() share the transaction's connection
        ThreadLocal<Connection> txConnection = new ThreadLocal<>();

        @Override
        public void runInTransaction(Callable<Object> code) throws Exception {
            try (Connection con = Utils.getPostgresDataSource().getConnection()) {
                con.setAutoCommit(false);
                txConnection.set(con);
                try {
                    code.call();
                    con.commit();
                } catch (Exception e) {
                    try {
                        con.rollback();
                    } catch (Exception e2) {
                        e2.printStackTrace();
                    }
                    throw e;
                } finally {
                    txConnection.remove();
                    con.setAutoCommit(true);
                }
            }
        }

        @Override
        public Connection getConnection() throws SQLException {
            Connection con = txConnection.get();
            if (con == null) {
                return Utils.getPostgresDataSource().getConnection();
            }
            // close() must not close the transaction's connection
            return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { Connection.class },
                    (proxy, method, args) -> {
                        if ("close".equals(method.getName())) {
                            return null;
                        }
                        try {
                            return method.invoke(con, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        }

        @Override
        public String createExtraJson(Task task, Exception exception) {
            return null;
        }

        // Loader/unloader for String: loads "loaded-<status>", records what was unloaded
        List<String> tst_unloaded = new ArrayList<>();

        @Override
        protected Object _load(Task task, Class<?> type) throws Exception {
            if (String.class.equals(type)) {
                return "loaded-" + task.getStatus();
            }
            throw new IllegalArgumentException("No @DoerLoader for " + type.getName());
        }

        @Override
        protected void _unload(Task task, Class<?> type, Object data) throws Exception {
            if (String.class.equals(type)) {
                tst_unloaded.add(data + "@v" + task.getVersion());
            }
        }

        @Override
        public void runTask(Task task) throws Exception {
            String status = task.getStatus();
            if ("A".equals(status)) {
                task.setStatus("B");
            } else if ("B".equals(status)) {
                task.setStatus("C");
            } else {
                task.setStatus(null);
            }
        }

        @Override
        public List<Task> loadTasksFromDatabase(List<Integer> limits) throws SQLException, IOException {
            tst_limits = limits;
            ArrayList<Task> returnValue = new ArrayList<>(tst_task_from_db);
            tst_task_from_db.clear();
            return returnValue;
        }
    }
}
