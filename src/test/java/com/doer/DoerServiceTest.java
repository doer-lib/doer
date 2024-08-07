package com.doer;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DoerServiceTest {

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
        service.self = service;
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

    private Task createNewTask(String status) throws Exception {
        Task task = new Task();
        task.setStatus(status);
        service.insert(task);
        return task;
    }

    static class TstDoerService extends DoerService {

        List<Integer> tst_limits;
        ArrayList<Task> tst_task_from_db = new ArrayList<>();

        @Override
        public void runInTransaction(Callable<Object> code) throws Exception {
            try (Connection con = getConnection()) {
                con.setAutoCommit(false);
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
                    con.setAutoCommit(true);
                }
            }
        }

        @Override
        public Connection getConnection() throws SQLException {
            return Utils.getPostgresDataSource().getConnection();
        }

        @Override
        public String createExtraJson(Task task, Exception exception) {
            return null;
        }

        @Override
        protected Object _callLoader(Class<?> klazz, Task task) throws Exception {
            return null;
        }

        @Override
        protected void _callUnLoader(Class<?> klazz, Task task, Object data) throws Exception {
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
