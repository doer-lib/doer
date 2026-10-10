package com.doer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Scheduling logic of DoerService, without database: tasks "from db" are given by the test. */
public class DoerServiceTest {

    TstDoerService service;
    LinkedList<Runnable> executorJobs;
    long nextTaskId = 1000;

    @BeforeEach
    void init() {
        executorJobs = new LinkedList<>();
        service = new TstDoerService();
        service.setExecutor(executorJobs::add);
        service.setSelfReference(service);
    }

    void runAllExecutorJobs() {
        while (!executorJobs.isEmpty()) {
            executorJobs.pollFirst().run();
        }
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
            // 3 queues (asap, delayed 2 min, retry 5 min)
            HashMap<String, Duration> delays = new HashMap<>();
            delays.put("A", Duration.ZERO);
            delays.put("B", Duration.ZERO);
            delays.put("B_delayed", Duration.ofMinutes(2));
            service.setupConcurrencyDomain("D1", 2, delays, new HashMap<>());
        }
        {
            // 4 queues (asap, delayed 20 sec, retry 20 sec, retry 5 min)
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

    /** Task as inserted to db: queues order tasks by created and modified. */
    private Task createNewTask(String status) {
        Task task = new Task();
        task.setId(nextTaskId++);
        task.setStatus(status);
        Instant now = Instant.now();
        task.setCreated(now);
        task.setModified(now);
        return task;
    }

    static class TstDoerService extends DoerService {

        List<Integer> tst_limits;
        ArrayList<Task> tst_task_from_db = new ArrayList<>();

        @Override
        public void runInTransaction(Callable<Object> code) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String createExtraJson(Task task, Exception exception) {
            return null;
        }

        @Override
        protected Object _load(Task task, Class<?> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void _save(Task task, Class<?> type, Object data) {
            throw new UnsupportedOperationException();
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
        public List<Task> loadTasksFromDatabase(List<Integer> limits) {
            tst_limits = limits;
            ArrayList<Task> returnValue = new ArrayList<>(tst_task_from_db);
            tst_task_from_db.clear();
            return returnValue;
        }
    }
}
