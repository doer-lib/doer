package com.doer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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

    @Test
    void monitor__should_run_task_queued_while_idle_after_its_delay() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            service.setExecutor(executor);
            HashMap<String, Duration> delays = new HashMap<>();
            delays.put("Delayed", Duration.ofSeconds(1));
            service.setupConcurrencyDomain("D1", 2, delays, new HashMap<>());
            service.start(true);
            // Nothing to run: the monitor waits for its next planned check, up to a minute
            waitUntilIdleMonitorWaits();

            Task task = createNewTask("Delayed");
            service.tst_tasks_by_id.put(task.getId(), task);
            service.triggerTaskReloadFromDb(task.getId());

            Instant ran = service.tst_runs.poll(10, TimeUnit.SECONDS);
            assertNotNull(ran, "the task did not run 10 s after it was queued; its delay is 1 s");
            assertFalse(ran.isBefore(task.getModified().plusSeconds(1)), "the task ran before its delay");
        } finally {
            service.stop();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /** Waits until the thread of the idle monitor waits for its next check. */
    private static void waitUntilIdleMonitorWaits() throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(5);
        while (Instant.now().isBefore(deadline)) {
            boolean waiting = Thread.getAllStackTraces().keySet().stream()
                    .anyMatch(t -> t.getName().equals("doer-idle-monitor")
                            && t.getState() == Thread.State.TIMED_WAITING);
            if (waiting) {
                return;
            }
            Thread.sleep(20);
        }
        fail("the idle monitor does not wait");
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
        /** Tasks "in db" for {@link #loadTask}. */
        Map<Long, Task> tst_tasks_by_id = new ConcurrentHashMap<>();
        /** When {@link #runTask} was called, one entry per call. */
        BlockingQueue<Instant> tst_runs = new LinkedBlockingQueue<>();

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
            tst_runs.add(Instant.now());
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
        public Task loadTask(long id) {
            return tst_tasks_by_id.get(id);
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
