package com.doer;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Scheduling logic of DoerService, without database: tasks "from db" are given by the test. */
public class DoerServiceTest {

    TstDoerService service;
    LinkedList<Runnable> executorJobs;
    /** Time of the service, moved by the test; the system clock with {@link #useThreadPool}. */
    TestClock clock = new TestClock(Instant.parse("2026-01-01T00:00:00Z"));
    /** Timer of the scheduler, fired by the test; the real one with {@link #useThreadPool}. */
    ManualWakeUpTimer timer;
    /** Executor of the tests with real threads (see {@link #useThreadPool}); stopped after the test. */
    ExecutorService threadPool;
    long nextTaskId = 1000;

    @BeforeEach
    void init() {
        executorJobs = new LinkedList<>();
        service = new TstDoerService();
        service.setExecutor(executorJobs::add);
        service.beanProviders.put(DoerService.class, () -> service);
        service.clock = clock;
        service.wakeUpTimerFactory = onWakeUp -> timer = new ManualWakeUpTimer(onWakeUp, clock);
    }

    @AfterEach
    void stopThreadPool() throws InterruptedException {
        if (threadPool != null) {
            service.stop();
            // Interrupts the tasks held by tst_on_run
            threadPool.shutdownNow();
            assertTrue(threadPool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /** Real threads, real timer and real time. */
    void useThreadPool() {
        threadPool = Executors.newCachedThreadPool();
        service.setExecutor(threadPool);
        service.clock = Clock.systemUTC();
        service.wakeUpTimerFactory = ScheduledWakeUpTimer::new;
    }

    void runAllExecutorJobs() {
        while (!executorJobs.isEmpty()) {
            executorJobs.pollFirst().run();
        }
    }

    /** Moves the clock to the planned wake-up of the scheduler, fires it and runs the jobs it hands out. */
    void advanceToWakeUp() {
        Instant plannedAt = timer.plannedAt();
        assertNotNull(plannedAt, "no wake-up is planned");
        clock.set(plannedAt);
        assertTrue(timer.fireIfDue());
        runAllExecutorJobs();
    }

    @Test
    void reloadQueues__should_calculate_limits() {
        {
            HashMap<String, Duration> delays = new HashMap<>();
            delays.put("S1", Duration.ZERO);
            service.setupConcurrencyDomain("D1", 2, delays, new HashMap<>());
        }
        service.start();

        runAllExecutorJobs();

        assertEquals(Arrays.asList(10, 10), service.tst_limits);
    }

    @Test
    void start__should_reloadQueues_and_start_processing_loaded_tasks() throws Exception {
        service.setMinQueueSize(5);
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

        service.start();
        runAllExecutorJobs();

        assertEquals(Arrays.asList(5, 5, 5, 5, 5, 5, 5), service.tst_limits);
        assertNull(task.getStatus());
    }

    @Test
    void reloadQueues__should_increase_limit() throws Exception {
        service.setMinQueueSize(2);
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
        service.start();
        runAllExecutorJobs();

        assertEquals(Arrays.asList(4, 2, 2, 2), service.tst_limits); // actually it is second reloadQueues call
    }

    @Test
    void start__should_reloadQueues_after_stop_skipped_scheduled_reload() {
        {
            HashMap<String, Duration> delays = new HashMap<>();
            delays.put("S1", Duration.ZERO);
            service.setupConcurrencyDomain("D1", 2, delays, new HashMap<>());
        }
        service.start();
        service.stop();
        runAllExecutorJobs();
        assertNull(service.tst_limits);

        service.start();
        runAllExecutorJobs();

        assertEquals(Arrays.asList(10, 10), service.tst_limits);
    }

    @Test
    void scheduler__should_run_task_queued_while_idle_after_its_delay() {
        service.setupConcurrencyDomain("D1", 2, Map.of("Delayed", Duration.ofSeconds(1)), Map.of());
        service.start();
        runAllExecutorJobs();

        Task task = createNewTask("Delayed");
        service.tst_tasks_by_id.put(task.getId(), task);
        service.triggerTaskReloadFromDb(task.getId());
        runAllExecutorJobs();

        assertTrue(service.tst_run_statuses.isEmpty(), "the task ran before its delay");
        assertEquals(Duration.ofSeconds(1), timer.plannedDelay());

        advanceToWakeUp();

        assertEquals(List.of("Delayed"), List.copyOf(service.tst_run_statuses));
    }

    @Test
    void scheduler__should_plan_periodic_load_after_reload_interval() {
        service.setQueueReloadInterval(Duration.ofSeconds(30));
        service.setupConcurrencyDomain("D1", 2, Map.of("S", Duration.ZERO), Map.of());
        service.start();
        runAllExecutorJobs();
        assertEquals(1, service.tst_loads.get());

        assertEquals(Duration.ofSeconds(30), timer.plannedDelay());
        advanceToWakeUp();

        assertEquals(2, service.tst_loads.get());
    }

    @Test
    void scheduler__should_wake_up_at_least_once_a_minute() {
        service.setupConcurrencyDomain("D1", 2, Map.of("S", Duration.ZERO), Map.of());
        service.start();
        runAllExecutorJobs();

        // The next load is due in 10 min, the reset of stalled tasks in 30 min
        assertEquals(Duration.ofMinutes(1), timer.plannedDelay());
        advanceToWakeUp();

        assertEquals(Duration.ofMinutes(1), timer.plannedDelay());
    }

    @Test
    void scheduler__should_start_delayed_task_of_free_domain_while_other_domain_is_busy() throws Exception {
        useThreadPool();
        service.setupConcurrencyDomain("D1", 1, Map.of("Long", Duration.ZERO), Map.of());
        service.setupConcurrencyDomain("D2", 1, Map.of("Delayed", Duration.ofSeconds(1)), Map.of());
        CountDownLatch longStarted = holdTasksOf("Long");
        service.tst_task_from_db.add(createNewTask("Long"));
        service.start();
        assertTrue(longStarted.await(10, TimeUnit.SECONDS));

        Task delayed = createNewTask("Delayed");
        service.tst_tasks_by_id.put(delayed.getId(), delayed);
        service.triggerTaskReloadFromDb(delayed.getId());

        assertTrue(waitForRun("Delayed", Duration.ofSeconds(5)),
                "the task of D2 did not run 5 s after it was queued, while D1 was busy; its delay is 1 s");
    }

    @Test
    void scheduler__should_reset_stalled_tasks_while_task_is_in_progress() throws Exception {
        useThreadPool();
        service.stalledTaskCheckInterval = Duration.ofMillis(500);
        service.setupConcurrencyDomain("D1", 1, Map.of("Long", Duration.ZERO), Map.of());
        CountDownLatch longStarted = holdTasksOf("Long");
        service.tst_task_from_db.add(createNewTask("Long"));
        service.start();
        assertTrue(longStarted.await(10, TimeUnit.SECONDS));

        assertNotNull(service.tst_stalled_resets.poll(5, TimeUnit.SECONDS),
                "no reset of stalled tasks 5 s after start while a task was in progress; the interval is 0.5 s");
    }

    @Test
    void scheduler__should_reload_queues_periodically_while_task_is_in_progress() throws Exception {
        useThreadPool();
        service.setQueueReloadInterval(Duration.ofMillis(500));
        service.setupConcurrencyDomain("D1", 1, Map.of("Long", Duration.ZERO), Map.of());
        CountDownLatch longStarted = holdTasksOf("Long");
        service.tst_task_from_db.add(createNewTask("Long"));
        service.start();
        assertTrue(longStarted.await(10, TimeUnit.SECONDS));
        int loads = service.tst_loads.get();

        assertTrue(waitUntil(() -> service.tst_loads.get() > loads, Duration.ofSeconds(5)),
                "no load 5 s after start while a task was in progress; the reload interval is 0.5 s");
    }

    @Test
    void scheduler__should_reload_queues_after_stalled_tasks_reset() throws Exception {
        useThreadPool();
        service.stalledTaskCheckInterval = Duration.ofMillis(500);
        service.tst_stalled_reset_result = 1;
        service.setupConcurrencyDomain("D1", 1, Map.of("S", Duration.ZERO), Map.of());
        service.start();
        assertTrue(waitUntil(() -> service.tst_loads.get() == 1, Duration.ofSeconds(5)));

        assertNotNull(service.tst_stalled_resets.poll(5, TimeUnit.SECONDS));

        assertTrue(waitUntil(() -> service.tst_loads.get() > 1, Duration.ofSeconds(3)),
                "no load after a reset of stalled tasks that reset a task");
    }

    @Test
    void scheduler__should_queue_task_finished_on_other_node_soon_after_load() throws Exception {
        useThreadPool();
        service.setupConcurrencyDomain("D1", 1, Map.of("S", Duration.ZERO), Map.of());
        Task elsewhere = createNewTask("S");
        elsewhere.setInProgress(true);
        service.tst_task_from_db.add(elsewhere);
        service.tst_tasks_by_id.put(elsewhere.getId(), copyOf(elsewhere));
        service.start();
        // The load skips the task in progress on the other node, and the first reconciliation finds it unchanged
        assertTrue(waitUntil(() -> service.tst_loaded_ids.contains(elsewhere.getId()), Duration.ofSeconds(5)));

        // The other node finishes its step
        Task finished = copyOf(elsewhere);
        finished.setInProgress(false);
        finished.setVersion(elsewhere.getVersion() + 1);
        finished.setModified(Instant.now());
        service.tst_tasks_by_id.put(finished.getId(), finished);

        assertTrue(waitForRun("S", Duration.ofSeconds(5)),
                "the task finished on the other node did not run 5 s later, with no other event");
    }

    @Test
    void scheduler__should_reconcile_skipped_tasks_with_one_query_outside_lock() throws Exception {
        useThreadPool();
        service.setupConcurrencyDomain("D1", 1, Map.of("S", Duration.ZERO), Map.of());
        Task first = inProgressElsewhere("S");
        Task second = inProgressElsewhere("S");

        service.start();

        Set<Long> loaded = service.tst_load_tasks_calls.poll(5, TimeUnit.SECONDS);
        assertEquals(Set.of(first.getId(), second.getId()), loaded);
        assertEquals(0, service.tst_db_calls_under_lock.get(), "a query ran under the lock of DoerService");
    }

    @Test
    void scheduler__should_drop_reconciliation_of_older_load() throws Exception {
        useThreadPool();
        service.setupConcurrencyDomain("D1", 1, Map.of("S", Duration.ZERO), Map.of());
        inProgressElsewhere("S");
        CountDownLatch reconciliationStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        service.tst_on_load_tasks = ids -> {
            if (reconciliationStarted.getCount() > 0) {
                reconciliationStarted.countDown();
                assertTrue(release.await(10, TimeUnit.SECONDS));
            }
        };
        service.start();
        assertTrue(reconciliationStarted.await(5, TimeUnit.SECONDS));

        // While the reconciliation of the first load waits for its query, the second load skips another task
        Task next = inProgressElsewhere("S");
        service.triggerQueuesReload();
        assertTrue(waitUntil(() -> {
            synchronized (service) {
                return service.tasksWithUncertainState.stream().anyMatch(t -> t.getId() == next.getId());
            }
        }, Duration.ofSeconds(5)));
        // The other node finishes it
        Task finished = copyOf(next);
        finished.setInProgress(false);
        finished.setVersion(next.getVersion() + 1);
        service.tst_tasks_by_id.put(finished.getId(), finished);
        // The query of the first reconciliation returns without the task of the second load
        release.countDown();

        assertTrue(waitForRun("S", Duration.ofSeconds(5)),
                "the task skipped by the second load was dropped by the reconciliation of the first load");
    }

    @Test
    void scheduler__should_run_loaded_tasks_while_next_load_is_requested() {
        service.setupConcurrencyDomain("D1", 1, Map.of("S", Duration.ZERO), Map.of());
        Task task = createNewTask("S");
        service.tst_tasks_by_id.put(task.getId(), copyOf(task));
        service.tst_task_from_db.add(copyOf(task));
        // The task leaves its status when it runs: the next load does not select it
        service.tst_on_run = t -> service.tst_task_from_db.clear();
        // Each load selects the task and gets a request for one more load, until the task runs
        service.tst_on_select = new TstDoerService.Hook() {
            @Override
            public void run() {
                if (service.tst_run_statuses.isEmpty() && service.tst_loads.get() < 10) {
                    service.tst_task_from_db.add(copyOf(task));
                    service.tst_on_select = this;
                    service.triggerQueuesReload();
                }
            }
        };
        service.start();

        runAllExecutorJobs();

        assertEquals(List.of("S"), List.copyOf(service.tst_run_statuses));
        assertTrue(service.tst_loads.get() < 10, "the task waited for the end of the loads");
    }

    @Test
    void triggerStalledTaskReset__should_not_submit_job_when_not_running() {
        service.triggerStalledTaskReset();

        assertTrue(executorJobs.isEmpty(), "a job was submitted to the executor");
    }

    @Test
    void stop__should_stop_planned_work() throws Exception {
        useThreadPool();
        service.setupConcurrencyDomain("D1", 1, Map.of("S", Duration.ZERO), Map.of());
        service.start();
        waitUntilSchedulerWaits();
        int loads = service.tst_loads.get();
        // The reset is due, but nothing is told about it; no tick runs before stop
        synchronized (service) {
            service.lastStalledTaskCheckTime = Instant.now().minus(Duration.ofHours(1));
            service.stop();
        }

        assertNull(service.tst_stalled_resets.poll(2, TimeUnit.SECONDS), "stalled tasks were reset after stop");
        assertEquals(loads, service.tst_loads.get(), "the queues were loaded after stop");
        assertTrue(waitUntil(() -> Thread.getAllStackTraces().keySet().stream()
                .noneMatch(t -> t.getName().equals("doer-scheduler")), Duration.ofSeconds(5)),
                "the thread of the scheduler is alive after stop");
    }

    @Test
    void triggerTaskReloadFromDb__should_reload_task_again_after_load_that_selected_before_it() {
        service.setupConcurrencyDomain("D1", 1, Map.of("S", Duration.ZERO), Map.of());
        Task task = createNewTask("S");
        // The task is inserted after the select of the load, and its reload ends before the load replaces the buffers
        service.tst_on_select = () -> {
            service.tst_tasks_by_id.put(task.getId(), task);
            service.triggerTaskReloadFromDb(task.getId());
            executorJobs.pollLast().run();
        };
        service.start();

        runAllExecutorJobs();

        assertEquals(List.of("S"), List.copyOf(service.tst_run_statuses), "the load dropped the task");
    }

    @Test
    void facilitateCoordinatedUpdate__should_queue_copy_of_committed_row_without_loading_it() throws Exception {
        service.setupConcurrencyDomain("D1", 1, Map.of("S", Duration.ZERO), Map.of());
        service.start();
        runAllExecutorJobs();
        Task task = createNewTask("New");
        service.tst_tasks_by_id.put(task.getId(), task);

        Task updated = service.facilitateCoordinatedUpdate(task.getId(), Duration.ZERO, false, t -> t.setStatus("S"));
        // The caller may change the task it gets: the queues have their own copy
        updated.setStatus("Changed by the caller");
        runAllExecutorJobs();

        assertEquals(List.of("S"), List.copyOf(service.tst_run_statuses));
        assertFalse(service.tst_loaded_ids.contains(task.getId()), "the committed task was loaded again");
    }

    @Test
    void facilitateCoordinatedUpdate__should_reload_task_after_load_that_selected_before_it() {
        service.setupConcurrencyDomain("D1", 1, Map.of("S", Duration.ZERO), Map.of());
        Task task = createNewTask("New");
        service.tst_tasks_by_id.put(task.getId(), task);
        // The update commits after the select of the load, and queues the task before the load replaces the buffers
        service.tst_on_select = () -> service.facilitateCoordinatedUpdate(task.getId(), Duration.ZERO, false,
                t -> t.setStatus("S"));
        service.start();

        runAllExecutorJobs();

        assertEquals(List.of("S"), List.copyOf(service.tst_run_statuses), "the load dropped the task");
    }

    @Test
    void facilitateCoordinatedUpdate__should_wait_for_end_of_run_here_without_polling() throws Exception {
        useThreadPool();
        service.setupConcurrencyDomain("D1", 1, Map.of("Long", Duration.ZERO), Map.of());
        Task task = createNewTask("Long");
        service.tst_task_from_db.add(copyOf(task));
        Task inProgress = copyOf(task);
        inProgress.setInProgress(true);
        service.tst_tasks_by_id.put(task.getId(), inProgress);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        // The row is in progress while the task runs, and the run commits it before it finishes
        service.tst_on_run = t -> {
            started.countDown();
            release.await();
            service.tst_tasks_by_id.put(task.getId(), copyOf(task));
        };
        service.start();
        assertTrue(started.await(10, TimeUnit.SECONDS));

        Future<Task> update = threadPool.submit(() -> service.facilitateCoordinatedUpdate(task.getId(),
                Duration.ofSeconds(10), false, t -> t.setStatus("Done")));
        assertTrue(waitUntil(() -> service.tst_row_locks.get() == 1, Duration.ofSeconds(5)));
        // Polling would try again 50, 150 and 350 ms after the first attempt
        Thread.sleep(500);
        release.countDown();

        assertEquals("Done", update.get(5, TimeUnit.SECONDS).getStatus());
        assertEquals(2, service.tst_row_locks.get(), "the database was polled while the task ran here");
    }

    @Test
    void processNextTask__should_not_ask_for_load_of_drained_queue_while_load_runs() {
        service.setupConcurrencyDomain("D1", 1, Map.of("S", Duration.ZERO), Map.of());
        // As many as the LIMIT of the sub-queue: the database may have more, and the last start drains the sub-queue
        for (int i = 0; i < 10; i++) {
            service.tst_task_from_db.add(createNewTask("S"));
        }
        service.tst_on_run = task -> {
            if (service.tst_run_statuses.size() == 9) {
                synchronized (service) {
                    // As if a load were running when the last task starts
                    service.queuesReloadState = DoerService.QueuesReloadState.RUNNING;
                }
            }
        };
        service.start();

        runAllExecutorJobs();

        assertEquals(10, service.tst_run_statuses.size());
        assertEquals(DoerService.QueuesReloadState.RUNNING, service.queuesReloadState,
                "the running load refills the drained sub-queue, no other load is needed");
    }

    @Test
    void processNextTask__should_start_task_moved_to_other_domain_while_its_old_domain_is_busy() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch othersStarted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        try {
            service.setExecutor(executor);
            service.setupConcurrencyDomain("D1", 3, Map.of("A", Duration.ZERO), Map.of());
            service.setupConcurrencyDomain("D2", 2, Map.of("B", Duration.ZERO), Map.of());
            Task first = createNewTask("A");
            Task second = createNewTask("A");
            Task third = createNewTask("A");
            // first runs to the end only when second and third are in progress in D1, and they stay there
            service.tst_on_run = task -> {
                if (task == first) {
                    firstStarted.countDown();
                    assertTrue(othersStarted.await(10, TimeUnit.SECONDS));
                } else if ("A".equals(task.getStatus())) {
                    othersStarted.countDown();
                    assertTrue(release.await(10, TimeUnit.SECONDS));
                }
            };
            service.tst_task_from_db.addAll(List.of(first, second, third));
            // The load starts the three tasks in D1
            service.start();
            assertTrue(firstStarted.await(10, TimeUnit.SECONDS));

            // first is now in D2 with the status B: D2 has free slots, though D1 has none
            assertTrue(waitForRun("B", Duration.ofSeconds(5)), "the task in D2 did not run while D1 was busy");
        } finally {
            release.countDown();
            service.stop();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Keeps the tasks of the status in progress until the end of the test (see {@link #stopThreadPool}). The latch is
     * released when the first of them starts.
     */
    private CountDownLatch holdTasksOf(String status) {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        service.tst_on_run = task -> {
            if (status.equals(task.getStatus())) {
                started.countDown();
                never.await();
            }
        };
        return started;
    }

    private static boolean waitUntil(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(20);
        }
        return condition.getAsBoolean();
    }

    /** A task in progress on another node: the next load returns it, and the load skips it. */
    private Task inProgressElsewhere(String status) {
        Task task = createNewTask(status);
        task.setInProgress(true);
        service.tst_task_from_db.add(task);
        service.tst_tasks_by_id.put(task.getId(), copyOf(task));
        return task;
    }

    private static Task copyOf(Task task) {
        Task copy = new Task();
        copy.assignFieldsFrom(task);
        return copy;
    }

    /** Waits until runTask is called with a task of the status. */
    private boolean waitForRun(String status, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            String run = service.tst_run_statuses.poll(20, TimeUnit.MILLISECONDS);
            if (status.equals(run)) {
                return true;
            }
        }
        return false;
    }

    @Test
    void bean__should_look_up_bean_on_first_use_once_and_drop_provider() {
        List<String> lookups = new ArrayList<>();
        service.beanProviders.put(StringBuilder.class, () -> {
            lookups.add("lookup");
            return new StringBuilder("bean");
        });
        assertEquals(List.of(), lookups);

        StringBuilder first = service.bean(StringBuilder.class);
        StringBuilder second = service.bean(StringBuilder.class);

        assertSame(first, second);
        assertEquals(List.of("lookup"), lookups);
        assertFalse(service.beanProviders.containsKey(StringBuilder.class));
    }

    @Test
    void bean__should_keep_provider_when_it_returns_null_or_throws() {
        LinkedList<Object> results = new LinkedList<>(List.of("null", "fail", "bean"));
        service.beanProviders.put(String.class, () -> switch ((String) results.poll()) {
            case "null" -> null;
            case "fail" -> throw new IllegalStateException("not ready");
            default -> "bean";
        });

        assertNull(service.bean(String.class));
        assertEquals("not ready", assertThrows(IllegalStateException.class, () -> service.bean(String.class))
                .getMessage());
        assertTrue(service.beanProviders.containsKey(String.class));
        assertEquals("bean", service.bean(String.class));
        assertFalse(service.beanProviders.containsKey(String.class));
    }

    @Test
    void bean__should_fail_without_provider() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.bean(String.class));

        assertEquals("No provider of bean java.lang.String was injected", e.getMessage());
    }

    /**
     * Waits until the first load is done and nothing is due for a while: the timer of the scheduler waits for more than
     * a second.
     */
    private void waitUntilSchedulerWaits() throws InterruptedException {
        BooleanSupplier waits = () -> {
            synchronized (service) {
                Duration delay = service.wakeUpTimer == null ? null : service.wakeUpTimer.plannedDelay();
                return service.lastQueueSelectTime != null
                        && service.queuesReloadState == DoerService.QueuesReloadState.IDLE
                        && delay != null && delay.compareTo(Duration.ofSeconds(1)) > 0;
            }
        };
        assertTrue(waitUntil(waits, Duration.ofSeconds(5)), "the scheduler does not wait");
    }

    /** Task as inserted to db: queues order tasks by created and modified. */
    private Task createNewTask(String status) {
        Task task = new Task();
        task.setId(nextTaskId++);
        task.setStatus(status);
        Instant now = service.clock.instant();
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
        /** Status of the task {@link #runTask} was called with, one entry per call. */
        BlockingQueue<String> tst_run_statuses = new LinkedBlockingQueue<>();
        /** Called by {@link #runTask} before it changes the status: may block to keep the task in progress. */
        volatile RunHook tst_on_run = task -> {
        };
        /** Number of calls of {@link #loadTasksFromDatabase}. */
        AtomicInteger tst_loads = new AtomicInteger();
        /** Ids of the tasks loaded one by one or by ids, one entry per load. */
        BlockingQueue<Long> tst_loaded_ids = new LinkedBlockingQueue<>();
        /** When {@link #resetStalledInProgressTasks} was called, one entry per call. */
        BlockingQueue<Instant> tst_stalled_resets = new LinkedBlockingQueue<>();
        /** Number of tasks {@link #resetStalledInProgressTasks} reports as reset. */
        volatile int tst_stalled_reset_result;
        /** Ids of each call of {@link #loadTasks}. */
        BlockingQueue<Set<Long>> tst_load_tasks_calls = new LinkedBlockingQueue<>();
        /** Called by {@link #loadTasks} before it loads: may block to keep the query running. */
        volatile IdsHook tst_on_load_tasks = ids -> {
        };
        /** Called once by {@link #loadTasksFromDatabase} after the select, before the load replaces the buffers. */
        volatile Hook tst_on_select;
        /** Number of calls of {@link #selectTaskForUpdate}: the attempts of facilitateCoordinatedUpdate. */
        AtomicInteger tst_row_locks = new AtomicInteger();
        /** Number of the calls of the database methods made under the lock of DoerService. */
        AtomicInteger tst_db_calls_under_lock = new AtomicInteger();

        interface IdsHook {
            void run(Collection<Long> ids) throws Exception;
        }

        interface Hook {
            void run() throws Exception;
        }

        private void checkNoLock() {
            if (Thread.holdsLock(this)) {
                tst_db_calls_under_lock.incrementAndGet();
            }
        }

        interface RunHook {
            void run(Task task) throws Exception;
        }

        @Override
        public void runInTransaction(Callable<Object> code) throws Exception {
            code.call();
        }

        /** The row of {@link #tst_tasks_by_id}, as the database would lock it. */
        @Override
        protected Task selectTaskForUpdate(long taskId, boolean allowInProgress) {
            tst_row_locks.incrementAndGet();
            Task row = tst_tasks_by_id.get(taskId);
            return row == null || !allowInProgress && row.isInProgress() ? null : copyOf(row);
        }

        @Override
        public boolean updateAndBumpVersion(Task task) {
            Task row = tst_tasks_by_id.get(task.getId());
            if (row == null || row.getVersion() != task.getVersion()) {
                return false;
            }
            Task updated = copyOf(task);
            updated.setVersion(task.getVersion() + 1);
            updated.setModified(clock.instant());
            tst_tasks_by_id.put(task.getId(), updated);
            task.assignFieldsFrom(updated);
            return true;
        }

        @Override
        public long writeTaskLog(Long taskId, String initialStatus, String finalStatus, String className,
                String methodName, String exceptionType, String extraJson, Integer durationMs) {
            return 1;
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

        @Override
        public void runTask(Task task) throws Exception {
            tst_runs.add(Instant.now());
            String status = task.getStatus();
            tst_run_statuses.add(String.valueOf(status));
            tst_on_run.run(task);
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
            checkNoLock();
            tst_loaded_ids.add(id);
            return tst_tasks_by_id.get(id);
        }

        @Override
        public Map<Long, Task> loadTasks(Collection<Long> ids) throws SQLException {
            checkNoLock();
            tst_load_tasks_calls.add(Set.copyOf(ids));
            try {
                tst_on_load_tasks.run(ids);
            } catch (Exception e) {
                throw new SQLException(e);
            }
            Map<Long, Task> result = new HashMap<>();
            for (long id : ids) {
                tst_loaded_ids.add(id);
                Task task = tst_tasks_by_id.get(id);
                if (task != null) {
                    result.put(id, task);
                }
            }
            return result;
        }

        @Override
        public int resetStalledInProgressTasks(Duration timeout) {
            checkNoLock();
            tst_stalled_resets.add(Instant.now());
            return tst_stalled_reset_result;
        }

        @Override
        public List<Task> loadTasksFromDatabase(List<Integer> limits) {
            checkNoLock();
            tst_loads.incrementAndGet();
            tst_limits = limits;
            ArrayList<Task> returnValue = new ArrayList<>(tst_task_from_db);
            tst_task_from_db.clear();
            Hook onSelect = tst_on_select;
            if (onSelect != null) {
                tst_on_select = null;
                try {
                    onSelect.run();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            return returnValue;
        }
    }
}
