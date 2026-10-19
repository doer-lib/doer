package com.doer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class TaskQueuesTest {

    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    static final Duration RELOAD_INTERVAL = Duration.ofMinutes(10);
    static final int MIN_QUEUE_SIZE = 2;
    static final int MAX_ALL_QUEUES_SIZE = 100;

    TaskQueues taskQueues = new TaskQueues();
    long nextTaskId = 1000;

    @Test
    void activeStatuses__should_be_statuses_of_all_domains() {
        addDomain("D1", "A", "B");
        assertEquals(Set.of("A", "B"), taskQueues.activeStatuses());

        addDomain("D2", "C");
        assertEquals(Set.of("A", "B", "C"), taskQueues.activeStatuses());
    }

    @Test
    void domainOf__should_return_domain_of_status_or_null() {
        ConcurrencyDomain d1 = addDomain("D1", "A");
        ConcurrencyDomain d2 = addDomain("D2", "B");

        assertSame(d1, taskQueues.domainOf("A"));
        assertSame(d2, taskQueues.domainOf("B"));
        assertNull(taskQueues.domainOf("X"));
        assertNull(taskQueues.domainOf(null));
    }

    @Test
    void startNext__should_mark_task_in_progress_until_finish() {
        ConcurrencyDomain domain = addDomain("D1", "A");
        Task task = task("A", 10);
        taskQueues.requeue(task);

        assertSame(task, taskQueues.startNext(domain, T0));
        assertTrue(taskQueues.isInProgress(task.getId()));
        assertTrue(taskQueues.hasTasksInProgress());
        assertEquals(1, domain.inProgressCount());

        taskQueues.finish(task, domain);
        assertFalse(taskQueues.isInProgress(task.getId()));
        assertFalse(taskQueues.hasTasksInProgress());
        assertEquals(0, domain.inProgressCount());
    }

    @Test
    void startNext__should_return_null_when_domain_has_no_free_slot_or_no_ready_task() {
        ConcurrencyDomain domain = addDomain("D1", "A"); // limit 2
        assertNull(taskQueues.startNext(domain, T0));

        taskQueues.requeue(task("A", 10));
        taskQueues.requeue(task("A", 20));
        taskQueues.requeue(task("A", 30));
        assertNotNull(taskQueues.startNext(domain, T0));
        assertNotNull(taskQueues.startNext(domain, T0));

        assertNull(taskQueues.startNext(domain, T0));
    }

    @Test
    void requeue__should_put_task_to_domain_of_its_status() {
        ConcurrencyDomain d1 = addDomain("D1", "A");
        ConcurrencyDomain d2 = addDomain("D2", "B");
        Task task = task("A", 10);
        taskQueues.requeue(task);
        taskQueues.startNext(d1, T0);
        taskQueues.finish(task, d1);
        task.setStatus("B");

        assertSame(d2, taskQueues.requeue(task));
        assertSame(task, taskQueues.startNext(d2, T0));
    }

    @Test
    void requeue__should_drop_task_with_final_status() {
        ConcurrencyDomain domain = addDomain("D1", "A");

        assertNull(taskQueues.requeue(task(null, 10)));
        assertNull(taskQueues.startNext(domain, T0));
    }

    @Test
    void replaceTask__should_replace_queued_copy() {
        ConcurrencyDomain domain = addDomain("D1", "A");
        Task queued = task("A", 10);
        taskQueues.requeue(queued);
        Task fromDb = task("A", 10);
        fromDb.setId(queued.getId());
        fromDb.setVersion(1);

        assertSame(domain, taskQueues.replaceTask(fromDb));
        assertEquals(List.of(fromDb), List.copyOf(domain.queues().get(0).tasks()));
    }

    @Test
    void nextLimits__should_keep_capacities_of_queues_in_use() {
        addDomain("D1", "A");
        addDomain("D2", "C");

        assertEquals(List.of(2, 2, 2, 2), nextLimits(T0));
        assertEquals(List.of(2, 2, 2, 2), nextLimits(null));
    }

    @Test
    void nextLimits__should_double_first_drained_queue() {
        ConcurrencyDomain d1 = addDomain("D1", "A");
        ConcurrencyDomain d2 = addDomain("D2", "C");
        drain(d1.queues().get(0));
        drain(d2.queues().get(0));

        assertEquals(List.of(4, 2, 2, 2), nextLimits(T0));
    }

    @Test
    void nextLimits__should_not_double_drained_queue_over_max_all_queues_size() {
        ConcurrencyDomain d1 = addDomain("D1", "A");
        addDomain("D2", "C");
        drain(d1.queues().get(0));

        assertEquals(List.of(4, 2, 2, 2), taskQueues.nextLimits(T0, T0, RELOAD_INTERVAL, MIN_QUEUE_SIZE, 10));
        assertEquals(List.of(2, 2, 2, 2), taskQueues.nextLimits(T0, T0, RELOAD_INTERVAL, MIN_QUEUE_SIZE, 9));
    }

    @Test
    void nextLimits__should_halve_queue_used_less_than_half_when_other_queue_is_drained() {
        ConcurrencyDomain d1 = addDomain("D1", "A");
        addDomain("D2", "C");
        drain(d1.queues().get(0));
        fill(d1.queues().get(1), 10, 6);

        assertEquals(List.of(4, 5, 2, 2), nextLimits(T0));
    }

    @Test
    void nextLimits__should_halve_queue_used_less_than_half_after_80_percent_of_reload_interval() {
        ConcurrencyDomain d1 = addDomain("D1", "A");
        addDomain("D2", "C");
        fill(d1.queues().get(1), 10, 6);

        assertEquals(List.of(2, 10, 2, 2), nextLimits(T0.minusSeconds(8 * 60 - 1)));
        assertEquals(List.of(2, 5, 2, 2), nextLimits(T0.minusSeconds(8 * 60)));
    }

    @Test
    void nextLimits__should_not_double_drained_queue_after_80_percent_of_reload_interval() {
        ConcurrencyDomain d1 = addDomain("D1", "A");
        addDomain("D2", "C");
        drain(d1.queues().get(0));

        assertEquals(List.of(2, 2, 2, 2), nextLimits(T0.minusSeconds(8 * 60)));
    }

    @Test
    void replaceFromDb__should_fill_queues_and_set_capacities_by_loaded_rows() {
        ConcurrencyDomain domain = addDomain("D1", "A");
        Task a1 = task("A", 10);
        Task a2 = task("A", 20);
        Task failing = task("A", 5);
        failing.setFailingSince(T0);

        List<Task> toReCheck = taskQueues.replaceFromDb(List.of(4, 10), List.of(a2, failing, a1), 2);

        assertEquals(List.of(), toReCheck);
        assertEquals(List.of(a1, a2), List.copyOf(domain.queues().get(0).tasks()));
        assertEquals(List.of(failing), List.copyOf(domain.queues().get(1).tasks()));
        assertEquals("4/2, 10/1", taskQueues.describeLoad(List.of(4, 10)));
        // asap: 2 of 4 loaded, the db may have more; failing: 1 of 10 loaded, the capacity is halved
        assertEquals(List.of(4, 5), nextLimits(T0));
        assertFalse(domain.queues().get(0).isDrained());
    }

    @Test
    void replaceFromDb__should_remember_that_db_has_more_when_limit_is_reached() {
        ConcurrencyDomain domain = addDomain("D1", "A");
        Task a1 = task("A", 10);

        taskQueues.replaceFromDb(List.of(1, 2), List.of(a1), 2);
        taskQueues.startNext(domain, T0);

        assertTrue(domain.queues().get(0).isDrained());
    }

    @Test
    void replaceFromDb__should_return_rows_in_progress_in_db_to_recheck() {
        ConcurrencyDomain domain = addDomain("D1", "A");
        Task queued = task("A", 10);
        Task inProgressInDb = task("A", 5);
        inProgressInDb.setInProgress(true);

        List<Task> toReCheck = taskQueues.replaceFromDb(List.of(2, 2), List.of(inProgressInDb, queued), 2);

        assertEquals(List.of(inProgressInDb), toReCheck);
        assertEquals(List.of(queued), List.copyOf(domain.queues().get(0).tasks()));
    }

    @Test
    void replaceFromDb__should_not_count_rows_in_progress_as_loaded_rows_of_queue() {
        ConcurrencyDomain domain = addDomain("D1", "A");
        Task ready = task("A", 10);
        Task inProgress1 = task("A", 5);
        inProgress1.setInProgress(true);
        Task inProgress2 = task("A", 6);
        inProgress2.setInProgress(true);

        // The in_progress rows come from the last select, not from the select of the queue with LIMIT 2
        taskQueues.replaceFromDb(List.of(2, 2), List.of(ready, inProgress1, inProgress2), 2);
        taskQueues.startNext(domain, T0);

        assertFalse(domain.queues().get(0).isDrained(), "the select of the queue returned all its rows");
    }

    @Test
    void replaceFromDb__should_skip_rows_older_than_queued_tasks_and_tasks_in_progress_here() {
        ConcurrencyDomain domain = addDomain("D1", "A");
        Task running = task("A", 5);
        taskQueues.requeue(running);
        taskQueues.startNext(domain, T0);
        Task queued = task("A", 10);
        queued.setVersion(2);
        taskQueues.requeue(queued);
        Task runningRow = copy(running, 0);
        Task olderRow = copy(queued, 1);
        Task newRow = task("A", 20);

        List<Task> toReCheck = taskQueues.replaceFromDb(List.of(10, 10), List.of(runningRow, olderRow, newRow), 2);

        assertEquals(List.of(olderRow), toReCheck);
        assertEquals(List.of(newRow), List.copyOf(domain.queues().get(0).tasks()));
    }

    @Test
    void finish__should_tell_that_reload_was_requested_while_task_was_in_progress() {
        ConcurrencyDomain domain = addDomain("D1", "A");
        Task first = task("A", 10);
        Task second = task("A", 20);
        taskQueues.requeue(first);
        taskQueues.requeue(second);
        taskQueues.startNext(domain, T0);
        taskQueues.startNext(domain, T0);

        taskQueues.reloadWhenFinished(first.getId());

        assertTrue(taskQueues.finish(first, domain));
        assertFalse(taskQueues.finish(second, domain));
    }

    @Test
    void replaceFromDb__should_reload_task_in_progress_here_when_it_finishes() {
        ConcurrencyDomain domain = addDomain("D1", "A");
        Task running = task("A", 5);
        taskQueues.requeue(running);
        taskQueues.startNext(domain, T0);

        taskQueues.replaceFromDb(List.of(10, 10), List.of(copy(running, 0)), 2);

        assertTrue(taskQueues.finish(running, domain), "the row was skipped, the task may have changed meanwhile");
    }

    @Test
    void earliestReadyTime__should_be_min_ready_time_of_first_tasks() {
        addDomain("D1", Map.of("A", Duration.ZERO, "B", Duration.ofSeconds(30), "C", Duration.ofSeconds(10)));
        assertNull(taskQueues.earliestReadyTime());

        taskQueues.requeue(task("B", 0));
        taskQueues.requeue(task("C", 25));
        assertEquals(T0.plusSeconds(30), taskQueues.earliestReadyTime());

        taskQueues.requeue(task("A", 100));
        assertEquals(Instant.MIN, taskQueues.earliestReadyTime());
    }

    private List<Integer> nextLimits(Instant lastSelectTime) {
        return taskQueues.nextLimits(T0, lastSelectTime, RELOAD_INTERVAL, MIN_QUEUE_SIZE, MAX_ALL_QUEUES_SIZE);
    }

    /** Domain with limit 2 whose statuses are processed as soon as possible. */
    private ConcurrencyDomain addDomain(String name, String... statuses) {
        Map<String, Duration> delays = new HashMap<>();
        for (String status : statuses) {
            delays.put(status, Duration.ZERO);
        }
        return addDomain(name, delays);
    }

    private ConcurrencyDomain addDomain(String name, Map<String, Duration> delays) {
        return taskQueues.addDomain(name, 2, delays, Map.of(), MIN_QUEUE_SIZE);
    }

    /** Makes the queue drained: its capacity is 2, the buffer is empty and the db has more. */
    private static void drain(SubQueue queue) {
        queue.replace(List.of(), 2, 2, 2);
    }

    /** Sets the capacity of the queue to {@code capacity} and puts {@code size} tasks to it. */
    private void fill(SubQueue queue, int capacity, int size) {
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            tasks.add(task("A", i));
        }
        queue.replace(tasks, capacity, capacity, 2);
    }

    private Task task(String status, int createdSec) {
        Task task = new Task();
        task.setId(nextTaskId++);
        task.setStatus(status);
        task.setCreated(T0.plusSeconds(createdSec));
        task.setModified(T0.plusSeconds(createdSec));
        return task;
    }

    private static Task copy(Task task, int version) {
        Task copy = new Task();
        copy.assignFieldsFrom(task);
        copy.setVersion(version);
        return copy;
    }
}
