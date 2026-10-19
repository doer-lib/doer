package com.doer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SubQueueTest {

    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    long nextTaskId = 1000;

    @Test
    void accepts__should_match_failing_flag_and_status() {
        SubQueue asap = new SubQueue(false, Duration.ZERO, List.of("A", "B"), 10);
        SubQueue retry = new SubQueue(true, Duration.ofMinutes(5), List.of("A"), 10);

        assertTrue(asap.accepts(task("A", 0)));
        assertTrue(asap.accepts(task("B", 0)));
        assertFalse(asap.accepts(task("C", 0)));
        assertFalse(asap.accepts(failing(task("A", 0))));
        assertTrue(retry.accepts(failing(task("A", 0))));
        assertFalse(retry.accepts(task("A", 0)));
        assertFalse(asap.accepts(task(null, 0)));
    }

    @Test
    void offer__should_keep_asap_tasks_by_created() {
        SubQueue queue = new SubQueue(false, Duration.ZERO, List.of("A"), 10);
        Task second = task("A", 20, 0);
        Task first = task("A", 10, 30);

        queue.offer(second);
        queue.offer(first);

        assertSame(first, queue.poll());
        assertSame(second, queue.poll());
    }

    @Test
    void offer__should_keep_delayed_tasks_by_modified() {
        SubQueue queue = new SubQueue(false, Duration.ofMinutes(1), List.of("A"), 10);
        Task second = task("A", 10, 30);
        Task first = task("A", 20, 25);

        queue.offer(second);
        queue.offer(first);

        assertSame(first, queue.poll());
        assertSame(second, queue.poll());
    }

    @Test
    void offer__should_drop_last_task_and_remember_that_db_has_more_when_buffer_overflows() {
        SubQueue queue = new SubQueue(false, Duration.ZERO, List.of("A"), 2);
        Task t1 = task("A", 10);
        Task t2 = task("A", 20);

        queue.offer(t1);
        queue.offer(t2);
        queue.offer(task("A", 30));

        assertEquals(List.of(t1, t2), List.copyOf(queue.tasks()));
        queue.poll();
        queue.poll();
        assertTrue(queue.isDrained());
    }

    @Test
    void offer__should_leave_task_in_db_when_db_has_more_and_task_goes_after_buffer_end() {
        SubQueue queue = new SubQueue(false, Duration.ZERO, List.of("A"), 2);
        Task t1 = task("A", 10);
        Task t3 = task("A", 30);
        queue.offer(t1);
        queue.offer(t3);
        queue.offer(task("A", 40)); // overflow: the db has more

        queue.offer(task("A", 50));
        assertEquals(List.of(t1, t3), List.copyOf(queue.tasks()));

        Task t2 = task("A", 20);
        queue.offer(t2);
        assertEquals(List.of(t1, t2), List.copyOf(queue.tasks()));
    }

    @Test
    void offer__should_leave_task_in_db_when_buffer_is_empty_and_db_has_more() {
        SubQueue queue = new SubQueue(false, Duration.ZERO, List.of("A"), 2);
        queue.replace(List.of(), 2, 2, 2);

        queue.offer(task("A", 10));

        assertEquals(0, queue.size());
        assertTrue(queue.isDrained());
    }

    @Test
    void readyTime__should_be_min_for_asap_and_modified_plus_delay_otherwise() {
        Task task = task("A", 0, 30);
        SubQueue asap = new SubQueue(false, Duration.ZERO, List.of("A"), 10);
        SubQueue delayed = new SubQueue(false, Duration.ofMinutes(2), List.of("A"), 10);
        SubQueue retry = new SubQueue(true, Duration.ofSeconds(20), List.of("A"), 10);
        SubQueue retryWithoutDelay = new SubQueue(true, Duration.ZERO, List.of("A"), 10);

        assertEquals(Instant.MIN, asap.readyTime(task));
        assertEquals(T0.plusSeconds(30 + 120), delayed.readyTime(task));
        assertEquals(T0.plusSeconds(30 + 20), retry.readyTime(task));
        assertEquals(T0.plusSeconds(30), retryWithoutDelay.readyTime(task));
    }

    @Test
    void isHeadReady__should_compare_ready_time_of_first_task_with_now() {
        SubQueue queue = new SubQueue(false, Duration.ofSeconds(10), List.of("A"), 10);
        assertNull(queue.headReadyTime());
        assertFalse(queue.isHeadReady(T0.plusSeconds(1000)));

        queue.offer(task("A", 0, 5));

        assertEquals(T0.plusSeconds(15), queue.headReadyTime());
        assertFalse(queue.isHeadReady(T0.plusSeconds(14)));
        assertTrue(queue.isHeadReady(T0.plusSeconds(15)));
    }

    @Test
    void remove__should_drop_task_by_id() {
        SubQueue queue = new SubQueue(false, Duration.ZERO, List.of("A"), 10);
        Task t1 = task("A", 10);
        Task t2 = task("A", 20);
        queue.offer(t1);
        queue.offer(t2);

        queue.remove(t1.getId());

        assertEquals(List.of(t2), List.copyOf(queue.tasks()));
    }

    @Test
    void nextLimit__should_halve_capacity_of_queue_used_less_than_half_when_shrinking() {
        SubQueue queue = queueWithTasks(10, 6);

        assertEquals(5, queue.nextLimit(true, 2));
        assertEquals(10, queue.nextLimit(false, 2));
        assertEquals(10, queueWithTasks(10, 5).nextLimit(true, 2));
    }

    @Test
    void nextLimit__should_not_go_below_min_size() {
        assertEquals(3, queueWithTasks(4, 3).nextLimit(true, 3));
        assertEquals(3, queueWithTasks(3, 3).nextLimit(true, 3));
    }

    @Test
    void replace__should_set_capacity_and_db_has_more_by_loaded_rows() {
        SubQueue queue = new SubQueue(false, Duration.ZERO, List.of("A"), 2);

        queue.replace(List.of(), 10, 4, 2);
        assertEquals(5, queue.capacity());
        assertFalse(queue.isDrained());

        queue.replace(List.of(), 10, 5, 2);
        assertEquals(10, queue.capacity());
        assertFalse(queue.isDrained());

        queue.replace(List.of(), 10, 10, 2);
        assertEquals(10, queue.capacity());
        assertTrue(queue.isDrained());

        queue.replace(List.of(), 4, 1, 3);
        assertEquals(3, queue.capacity());
    }

    @Test
    void replace__should_replace_buffer() {
        SubQueue queue = new SubQueue(false, Duration.ZERO, List.of("A"), 10);
        queue.offer(task("A", 10));
        Task loaded = task("A", 20);

        queue.replace(List.of(loaded), 10, 1, 2);

        assertEquals(List.of(loaded), List.copyOf(queue.tasks()));
    }

    /** Asap queue whose capacity is {@code capacity}, with {@code size} tasks in the buffer. */
    private SubQueue queueWithTasks(int capacity, int size) {
        SubQueue queue = new SubQueue(false, Duration.ZERO, List.of("A"), capacity);
        for (int i = 0; i < size; i++) {
            queue.offer(task("A", i));
        }
        return queue;
    }

    private Task task(String status, int createdSec) {
        return task(status, createdSec, createdSec);
    }

    private Task task(String status, int createdSec, int modifiedSec) {
        Task task = new Task();
        task.setId(nextTaskId++);
        task.setStatus(status);
        task.setCreated(T0.plusSeconds(createdSec));
        task.setModified(T0.plusSeconds(modifiedSec));
        return task;
    }

    private static Task failing(Task task) {
        task.setFailingSince(task.getModified());
        return task;
    }
}
