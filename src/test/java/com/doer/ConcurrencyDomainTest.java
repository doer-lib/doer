package com.doer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ConcurrencyDomainTest {

    static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    long nextTaskId = 1000;

    @Test
    void queues__should_be_asap_then_delayed_then_failing_by_delay() {
        ConcurrencyDomain domain = new ConcurrencyDomain("D", 2,
                Map.of("C", Duration.ZERO, "D_Delayed", Duration.ofSeconds(20)),
                Map.of("C", Duration.ofSeconds(20)), 10);

        List<SubQueue> queues = domain.queues();

        assertEquals(4, queues.size());
        assertAccepts(queues.get(0), task("C", 0));
        assertAccepts(queues.get(1), task("D_Delayed", 0));
        assertAccepts(queues.get(2), failing(task("C", 0)));
        assertAccepts(queues.get(3), failing(task("D_Delayed", 0)));
        assertEquals(T0.plusSeconds(20), queues.get(2).readyTime(task("C", 0)));
        assertEquals(T0.plusSeconds(5 * 60), queues.get(3).readyTime(task("D_Delayed", 0)));
    }

    @Test
    void queues__should_be_ordered_by_delay_and_group_statuses_of_one_delay() {
        ConcurrencyDomain domain = new ConcurrencyDomain("D", 2,
                Map.of("A", Duration.ofMinutes(2), "B", Duration.ofSeconds(30), "C", Duration.ZERO,
                        "D", Duration.ofSeconds(30)),
                Map.of(), 10);

        List<SubQueue> queues = domain.queues();

        assertEquals(4, queues.size());
        assertAccepts(queues.get(0), task("C", 0));
        assertAccepts(queues.get(1), task("B", 0), task("D", 0));
        assertAccepts(queues.get(2), task("A", 0));
        assertAccepts(queues.get(3), failing(task("A", 0)), failing(task("B", 0)), failing(task("C", 0)),
                failing(task("D", 0)));
    }

    @Test
    void statuses__should_be_keys_of_delays_and_retry_delays() {
        ConcurrencyDomain domain = new ConcurrencyDomain("D", 2,
                Map.of("B", Duration.ZERO, "A", Duration.ZERO), Map.of("C", Duration.ofSeconds(20)), 10);

        assertEquals(List.of("A", "B", "C"), List.copyOf(domain.statuses()));
        assertTrue(domain.accepts("C"));
        assertFalse(domain.accepts("X"));
        assertFalse(domain.accepts(null));
    }

    @Test
    void putTaskToQueue__should_ignore_task_of_other_status() {
        ConcurrencyDomain domain = new ConcurrencyDomain("D", 2, Map.of("A", Duration.ZERO), Map.of(), 10);

        domain.putTaskToQueue(task("X", 0));
        domain.putTaskToQueue(task(null, 0));

        assertNull(domain.pullNextReadyTask(T0.plusSeconds(100)));
    }

    @Test
    void pullNextReadyTask__should_take_ready_task_created_first() {
        ConcurrencyDomain domain = new ConcurrencyDomain("D", 2,
                Map.of("A", Duration.ZERO, "B", Duration.ofSeconds(30)), Map.of(), 10);
        Task asap = task("A", 20);
        Task delayed = task("B", 10); // ready at T0 + 40s
        domain.putTaskToQueue(asap);
        domain.putTaskToQueue(delayed);

        assertSame(delayed, domain.pullNextReadyTask(T0.plusSeconds(40)));
        assertSame(asap, domain.pullNextReadyTask(T0.plusSeconds(40)));
        assertNull(domain.pullNextReadyTask(T0.plusSeconds(40)));
    }

    @Test
    void pullNextReadyTask__should_skip_task_that_is_not_ready() {
        ConcurrencyDomain domain = new ConcurrencyDomain("D", 2,
                Map.of("A", Duration.ZERO, "B", Duration.ofSeconds(30)), Map.of(), 10);
        Task asap = task("A", 20);
        domain.putTaskToQueue(asap);
        domain.putTaskToQueue(task("B", 10));

        assertSame(asap, domain.pullNextReadyTask(T0.plusSeconds(39)));
        assertNull(domain.pullNextReadyTask(T0.plusSeconds(39)));
    }

    @Test
    void pullNextReadyTask__should_take_task_of_first_queue_on_tie() {
        ConcurrencyDomain domain = new ConcurrencyDomain("D", 2,
                Map.of("A", Duration.ZERO, "B", Duration.ofSeconds(1)), Map.of(), 10);
        Task asap = task("A", 10);
        Task delayed = task("B", 10);
        delayed.setId(asap.getId());
        domain.putTaskToQueue(delayed);
        domain.putTaskToQueue(asap);

        assertSame(asap, domain.pullNextReadyTask(T0.plusSeconds(100)));
    }

    @Test
    void slots__should_count_tasks_in_progress() {
        ConcurrencyDomain domain = new ConcurrencyDomain("D", 2, Map.of("A", Duration.ZERO), Map.of(), 10);
        assertTrue(domain.hasFreeSlot());
        assertEquals(2, domain.freeSlots());

        domain.taskStarted();
        domain.taskStarted();
        assertFalse(domain.hasFreeSlot());
        assertEquals(0, domain.freeSlots());
        assertEquals(2, domain.inProgressCount());

        domain.taskFinished();
        assertTrue(domain.hasFreeSlot());
        assertEquals(1, domain.freeSlots());
    }

    private static void assertAccepts(SubQueue queue, Task... tasks) {
        for (Task task : tasks) {
            assertTrue(queue.accepts(task), () -> "queue does not accept " + task.getStatus());
        }
    }

    private Task task(String status, int createdSec) {
        Task task = new Task();
        task.setId(nextTaskId++);
        task.setStatus(status);
        task.setCreated(T0.plusSeconds(createdSec));
        task.setModified(T0.plusSeconds(createdSec));
        return task;
    }

    private static Task failing(Task task) {
        task.setFailingSince(task.getModified());
        return task;
    }
}
