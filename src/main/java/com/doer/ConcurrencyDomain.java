package com.doer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * Concurrency group of doer methods: at most {@code limit} of its tasks run at the same time on one node. Keeps the
 * tasks of its statuses in sub-queues, one per select of the domain in SelectTasks.sql.
 * <p>
 * Not thread-safe: DoerService uses it only while holding its monitor.
 */
final class ConcurrencyDomain {
    private static final Duration DEFAULT_RETRY_DELAY = Duration.ofMinutes(5);

    private final String name;
    private final int limit;
    private final SortedSet<String> statuses;
    private final List<SubQueue> queues;
    private int inProgress;

    /**
     * The statuses of the domain are the keys of {@code delays} and {@code retryDelays}. A status without a delay is
     * processed as soon as possible, a status without a retry delay is retried in 5 minutes. The generated service
     * passes the retry delay of every status, so the default is only for services set up by hand (tests).
     * <p>
     * The sub-queues are in the order of the selects of the domain in SelectTasks.sql (see
     * {@code DoerProcessor.generateSelectTaskSql}), which DoerService relies on to match the LIMIT parameters: asap
     * statuses, then delayed statuses by delay, then failing statuses by retry delay, the shortest delay first
     * (checked by {@code SelectTasksOrderITCase}).
     */
    ConcurrencyDomain(String name, int limit, Map<String, Duration> delays, Map<String, Duration> retryDelays,
            int minQueueSize) {
        this.name = name;
        this.limit = limit;
        TreeSet<String> allStatuses = new TreeSet<>(delays.keySet());
        allStatuses.addAll(retryDelays.keySet());
        this.statuses = Collections.unmodifiableSortedSet(allStatuses);
        List<SubQueue> result = new ArrayList<>();
        groupStatuses(status -> delays.getOrDefault(status, Duration.ZERO))
                .forEach((delay, group) -> result.add(new SubQueue(false, delay, group, minQueueSize)));
        groupStatuses(status -> retryDelays.getOrDefault(status, DEFAULT_RETRY_DELAY))
                .forEach((delay, group) -> result.add(new SubQueue(true, delay, group, minQueueSize)));
        this.queues = List.copyOf(result);
    }

    /** Statuses of the domain by their delay, the shortest delay first. */
    private TreeMap<Duration, List<String>> groupStatuses(Function<String, Duration> delayOf) {
        TreeMap<Duration, List<String>> result = new TreeMap<>();
        for (String status : statuses) {
            result.computeIfAbsent(delayOf.apply(status), key -> new ArrayList<>()).add(status);
        }
        return result;
    }

    String name() {
        return name;
    }

    SortedSet<String> statuses() {
        return statuses;
    }

    /** False for the null status: a task with it is finished. */
    boolean accepts(String status) {
        return status != null && statuses.contains(status);
    }

    List<SubQueue> queues() {
        return queues;
    }

    int inProgressCount() {
        return inProgress;
    }

    boolean hasFreeSlot() {
        return inProgress < limit;
    }

    int freeSlots() {
        return limit - inProgress;
    }

    void taskStarted() {
        inProgress++;
    }

    void taskFinished() {
        inProgress--;
    }

    void putTaskToQueue(Task task) {
        for (SubQueue queue : queues) {
            if (queue.accepts(task)) {
                queue.offer(task);
                return;
            }
        }
    }

    /**
     * Takes the ready task that was created first, or returns null when no task is ready. On a tie the task of the
     * first sub-queue is taken.
     */
    Task pullNextReadyTask(Instant now) {
        SubQueue next = null;
        for (SubQueue queue : queues) {
            if (queue.isHeadReady(now)
                    && (next == null || SubQueue.ASAP_ORDER.compare(queue.peek(), next.peek()) < 0)) {
                next = queue;
            }
        }
        return next == null ? null : next.poll();
    }

    boolean hasDrainedQueue() {
        for (SubQueue queue : queues) {
            if (queue.isDrained()) {
                return true;
            }
        }
        return false;
    }

    void dropTask(long taskId) {
        for (SubQueue queue : queues) {
            queue.remove(taskId);
        }
    }
}
