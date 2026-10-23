package com.doer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Tasks of one select in SelectTasks.sql, kept in memory: the head of that queue in the database, in its order. A
 * sub-queue holds either not failing tasks with statuses of one delay (asap when the delay is zero) or failing tasks
 * with statuses of one retry delay.
 * <p>
 * Not thread-safe: DoerService uses it only while holding its monitor.
 */
final class SubQueue {
    static final Comparator<Task> ASAP_ORDER = Comparator.comparing(Task::getCreated)
            .thenComparing(Task::getId);
    static final Comparator<Task> DELAYED_ORDER = Comparator.comparing(Task::getModified)
            .thenComparing(ASAP_ORDER);

    private final boolean failing;
    private final Duration delay;
    private final Set<String> statuses;
    private final TreeSet<Task> buffer;
    /** Maximum number of tasks in the buffer, and the LIMIT of the next load from the database. */
    private int capacity;
    /** The database has tasks of this sub-queue that are not in the buffer. */
    private boolean hasMoreInDb;

    SubQueue(boolean failing, Duration delay, Collection<String> statuses, int capacity) {
        this.failing = failing;
        this.delay = delay;
        // Not Set.copyOf: its contains(null) throws, and a finished task has the null status
        this.statuses = Collections.unmodifiableSet(new HashSet<>(statuses));
        this.buffer = new TreeSet<>(isAsap() ? ASAP_ORDER : DELAYED_ORDER);
        this.capacity = capacity;
    }

    /** Not failing tasks without a delay, ordered by created; the other sub-queues are ordered by modified. */
    boolean isAsap() {
        return !failing && delay.isZero();
    }

    boolean isFailing() {
        return failing;
    }

    Set<String> statuses() {
        return statuses;
    }

    boolean accepts(Task task) {
        return (task.getFailingSince() != null) == failing && statuses.contains(task.getStatus());
    }

    /**
     * Adds the task unless the database has tasks before it that are not loaded yet: then the task is left in the
     * database for the next load. When the buffer overflows, its last task is dropped.
     */
    void offer(Task task) {
        if (hasMoreInDb && !goesBeforeLast(task)) {
            return;
        }
        buffer.add(task);
        if (buffer.size() > capacity) {
            buffer.pollLast();
            hasMoreInDb = true;
        }
    }

    private boolean goesBeforeLast(Task task) {
        return !buffer.isEmpty() && buffer.comparator().compare(buffer.last(), task) > 0;
    }

    /** First task in the queue order, or null when the buffer is empty. */
    Task peek() {
        return buffer.isEmpty() ? null : buffer.first();
    }

    Task poll() {
        return buffer.pollFirst();
    }

    void remove(long taskId) {
        buffer.removeIf(task -> task.getId() != null && task.getId() == taskId);
    }

    /** When the task of this sub-queue can run: {@link Instant#MIN} for an asap task. */
    Instant readyTime(Task task) {
        return isAsap() ? Instant.MIN : task.getModified().plus(delay);
    }

    /** Ready time of the first task, or null when the buffer is empty. */
    Instant headReadyTime() {
        return buffer.isEmpty() ? null : readyTime(buffer.first());
    }

    boolean isHeadReady(Instant now) {
        Instant readyTime = headReadyTime();
        return readyTime != null && readyTime.compareTo(now) <= 0;
    }

    /** All tasks of the sub-queue are taken from the buffer, and the database has more. */
    boolean isDrained() {
        return hasMoreInDb && buffer.isEmpty();
    }

    /**
     * LIMIT for the next load. With {@code shrink}, a sub-queue that is used less than half is halved, but not below
     * {@code minSize}.
     */
    int nextLimit(boolean shrink, int minSize) {
        if (shrink && capacity > minSize && buffer.size() > capacity / 2) {
            return Math.max(capacity / 2, minSize);
        }
        return capacity;
    }

    /**
     * Replaces the buffer with the tasks loaded from the database. {@code loaded} rows of this sub-queue were
     * selected with {@code limit}: when they are less than half of it, the capacity is halved, but not below
     * {@code minSize}.
     */
    void replace(List<Task> tasks, int limit, int loaded, int minSize) {
        buffer.clear();
        buffer.addAll(tasks);
        capacity = (loaded < limit / 2 ? Math.max(limit / 2, minSize) : limit);
        hasMoreInDb = (limit <= loaded);
    }

    SortedSet<Task> tasks() {
        return Collections.unmodifiableSortedSet(buffer);
    }

    int size() {
        return buffer.size();
    }

    int capacity() {
        return capacity;
    }
}
