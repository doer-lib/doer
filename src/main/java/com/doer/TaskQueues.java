package com.doer;

import static com.doer.DoerService.LOG;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

/**
 * Tasks of all concurrency domains in the memory of this node: queued in the sub-queues of the domains, or in
 * progress.
 * <p>
 * Not thread-safe: DoerService uses it only while holding its monitor.
 */
final class TaskQueues {

    private final List<ConcurrencyDomain> domains = new ArrayList<>();
    /** Sub-queues of all domains, in the order of the LIMIT parameters of SelectTasks.sql. */
    private final List<SubQueue> queues = new ArrayList<>();
    private final Map<Long, Task> inProgress = new HashMap<>();
    /** Tasks in progress here whose reload was requested meanwhile: they are reloaded when they finish. */
    private final Set<Long> reloadWhenFinished = new HashSet<>();
    private Set<String> activeStatuses;

    ConcurrencyDomain addDomain(String name, int limit, Map<String, Duration> delays,
            Map<String, Duration> retryDelays, int minQueueSize) {
        ConcurrencyDomain domain = new ConcurrencyDomain(name, limit, delays, retryDelays, minQueueSize);
        domains.add(domain);
        queues.addAll(domain.queues());
        activeStatuses = null;
        return domain;
    }

    List<ConcurrencyDomain> domains() {
        return Collections.unmodifiableList(domains);
    }

    /** Sub-queues of all domains, in the order of the LIMIT parameters of SelectTasks.sql. */
    List<SubQueue> queues() {
        return Collections.unmodifiableList(queues);
    }

    /** Domain that processes the status, or null for a status no domain accepts (a final status). */
    ConcurrencyDomain domainOf(String status) {
        for (ConcurrencyDomain domain : domains) {
            if (domain.accepts(status)) {
                return domain;
            }
        }
        return null;
    }

    /** Statuses of all domains. */
    Set<String> activeStatuses() {
        if (activeStatuses == null) {
            HashSet<String> statuses = new HashSet<>();
            for (ConcurrencyDomain domain : domains) {
                statuses.addAll(domain.statuses());
            }
            activeStatuses = Collections.unmodifiableSet(statuses);
        }
        return activeStatuses;
    }

    /**
     * Takes the next ready task of the domain and marks it in progress. Returns null when the domain has no free slot
     * or no ready task.
     */
    Task startNext(ConcurrencyDomain domain, Instant now) {
        if (!domain.hasFreeSlot()) {
            return null;
        }
        Task task = domain.pullNextReadyTask(now);
        if (task == null) {
            return null;
        }
        inProgress.put(task.getId(), task);
        domain.taskStarted();
        return task;
    }

    /**
     * Marks the task started by {@link #startNext} not in progress. Returns true when its reload was requested while
     * it was in progress (see {@link #reloadWhenFinished}).
     */
    boolean finish(Task task, ConcurrencyDomain domain) {
        domain.taskFinished();
        inProgress.remove(task.getId());
        return reloadWhenFinished.remove(task.getId());
    }

    /**
     * Asks to reload the task in progress here when it finishes: this node was asked to reload it, and could not do
     * it now. When the task was changed meanwhile, its run ends with a version conflict, and without the reload this
     * node would drop it.
     */
    void reloadWhenFinished(long taskId) {
        reloadWhenFinished.add(taskId);
    }

    /** Puts the task to the queue of its status. Returns the domain of the status, or null for a final status. */
    ConcurrencyDomain requeue(Task task) {
        ConcurrencyDomain domain = domainOf(task.getStatus());
        if (domain != null) {
            domain.putTaskToQueue(task);
        }
        return domain;
    }

    /**
     * Replaces the queued copy of the task with the one loaded from the database. Returns the domain of its status,
     * or null for a final status.
     */
    ConcurrencyDomain replaceTask(Task fromDb) {
        for (ConcurrencyDomain domain : domains) {
            domain.dropTask(fromDb.getId());
        }
        return requeue(fromDb);
    }

    boolean isInProgress(long taskId) {
        return inProgress.containsKey(taskId);
    }

    /**
     * LIMIT of each sub-queue for the next load. When a sub-queue is drained, or 80% of {@code queueReloadInterval}
     * has passed since the last load ({@code lastQueueSelectTime}), the sub-queues used less than half are halved, but
     * not below {@code minQueueSize}. When a sub-queue is drained before that, the first drained one is doubled, unless
     * all sub-queues would get more than {@code maxTotalQueueSize}.
     */
    List<Integer> nextLimits(Instant now, Instant lastQueueSelectTime, Duration queueReloadInterval, int minQueueSize,
            int maxTotalQueueSize) {
        int drainedIndex = -1;
        for (int i = 0; i < queues.size(); i++) {
            if (queues.get(i).isDrained()) {
                drainedIndex = i;
                break;
            }
        }
        boolean hasDrainedQueue = drainedIndex >= 0;
        Duration queueUsageTime = (lastQueueSelectTime == null ? Duration.ZERO
                : Duration.between(lastQueueSelectTime, now));
        boolean queueIsUsed80PercentOfReloadTime = (queueUsageTime
                .compareTo(queueReloadInterval.dividedBy(10).multipliedBy(8)) >= 0);

        List<Integer> limits = new ArrayList<>();
        int totalQueuesSize = 0;
        for (SubQueue queue : queues) {
            int limit = queue.nextLimit(queueIsUsed80PercentOfReloadTime || hasDrainedQueue, minQueueSize);
            limits.add(limit);
            totalQueuesSize += limit;
        }
        if (!queueIsUsed80PercentOfReloadTime && hasDrainedQueue) {
            int drainedQueueSize = queues.get(drainedIndex).capacity();
            if (totalQueuesSize + drainedQueueSize <= maxTotalQueueSize) {
                limits.set(drainedIndex, drainedQueueSize * 2);
            }
        }
        return limits;
    }

    /**
     * Replaces the buffers of the sub-queues with the rows loaded with {@code limits}: the rows of each sub-queue
     * (selected with its LIMIT) and all in_progress rows.
     * <p>
     * A row is not queued when this node has a newer version of the task in a queue (the task changed after the
     * select), when the task is in progress on this node (it is reloaded when it finishes), or when it is in progress
     * in the database (on another node, or it has finished since the select). The rows of the first and the last case
     * are returned: they are to be checked again later.
     */
    List<Task> replaceFromDb(List<Integer> limits, List<Task> rows, int minQueueSize) {
        List<List<Task>> rowsByQueue = rowsByQueue(rows);
        if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "Queues loaded by sub-queue: inProgress="
                    + rows.stream().filter(Task::isInProgress).count() + "\n"
                    + formatLoad(limits, rowsByQueue.stream().map(List::size).toList()));
        }

        List<Task> tasksWithUncertainState = new ArrayList<>();
        Set<Task> notQueued = Collections.newSetFromMap(new IdentityHashMap<>());
        Map<Long, Task> queuedTasks = queuedTasks();
        for (Task row : rows) {
            Task queued = queuedTasks.get(row.getId());
            if (queued != null && queued.getVersion() > row.getVersion()) {
                LOG.log(Level.FINER, "Loaded task not queued, a newer version is queued: taskId=" + row.getId());
                tasksWithUncertainState.add(row);
                notQueued.add(row);
            } else if (inProgress.containsKey(row.getId())) {
                LOG.log(Level.FINER, "Loaded task not queued, it runs here: taskId=" + row.getId());
                notQueued.add(row);
                reloadWhenFinished(row.getId());
            } else if (row.isInProgress()) {
                LOG.log(Level.FINER, "Loaded task not queued, it is in progress in the database: taskId="
                        + row.getId());
                tasksWithUncertainState.add(row);
                notQueued.add(row);
            }
        }

        for (int i = 0; i < queues.size(); i++) {
            List<Task> loaded = rowsByQueue.get(i);
            List<Task> buffer = new ArrayList<>();
            for (Task row : loaded) {
                if (!notQueued.contains(row)) {
                    buffer.add(row);
                }
            }
            queues.get(i).replace(buffer, limits.get(i), loaded.size(), minQueueSize);
        }
        return tasksWithUncertainState;
    }

    /**
     * Rows selected by the select of each sub-queue, in their original order; a row goes to the first sub-queue that
     * accepts it. The in_progress rows are not among them: they come from the last select, without a LIMIT.
     */
    private List<List<Task>> rowsByQueue(List<Task> rows) {
        List<List<Task>> result = new ArrayList<>();
        for (int i = 0; i < queues.size(); i++) {
            result.add(new ArrayList<>());
        }
        for (Task row : rows) {
            if (row.isInProgress()) {
                continue;
            }
            for (int i = 0; i < queues.size(); i++) {
                if (queues.get(i).accepts(row)) {
                    result.get(i).add(row);
                    break;
                }
            }
        }
        return result;
    }

    private Map<Long, Task> queuedTasks() {
        Map<Long, Task> result = new HashMap<>();
        for (SubQueue queue : queues) {
            for (Task task : queue.tasks()) {
                result.put(task.getId(), task);
            }
        }
        return result;
    }

    /**
     * Earliest ready time of the first tasks of the sub-queues of the domains with a free slot: {@link Instant#MIN}
     * when such a domain has an asap task queued, null when their sub-queues are empty.
     */
    Instant earliestReadyTimeOfFreeDomains() {
        Instant earliest = null;
        for (ConcurrencyDomain domain : domains) {
            if (!domain.hasFreeSlot()) {
                continue;
            }
            for (SubQueue queue : domain.queues()) {
                Instant readyTime = queue.headReadyTime();
                if (readyTime != null && (earliest == null || readyTime.isBefore(earliest))) {
                    earliest = readyTime;
                }
            }
        }
        return earliest;
    }

    /** "limit/queued" of each sub-queue, for the log. */
    /** Two lines, the LIMIT and the number of loaded rows of each sub-queue, one under the other. */
    static String formatLoad(List<Integer> limits, List<Integer> loaded) {
        StringBuilder limitLine = new StringBuilder("  limit: ");
        StringBuilder loadedLine = new StringBuilder("  loaded:");
        for (int i = 0; i < limits.size(); i++) {
            String limit = String.valueOf(limits.get(i));
            String count = String.valueOf(loaded.get(i));
            int width = Math.max(limit.length(), count.length()) + 2;
            limitLine.append(" ".repeat(width - limit.length())).append(limit);
            loadedLine.append(" ".repeat(width - count.length())).append(count);
        }
        return limitLine + "\n" + loadedLine;
    }
}
