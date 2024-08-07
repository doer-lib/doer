package com.doer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

class ConcurrencyDomainImpl implements ConcurrencyDomain {
    private static final Duration DEFAULT_RETRY_DELAY = Duration.ofMinutes(5);
    private static Comparator<Task> asapTaskOrder = Comparator.comparing(Task::getCreated)
            .thenComparing(Task::getId);
    private static Comparator<Task> delayedTaskOrder = Comparator.comparing(Task::getModified)
            .thenComparing(Task::getCreated)
            .thenComparing(Task::getId);

    String domainName;
    int numberOfTasksToRunSimultaneously;
    List<String> statuses;
    Map<String, Duration> delays;
    Map<String, Duration> retryDelays;

    int numberOfTasksInProgress;
    List<SubQueue> queues;

    void initSubQueues(int minQueueSize) {
        TreeSet<Duration> delays = new TreeSet<>();
        TreeSet<Duration> retryDelays = new TreeSet<>();
        for (String status : statuses) {
            delays.add(getDelay(status));
            retryDelays.add(getRetryDelay(status));
        }
        queues = new ArrayList<>();
        for (Duration delay : delays) {
            SubQueue queue = new SubQueue();
            queue.failingTaskQueue = false;
            if (delay.isZero()) {
                queue.buffer = new TreeSet<>(asapTaskOrder);
            } else {
                queue.buffer = new TreeSet<>(delayedTaskOrder);
            }
            queue.delay = delay;
            queue.queueSize = minQueueSize;
            queue.hasMoreInDb = false;
            queues.add(queue);
        }
        for (Duration retryDelay : retryDelays) {
            SubQueue queue = new SubQueue();
            queue.failingTaskQueue = true;
            queue.buffer = new TreeSet<>(delayedTaskOrder);
            queue.delay = retryDelay;
            queue.queueSize = minQueueSize;
            queue.hasMoreInDb = false;
            queues.add(queue);
        }
    }

    static class SubQueue {
        boolean failingTaskQueue;
        Duration delay;
        TreeSet<Task> buffer;
        int queueSize;
        boolean hasMoreInDb;
    }

    @Override
    public String getName() {
        return domainName;
    }

    @Override
    public int getValue() {
        return numberOfTasksToRunSimultaneously;
    }

    @Override
    public List<String> getStatuses() {
        return statuses;
    }

    @Override
    public Duration getDelay(String status) {
        return delays.getOrDefault(status, Duration.ZERO);
    }

    @Override
    public Duration getRetryDelay(String status) {
        return retryDelays.getOrDefault(status, DEFAULT_RETRY_DELAY);
    }

    void putTaskToQueue(Task task) {
        String status = task.getStatus();
        if (statuses.contains(status)) {
            boolean isFailing = (task.getFailingSince() != null);
            Duration delay = isFailing ? getRetryDelay(status) : getDelay(status);
            for (SubQueue queue : queues) {
                if (queue.failingTaskQueue == isFailing && queue.delay.equals(delay)) {
                    if (!queue.hasMoreInDb || isTaskGoesBeforeEndOfBuffer(queue.buffer, task)) {
                        queue.buffer.add(task);
                        if (queue.buffer.size() > queue.queueSize) {
                            queue.buffer.pollLast();
                            queue.hasMoreInDb = true;
                        }
                    } else {
                        queue.hasMoreInDb = true;
                    }
                    return;
                }
            }
        }
    }

    boolean isTaskGoesBeforeEndOfBuffer(TreeSet<Task> buffer, Task task) {
        return buffer.size() > 0 && buffer.comparator().compare(buffer.last(), task) > 0;
    }

    Task pullNextReadyTask(Instant now) {
        TreeSet<Task> candidates = new TreeSet<>(asapTaskOrder);
        for (SubQueue queue : queues) {
            if (!queue.buffer.isEmpty()) {
                Task task = queue.buffer.first();
                Instant readyTime = getTaskReadyTime(task);
                if (readyTime.compareTo(now) <= 0) {
                    candidates.add(task);
                }
            }
        }
        if (!candidates.isEmpty()) {
            for (SubQueue queue : queues) {
                if (!queue.buffer.isEmpty()) {
                    if (queue.buffer.first() == candidates.first()) {
                        return queue.buffer.pollFirst();
                    }
                }
            }
        }
        return null;
    }

    boolean hasDrainedQueue() {
        for (SubQueue queue : queues) {
            if (queue.hasMoreInDb && queue.buffer.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    Instant getTaskReadyTime(Task task) {
        if (task.getFailingSince() != null) {
            Duration retryDelay = getRetryDelay(task.getStatus());
            return task.getModified()
                    .plus(retryDelay);
        } else {
            Duration delay = getDelay(task.getStatus());
            if (Duration.ZERO.equals(delay)) {
                return Instant.MIN;
            } else {
                return task.getModified()
                        .plus(delay);
            }
        }
    }

    void dropTask(long taskId) {
        for (SubQueue queue : queues) {
            queue.buffer.removeIf(task -> task.getId() != null && task.getId() == taskId);
        }
    }
}
