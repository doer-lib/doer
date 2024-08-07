package com.doer;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.doer.ConcurrencyDomainImpl.SubQueue;

public abstract class DoerService {
    private static final long MAX_MONITOR_TIMEOUT_MS = 60 * 1000;
    Logger logger = Logger.getLogger(DoerService.class.getName());
    protected DoerService self;
    protected volatile boolean isRunning;
    protected volatile boolean taskLoadRequired;
    protected volatile boolean loadingInProgress;
    protected Executor executor;

    protected AtomicInteger dbTimeDiffMs = new AtomicInteger(0);
    protected ConcurrentLinkedQueue<Task> inProgressTasks = new ConcurrentLinkedQueue<>();
    protected ConcurrentLinkedQueue<Task> reCheckTasks = new ConcurrentLinkedQueue<>();
    protected Instant lastReCheck;

    private ConcurrentLinkedQueue<ConcurrencyDomainImpl> domains = new ConcurrentLinkedQueue<>();
    private Set<String> statusesCache;

    Duration queueReloadInterval = Duration.ofMinutes(10);
    Instant lastSelectTime;
    int maxAllQueuesSize = 10000;
    int minSingleQueueSize = 10;
    boolean monitorEnabled;
    Thread monitorThread;
    Duration stalledTaskCheckInterval = Duration.ofMinutes(30);
    Duration stalledTaskTimeout = Duration.ofHours(2);
    Instant lastStalledTaskChecked = Instant.now();

    public void start(boolean enableMonitor) {
        synchronized (this) {
            isRunning = true;
            monitorEnabled = enableMonitor;
            triggerQueuesReloadFromDb();
            if (!enableMonitor) {
                this.notify();
            }
        }
    }

    public void stop() {
        synchronized (this) {
            isRunning = false;
            this.notify();
        }
    }

    public boolean isMonitorEnabld() {
        synchronized (this) {
            return monitorEnabled;
        }
    }

    public void setMonitorEnabled(boolean value) {
        synchronized (this) {
            monitorEnabled = value;
            executor.execute(this::monitorIdleState);
        }
    }

    private void monitorIdleState() {
        synchronized (this) {
            if (!monitorEnabled) {
                return;
            }
            if (loadingInProgress) {
                return;
            }
            if (!inProgressTasks.isEmpty()) {
                return;
            }
            if (monitorThread != null) {
                return;
            }
            monitorThread = Thread.currentThread();
            String threadName = Thread.currentThread().getName();
            Thread.currentThread().setName("doer-idle-monitor");
            try {
                long timeout = getNextCheckTime();
                long waitTime = timeout > 0 ? timeout : 1000;
                logInfo("Next check after " + waitTime);
                this.wait(waitTime);
                executor.execute(self::onCheckTime);
            } catch (InterruptedException e) {
                logWarning("Monitoring thread interrupted", e);
            } finally {
                Thread.currentThread().setName(threadName);
                monitorThread = null;
            }
        }
    }

    private long getNextCheckTime() {
        TreeSet<Instant> plannedTimes = new TreeSet<>();
        synchronized (this) {
            Instant now = getDbNow();
            if (lastSelectTime == null) {
                return 0;
            }
            Instant nextDbLoadTime = lastSelectTime.plus(queueReloadInterval);
            if (nextDbLoadTime.isBefore(now)) {
                return 0;
            }
            plannedTimes.add(nextDbLoadTime);
            for (ConcurrencyDomainImpl domain : domains) {
                for (SubQueue queue : domain.queues) {
                    if (!queue.buffer.isEmpty()) {
                        Instant plannedTime = domain.getTaskReadyTime(queue.buffer.first());
                        if (plannedTime.equals(Instant.MIN)) {
                            return 0;
                        }
                        plannedTimes.add(plannedTime);
                    }
                }
            }
            if (stalledTaskCheckInterval != null) {
                if (lastStalledTaskChecked == null) {
                    return 0;
                }
                plannedTimes.add(lastStalledTaskChecked.plus(stalledTaskCheckInterval));
            }
            if (plannedTimes.isEmpty()) {
                return MAX_MONITOR_TIMEOUT_MS;
            }
            long ms = Duration.between(now, plannedTimes.first()).toMillis();
            return Math.min(MAX_MONITOR_TIMEOUT_MS, Math.max(0, ms));
        }
    }

    public void onCheckTime() {
        synchronized (this) {
            Instant now = getDbNow();
            if (stalledTaskCheckInterval != null && lastStalledTaskChecked != null &&
                    lastStalledTaskChecked.plus(stalledTaskCheckInterval).isBefore(now)) {
                triggerStalledTaskReset();
            } else if (lastSelectTime == null || lastSelectTime.plus(queueReloadInterval).isBefore(now)) {
                triggerQueuesReloadFromDb();
            }
            checkTasksBecomeReady();
        }
    }

    public void setMaxAllQueuesSize(int value) {
        maxAllQueuesSize = value;
    }

    public int getMaxAllQueuesSize() {
        return maxAllQueuesSize;
    }

    public void setMinSingleQueueSize(int value) {
        minSingleQueueSize = value;
    }

    public int getMinSingleQueueSize() {
        return minSingleQueueSize;
    }

    public void setQueueReloadInterval(Duration value) {
        queueReloadInterval = value;
    }

    public Duration getQueueReloadInterval() {
        return queueReloadInterval;
    }

    protected void setupConcurrencyDomain(String name, int concurrency, Map<String, Duration> delays, Map<String, Duration> retryDelays) {
        HashSet<String> keys = new HashSet<>(delays.keySet());
        keys.addAll(retryDelays.keySet());
        List<String> statuses = new CopyOnWriteArrayList<>(keys);
        Collections.sort(statuses);

        ConcurrencyDomainImpl domain = new ConcurrencyDomainImpl();
        domain.domainName = name;
        domain.numberOfTasksToRunSimultaneously = concurrency;
        domain.statuses = statuses;
        domain.delays = new HashMap<>(delays);
        domain.retryDelays = new HashMap<>(retryDelays);
        domain.numberOfTasksInProgress = 0;
        domain.initSubQueues(minSingleQueueSize);
        domains.add(domain);
        synchronized(this) {
            this.statusesCache = null;
        }
    }

    public void triggerQueuesReloadFromDb() {
        synchronized (this) {
            if (!isRunning) {
                return;
            }
            if (taskLoadRequired) {
                return;
            }
            taskLoadRequired = true;
            if (!loadingInProgress) {
                loadingInProgress = true;
                executor.execute(self::reloadTasksFromDb);
            }
        }
    }

    public void triggerTaskReloadFromDb(long taskId) {
        synchronized (this) {
            if (!isRunning) {
                return;
            }
            if (taskLoadRequired) {
                return;
            }
            executor.execute(() -> reloadQueuedTask(taskId));
        }
    }

    private void reloadQueuedTask(long taskId) {
        try {
            Task task = self.loadTask(taskId);
            if (task != null && !task.isInProgress()) {
                synchronized (this) {
                    for (Task t : inProgressTasks) {
                        if (taskId == t.getId()) {
                            return;
                        }
                    }
                    for (ConcurrencyDomainImpl domain : domains) {
                        domain.dropTask(taskId);
                    }
                    for (ConcurrencyDomainImpl domain : domains) {
                        if (domain.getStatuses().contains(task.getStatus())) {
                            domain.putTaskToQueue(task);
                            if (domain.numberOfTasksInProgress < domain.getValue()) {
                                executor.execute(() -> processNextTask(domain));
                            }
                            return;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            triggerQueuesReloadFromDb();
            logWarning("Failed to load single task. Queue reloade initiated.", e);
        }
    }

    private void reCheckTask() {
        if (reCheckTasks.isEmpty()) {
            return;
        }
        synchronized (this) {
            if (!isRunning) {
                return;
            }
            if (lastReCheck != null) {
                Duration breakBetweenChecks = Duration.ofSeconds(2);
                Instant earliestNextCheckTime = lastReCheck.plus(breakBetweenChecks);
                if (Instant.now().isBefore(earliestNextCheckTime)) {
                    return;
                }
            }
            lastReCheck = Instant.now();
        }
        try {
            Iterator<Task> iterator = reCheckTasks.iterator();
            while (iterator.hasNext()) {
                Task task = iterator.next();
                Task dbTask = self.loadTask(task.getId());
                if (dbTask == null) {
                    iterator.remove();
                } else if (dbTask.getVersion() > task.getVersion()) {
                    iterator.remove();
                    self.triggerTaskReloadFromDb(task.getId());
                } else if (getDbNow().isAfter(dbTask.getModified().plusSeconds(10))) {
                    iterator.remove();
                }
            }
        } catch (Exception e) {
            logWarning("Failed to load task for re-check", e);
        }
    }

    public void checkTasksBecomeReady() {
        reCheckTask();
        synchronized (this) {
            if (!isRunning) {
                return;
            }
            Iterator<ConcurrencyDomainImpl> iterator = domains.iterator();
            while (iterator.hasNext()) {
                ConcurrencyDomainImpl domain = iterator.next();
                if (domain.numberOfTasksInProgress < domain.getValue()) {
                    executor.execute(() -> processNextTask(domain));
                }
            }
        }
    }

    public void reloadTasksFromDb() {
        List<Integer> limits = new ArrayList<>();
        synchronized (this) {
            if (!isRunning) {
                return;
            }
            taskLoadRequired = false;
            loadingInProgress = true;
            limits = calculateLimits();
            lastReCheck = null;
            reCheckTasks.clear();
            lastSelectTime = getDbNow();
        }
        try {
            List<Task> tasksFromDb = self.loadTasksFromDatabase(limits);
            String infoMessage;
            synchronized (this) {
                if (isRunning) {
                    updateQueues(limits, tasksFromDb);
                    infoMessage = "Queues Loaded (limit/loaded): "
                            + createLimitVolumeMessage(limits, getQueueVolumes());
                } else {
                    infoMessage = "Queues Loaded but skipped. Because DoerService was stopped";
                }
            }
            logInfo(infoMessage);
        } catch (Exception e) {
            logWarning("Failed to load tasks from db. Limits: " + limits, e);
        } finally {
            synchronized (this) {
                if (taskLoadRequired) {
                    executor.execute(self::reloadTasksFromDb);
                } else {
                    loadingInProgress = false;
                    executor.execute(self::checkTasksBecomeReady);
                }
            }
        }
    }

    public Logger getLogger() {
        return logger;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public void logWarning(String message, Exception e) {
        if (logger != null) {
            logger.log(Level.WARNING, message, e);
        }
    }

    public void logInfo(String message) {
        if (logger != null) {
            logger.log(Level.INFO, message);
        }
    }

    private List<Integer> calculateLimits() {
        List<SubQueue> queues = new ArrayList<>();
        for (ConcurrencyDomainImpl domain : domains) {
            for (SubQueue queue : domain.queues) {
                queues.add(queue);
            }
        }

        boolean hasDrainedQueue = false;
        int drainedQueueSize = 0;
        int drainedQueueSizeIndex = 0;
        for (int i = 0; i < queues.size(); i++) {
            SubQueue queue = queues.get(i);
            if (queue.buffer.isEmpty() && queue.hasMoreInDb) {
                hasDrainedQueue = true;
                drainedQueueSize = queue.queueSize;
                drainedQueueSizeIndex = i;
                break;
            }
        }

        List<Integer> sizes = new ArrayList<>();
        int totalQueuesSize = 0;
        Duration queueUsageTime = (lastSelectTime == null ? Duration.ZERO : Duration.between(lastSelectTime, getDbNow()));
        boolean queueIsUsed80PercentOfReloadTime = (queueUsageTime
                .compareTo(queueReloadInterval.dividedBy(10).multipliedBy(8)) >= 0);
        for (int i = 0; i < queues.size(); i++) {
            SubQueue queue = queues.get(i);
            int size;
            if ((queueIsUsed80PercentOfReloadTime || hasDrainedQueue) && queue.queueSize > minSingleQueueSize
                    && queue.buffer.size() > queue.queueSize / 2) {
                size = Math.max(queue.queueSize / 2, minSingleQueueSize);
            } else {
                size = queue.queueSize;
            }
            sizes.add(size);
            totalQueuesSize += size;
        }
        if (!queueIsUsed80PercentOfReloadTime) {
            if (hasDrainedQueue && totalQueuesSize + drainedQueueSize <= maxAllQueuesSize) {
                sizes.set(drainedQueueSizeIndex, drainedQueueSize * 2);
            }
        }
        return sizes;
    }

    private void updateQueues(List<Integer> limits, List<Task> tasks) {
        // We select tasks from db limited by each subquery, and additionally all
        // in_progress.
        // Using loaded snapshot we calculate how much we actually loaded for every
        // subquery - to know that DB has more rows for that subquery.
        // Since we read DB, till now DoerService may have updated some task, and now it
        // has newer version of Task - we put that task in re-chek list.
        // When we read DB we may see in_progress task that is not in our memory any
        // more (other node processed it?)
        LinkedList<Integer> limitsCopy = new LinkedList<>(limits);
        LinkedList<Integer> loadedCounts = calculateActualLoadedCounts(tasks);
        logInfo("Load tasks.size: " + tasks.size());
        logInfo("Loaded in_progress: " + tasks.stream().filter(Task::isInProgress).count());
        logInfo("Limits: " + limitsCopy);
        logInfo("Loads: " + loadedCounts);
        HashSet<Long> inProgressIds = new HashSet<>();
        for (Task task : inProgressTasks) {
            inProgressIds.add(task.getId());
        }
        LinkedList<Task> copy = new LinkedList<>();
        HashMap<Long, Task> inMemoryTasks = getInMemoryTasks();
        for (Task task : tasks) {
            Task memo = inMemoryTasks.get(task.getId());
            if (memo != null && memo.getVersion() > task.getVersion()) {
                logInfo("Newer task " + task.getId() + " " + task.getModified());
                reCheckTasks.add(task);
            } else if (inProgressIds.contains(task.getId())) {
                // We should not put task to the list, if it is in_progress
                logInfo("Skipped In progress " + task.getId() + " " + task.getModified());
            } else if (task.isInProgress()) {
                // If task in db was in progress, and we don't have updated copy in memory, we
                // need to check it a bit later.
                logInfo("Skipped DB In progress " + task.getId() + " " + task.getModified());
                reCheckTasks.add(task);
            } else {
                copy.add(task);
            }
        }
        for (ConcurrencyDomainImpl domain : domains) {
            HashMap<Duration, HashSet<String>> delayedStatuses = new HashMap<>();
            HashMap<Duration, HashSet<String>> retryStatuses = new HashMap<>();
            for (String status : domain.getStatuses()) {
                delayedStatuses
                        .computeIfAbsent(domain.getDelay(status), key -> new HashSet<>())
                        .add(status);
                retryStatuses
                        .computeIfAbsent(domain.getRetryDelay(status), key -> new HashSet<>())
                        .add(status);
            }
            for (SubQueue queue : domain.queues) {
                List<Task> newBuffer = new ArrayList<>();
                if (!queue.failingTaskQueue) {
                    HashSet<String> statuses = delayedStatuses.get(queue.delay);
                    Iterator<Task> iterator = copy.iterator();
                    while (iterator.hasNext()) {
                        Task task = iterator.next();
                        if (task.getFailingSince() == null && statuses.contains(task.getStatus())) {
                            iterator.remove();
                            newBuffer.add(task);
                        }
                    }
                } else {
                    HashSet<String> statuses = retryStatuses.get(queue.delay);
                    Iterator<Task> iterator = copy.iterator();
                    while (iterator.hasNext()) {
                        Task task = iterator.next();
                        if (task.getFailingSince() != null && statuses.contains(task.getStatus())) {
                            iterator.remove();
                            newBuffer.add(task);
                        }
                    }
                }
                queue.buffer.clear();
                queue.buffer.addAll(newBuffer);
                int limit = limitsCopy.pollFirst();
                int loaded = loadedCounts.pollFirst();
                queue.queueSize = (loaded < limit / 2 ? Math.max(limit / 2, minSingleQueueSize) : limit);
                queue.hasMoreInDb = (limit <= loaded);
            }
        }
        logInfo("Tasks to reload later: " + reCheckTasks.stream().map(Task::getId).collect(Collectors.toList()));
    }

    private LinkedList<Integer> calculateActualLoadedCounts(List<Task> tasks) {
        LinkedList<Integer> counts = new LinkedList<>();
        LinkedList<Task> copy = new LinkedList<>(tasks);
        for (ConcurrencyDomainImpl domain : domains) {
            HashMap<Duration, HashSet<String>> delayedStatuses = new HashMap<>();
            HashMap<Duration, HashSet<String>> retryStatuses = new HashMap<>();
            for (String status : domain.getStatuses()) {
                delayedStatuses
                        .computeIfAbsent(domain.getDelay(status), key -> new HashSet<>())
                        .add(status);
                retryStatuses
                        .computeIfAbsent(domain.getRetryDelay(status), key -> new HashSet<>())
                        .add(status);
            }
            for (SubQueue queue : domain.queues) {
                int loadedTasks = 0;
                if (!queue.failingTaskQueue) {
                    HashSet<String> statuses = delayedStatuses.get(queue.delay);
                    Iterator<Task> iterator = copy.iterator();
                    while (iterator.hasNext()) {
                        Task task = iterator.next();
                        if (task.getFailingSince() == null && statuses.contains(task.getStatus())) {
                            iterator.remove();
                            loadedTasks++;
                        }
                    }
                } else {
                    HashSet<String> statuses = retryStatuses.get(queue.delay);
                    Iterator<Task> iterator = copy.iterator();
                    while (iterator.hasNext()) {
                        Task task = iterator.next();
                        if (task.getFailingSince() != null && statuses.contains(task.getStatus())) {
                            iterator.remove();
                            loadedTasks++;
                        }
                    }
                }
                counts.add(loadedTasks);
            }
        }
        return counts;
    }

    private HashMap<Long, Task> getInMemoryTasks() {
        HashMap<Long, Task> inMemoryTasks = new HashMap<>();
        for (ConcurrencyDomainImpl domain : domains) {
            for (SubQueue queue : domain.queues) {
                Iterator<Task> iterator = queue.buffer.iterator();
                while (iterator.hasNext()) {
                    Task task = iterator.next();
                    inMemoryTasks.put(task.getId(), task);
                }
            }
        }
        return inMemoryTasks;
    }

    private List<Integer> getQueueVolumes() {
        List<Integer> result = new ArrayList<>();
        for (ConcurrencyDomainImpl domain : domains) {
            for (SubQueue queue : domain.queues) {
                result.add(queue.buffer.size());
            }
        }
        return result;
    }

    private String createLimitVolumeMessage(List<Integer> limits, List<Integer> volumes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < limits.size(); i++) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(limits.get(i))
                    .append("/")
                    .append(volumes.get(i));
        }
        String ss = sb.toString();
        return ss;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    public Instant getDbNow() {
        Instant i = Instant.now()
                .plusMillis(dbTimeDiffMs.get());
        return i;
    }

    public long generateId() throws SQLException {
        String sql = "SELECT nextval('id_generator'::regclass) AS v";
        try (Connection con = getConnection();
                PreparedStatement pst = con.prepareStatement(sql);
                ResultSet rs = pst.executeQuery()) {
            rs.next();
            return rs.getLong("v");
        }
    }

    public void insert(Task task) throws SQLException {
        String sql = "INSERT INTO tasks (created, modified, status, in_progress, failing_since, version) VALUES (now(), now(), ?, ?, ?, ?) RETURNING *";
        try (Connection con = getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setString(1, task.getStatus());
            pst.setBoolean(2, task.isInProgress());
            pst.setObject(3, toOffsetDateTime(task.getFailingSince()));
            pst.setInt(4, 0);
            try (ResultSet rs = pst.executeQuery()) {
                if (rs.next()) {
                    task.assignFieldsFrom(readTask(rs));
                } else {
                    throw new IllegalStateException("Unexpected result after SQL INSERT command");
                }
            }
        }
    }

    private OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant == null ? null : OffsetDateTime.ofInstant(instant, ZoneId.systemDefault());
    }

    private Instant toInstant(OffsetDateTime odt) {
        return odt == null ? null : odt.toInstant();
    }

    public Task loadTask(long id) throws SQLException {
        String sql = "SELECT * FROM tasks WHERE id = ?";
        try (Connection con = getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setLong(1, id);
            try (ResultSet rs = pst.executeQuery()) {
                if (rs.next()) {
                    return readTask(rs);
                } else {
                    return null;
                }
            }
        }
    }

    private Task readTask(ResultSet rs) throws SQLException {
        Task task = new Task();
        task.setId(rs.getLong("id"));
        task.setCreated(toInstant(rs.getObject("created", OffsetDateTime.class)));
        task.setModified(toInstant(rs.getObject("modified", OffsetDateTime.class)));
        task.setStatus(rs.getString("status"));
        task.setInProgress(rs.getBoolean("in_progress"));
        task.setFailingSince(toInstant(rs.getObject("failing_since", OffsetDateTime.class)));
        task.setVersion(rs.getInt("version"));
        return task;
    }

    public boolean updateAndBumpVersion(Task task) throws SQLException {
        int newVersion = task.getVersion() + 1;
        String sql = "UPDATE tasks SET in_progress = ?, status = ?, modified = now(), failing_since = ?, version = ? WHERE id = ? AND version = ? RETURNING *";
        try (Connection con = getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setBoolean(1, task.isInProgress());
            pst.setString(2, task.getStatus());
            pst.setObject(3, toOffsetDateTime(task.getFailingSince()));
            pst.setInt(4, newVersion);
            pst.setLong(5, task.getId());
            pst.setInt(6, task.getVersion());
            try (ResultSet rs = pst.executeQuery()) {
                if (rs.next()) {
                    Task updated = readTask(rs);
                    task.assignFieldsFrom(updated);
                    return true;
                } else {
                    return false;
                }
            }
        }
    }

    public abstract void runTask(Task task) throws Exception;

    public abstract void runInTransaction(Callable<Object> code) throws Exception;

    public abstract Connection getConnection() throws SQLException;

    public List<Task> loadTasksFromDatabase(List<Integer> limits) throws SQLException, IOException {
        List<Task> tasks = new ArrayList<>();
        String sql;
        try (InputStream is = getClass().getResourceAsStream("/com/doer/generated/SelectTasks.sql");
             Scanner scanner = new Scanner(is, "UTF-8")) {
            sql = scanner.useDelimiter("\\A").next();
        }
        try (Connection con = getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            for (int i = 0; i < limits.size(); i++) {
                pst.setInt(i + 1, limits.get(i));
            }
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    Task task = readTask(rs);
                    tasks.add(task);
                }
            }
        }
        return tasks;
    }

    public void callDoerMethod(Task task, Callable<Object> loader, Callable<Object> caller, Callable<Object> unloader,
            String className, String methodName, Duration errorTimeout, String onErrorStatus) throws Exception {
        String initialStatus = task.getStatus();
        long t0 = System.currentTimeMillis();
        self.runInTransaction(() -> {
            task.setInProgress(true);
            if (!updateAndBumpVersion(task)) {
                throw new OptimisticLockException("Task update failed. Optimistic lock exception.");
            }
            loader.call();
            return null;
        });
        Exception exception;
        try {
            caller.call();
            exception = null;
        } catch (Exception e) {
            exception = e;
            logWarning("Doer method error", e);
        }
        if (exception == null) {
            try {
                self.runInTransaction(() -> {
                    task.setFailingSince(null);
                    task.setInProgress(false);
                    if (!updateAndBumpVersion(task)) {
                        throw new OptimisticLockException("Task update failed. Optimistic lock exception.");
                    }
                    unloader.call();
                    int t = (int) (System.currentTimeMillis() - t0);
                    writeTaskLog(task.getId(), initialStatus, task.getStatus(), className, methodName, null, null, t);
                    return null;
                });
                return;
            } catch (OptimisticLockException e) {
                throw e;
            } catch (Exception e) {
                exception = e;
                logWarning("Unloader error", e);
            }
        }
        Exception finalException = exception;
        self.runInTransaction(() -> {
            if (task.getFailingSince() == null) {
                task.setFailingSince(getDbNow());
                task.setStatus(initialStatus);
            } else if (errorTimeout != null && task.getFailingSince().plus(errorTimeout).isBefore(getDbNow())) {
                task.setFailingSince(null);
                task.setStatus(onErrorStatus);
            } else {
                task.setStatus(initialStatus);
            }
            task.setInProgress(false);
            if (!updateAndBumpVersion(task)) {
                throw new OptimisticLockException("Task update failed. Optimistic lock exception.");
            }
            String exceptionType = finalException.getClass().getName();
            String extraJson = createExtraJson(task, finalException);
            int t = (int) (System.currentTimeMillis() - t0);
            writeTaskLog(task.getId(), initialStatus, task.getStatus(), className, methodName, exceptionType, extraJson,
                    t);
            return null;
        });
    }

    public void runWithTask(long taskId, String className, String methodName, CodeZero code) throws Exception {
        Task task = self.loadTask(taskId);
        if (task == null) {
            throw new IllegalStateException("Task not found in database.");
        }
        if (task.isInProgress()) {
            throw new OptimisticLockException("Task is in progress");
        }
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        callDoerMethod(task, () -> null, () -> {
            try {
                code.call(task);
            } catch (Exception e) {
                exceptionRef.set(e);
                throw e;
            } catch (Throwable e) {
                // OutOfMemory and other errors
                exceptionRef.set(new ExecutionException(e));
                throw e;
            }
            return null;
        }, () -> null, className, methodName, null, null);
        if (exceptionRef.get() != null) {
            throw exceptionRef.get();
        }
    }

    public <T> void runWithTask(long taskId, String className, String methodName, Class<T> klazz, CodeOne<T> code) throws Exception {
        Task task = self.loadTask(taskId);
        if (task == null) {
            throw new IllegalStateException("Task not found in database.");
        }
        if (task.isInProgress()) {
            throw new OptimisticLockException("Task is in progress");
        }
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        Object[] args = new Object[2];
        callDoerMethod(task, () -> args[1] = _callLoader(klazz, task), () -> {
            try {
                code.call(task, (T) args[1]);
            } catch (Exception e) {
                exceptionRef.set(e);
                throw e;
            } catch (Throwable e) {
                // OutOfMemory and other errors
                exceptionRef.set(new ExecutionException(e));
                throw e;
            }
            return null;
        }, () -> {
            _callUnLoader(klazz, task, args[1]);
            return null;
        }, className, methodName, null, null);
        if (exceptionRef.get() != null) {
            throw exceptionRef.get();
        }
    }

    protected abstract Object _callLoader(Class<?> klazz, Task task) throws Exception;

    protected abstract void _callUnLoader(Class<?> klazz, Task task, Object data) throws Exception;

    public void triggerStalledTaskReset() {
        executor.execute(() -> {
            try {
                self.resetStalledInProgressTasks(stalledTaskTimeout);
            } catch (SQLException e) {
                logWarning("Error resetting stalled tasks", e);
            }
        });
    }

    public int resetStalledInProgressTasks(Duration timeout) throws SQLException {
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("Stalled tasks timeout should be a positive duration");
        }
        synchronized (this) {
            lastStalledTaskChecked = getDbNow();
        }
        try (Connection con = getConnection()) {
            Instant treshold = getDbNow().minus(timeout);
            String sqlInsertLog = "INSERT INTO task_logs (task_id, initial_status, final_status, class_name, method_name, exception_type) "
                    +
                    "SELECT id, status, status, 'DoerService', 'resetStalledInProgressTasks', 'StalledTaskReset' " +
                    "FROM tasks WHERE in_progress AND modified < ?";
            try (PreparedStatement pst = con.prepareStatement(sqlInsertLog)) {
                pst.setObject(1, toOffsetDateTime(treshold));
                pst.executeUpdate();
            }
            String sqlUpdate = "UPDATE tasks SET in_progress = FALSE, version = version + 1, modified = now() WHERE in_progress AND modified < ?";
            try (PreparedStatement pst = con.prepareStatement(sqlUpdate)) {
                pst.setObject(1, toOffsetDateTime(treshold));
                int updated = pst.executeUpdate();
                logInfo("Reset stalled in_progress tasks. Updated " + updated + " rows");
                return updated;
            }
        }
    }

    public abstract String createExtraJson(Task task, Exception exception);

    public long writeTaskLog(Long taskId, String initialStatus, String finalStatus, String className, String methodName,
            String exceptionType, String extraJson, Integer durationMs) throws SQLException {
        String sql = "INSERT INTO task_logs (task_id, initial_status, final_status, class_name, method_name, duration_ms, exception_type, extra_json) "
                +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?::json) RETURNING id";
        try (Connection con = getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setLong(1, taskId);
            pst.setString(2, initialStatus);
            pst.setString(3, finalStatus);
            pst.setString(4, className);
            pst.setString(5, methodName);
            if (durationMs == null) {
                pst.setNull(6, Types.INTEGER);
            } else {
                pst.setInt(6, durationMs);
            }
            pst.setString(7, exceptionType);
            pst.setString(8, extraJson);
            try (ResultSet rs = pst.executeQuery()) {
                rs.next();
                return rs.getLong("id");
            }
        }
    }

    private ConcurrencyDomainImpl selectConcurrencyDomain(String status) {
        Iterator<ConcurrencyDomainImpl> iterator = domains.iterator();
        while (iterator.hasNext()) {
            ConcurrencyDomainImpl domain = iterator.next();
            if (domain.getStatuses().contains(status)) {
                return domain;
            }
        }
        return null;
    }

    private void processNextTask(ConcurrencyDomainImpl domain) {
        Task task;
        synchronized (this) {
            if (!isRunning) {
                return;
            }
            if (domain.numberOfTasksToRunSimultaneously - domain.numberOfTasksInProgress < 1) {
                executor.execute(this::monitorIdleState);
                return;
            }
            task = domain.pullNextReadyTask(getDbNow());
            if (task == null) {
                executor.execute(this::monitorIdleState);
                return;
            }
            if (domain.hasDrainedQueue()) {
                triggerQueuesReloadFromDb();
            }
            inProgressTasks.add(task);
            domain.numberOfTasksInProgress++;
            this.notify();
        }
        boolean processedSuccessfully = false;
        String threadName = Thread.currentThread().getName();
        Thread.currentThread().setName("doer-task-" + task.getId());
        try {
            runTask(task);
            processedSuccessfully = true;
        } catch (OptimisticLockException e) {
            // Other node is working with this task. So we should ignore it until next time
            // we query db
        } catch (Exception e) {
            logWarning("Failed to run task TaskId: " + task.getId(), e);
        } finally {
            Thread.currentThread().setName(threadName);
            synchronized (this) {
                domain.numberOfTasksInProgress--;
                inProgressTasks.remove(task);
                if (processedSuccessfully) {
                    boolean nextProcessingStarted = false;
                    ConcurrencyDomainImpl newDomain = selectConcurrencyDomain(task.getStatus());
                    if (newDomain != null) {
                        newDomain.putTaskToQueue(task);
                        int nNew = newDomain.getValue() - domain.numberOfTasksInProgress;
                        for (int i = 0; i < nNew; i++) {
                            executor.execute(() -> processNextTask(newDomain));
                            nextProcessingStarted = true;
                        }
                    }
                    if (newDomain != domain) {
                        int nOld = domain.getValue() - domain.numberOfTasksInProgress;
                        for (int i = 0; i < nOld; i++) {
                            executor.execute(() -> processNextTask(domain));
                            nextProcessingStarted = true;
                        }
                    }
                    if (!nextProcessingStarted && inProgressTasks.isEmpty()) {
                        executor.execute(this::monitorIdleState);
                    }
                } else {
                    // When any task failed in concurrencyDomain we don't start all possible
                    // parralel executions, only one
                    executor.execute(() -> processNextTask(domain));
                }
            }
        }
    }

    public Set<String> getActiveStatuses() {
        synchronized (this) {
            if (statusesCache == null) {
                HashSet<String> statuses = new HashSet<>();
                for (ConcurrencyDomain domain : domains) {
                    statuses.addAll(domain.getStatuses());
                }
                statusesCache = Collections.unmodifiableSet(statuses);
            }
            return statusesCache;
        }
    }

    protected String limitTo1024(String s) {
        if (s == null) {
            return null;
        }
        if (s.length() <= 1024) {
            return s;
        }
        return s.substring(0, 1023) + "\u2026";
    }
}
