package com.doer;

import com.doer.ConcurrencyDomainImpl.SubQueue;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Array;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.sql.DataSource;

public abstract class DoerService {
    private static final long MAX_MONITOR_TIMEOUT_MS = 60 * 1000;
    protected static final Logger LOG = Logger.getLogger(DoerService.class.getName());

    private DoerService self;
    private Executor executor;
    private DataSource dataSource;

    private boolean isRunning;
    private boolean taskLoadRequired;
    private boolean loadingInProgress;

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

    public DoerService getSelfReference() {
        return self;
    }

    public void setSelfReference(DoerService self) {
        this.self = self;
    }

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
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

    public void triggerQueuesReloadFromDb() {
        synchronized (this) {
            if (!isRunning || taskLoadRequired) {
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
            if (!isRunning || taskLoadRequired) {
                return;
            }
            executor.execute(() -> reloadQueuedTask(taskId));
        }
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public Instant getDbNow() {
        return Instant.now().plusMillis(dbTimeDiffMs.get());
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

    public Map<Long, Task> loadTasks(Collection<Long> ids) throws SQLException {
        String sql = "SELECT * FROM tasks WHERE id = ANY(?)";
        HashMap<Long, Task> result = new HashMap<>();
        try (Connection con = getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            Array sqlArray = con.createArrayOf("BIGINT", ids.toArray());
            pst.setArray(1, sqlArray);
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    Task task = readTask(rs);
                    result.put(task.getId(), task);
                }
            }
        }
        return result;
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

    private void updateAndBumpVersionOrThrow(Task task) throws SQLException {
        if (!updateAndBumpVersion(task)) {
            throw new TaskVersionConflictException(task.getId());
        }
    }

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

    protected void setupConcurrencyDomain(String name, int limit, Map<String, Duration> delays, Map<String, Duration> retryDelays) {
        HashSet<String> keys = new HashSet<>(delays.keySet());
        keys.addAll(retryDelays.keySet());
        List<String> statuses = new CopyOnWriteArrayList<>(keys);
        Collections.sort(statuses);

        ConcurrencyDomainImpl domain = new ConcurrencyDomainImpl();
        domain.domainName = name;
        domain.numberOfTasksToRunSimultaneously = limit;
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

    private void monitorIdleState() {
        synchronized (this) {
            if (!monitorEnabled || loadingInProgress || !inProgressTasks.isEmpty() || monitorThread != null) {
                return;
            }
            monitorThread = Thread.currentThread();
            String threadName = Thread.currentThread().getName();
            Thread.currentThread().setName("doer-idle-monitor");
            try {
                long timeout = getNextCheckTime();
                long waitTime = timeout > 0 ? timeout : 1000;
                LOG.info("Next check after " + waitTime);
                this.wait(waitTime);
                executor.execute(this::onCheckTime);
            } catch (InterruptedException e) {
                LOG.log(Level.WARNING, "Monitoring thread interrupted", e);
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

    private void onCheckTime() {
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
                            if (domain.numberOfTasksInProgress < domain.getLimit()) {
                                executor.execute(() -> processNextTask(domain));
                            }
                            return;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            triggerQueuesReloadFromDb();
            LOG.log(Level.WARNING, "Failed to load single task. Queue reloade initiated.", e);
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
            LOG.log(Level.WARNING, "Failed to load task for re-check", e);
        }
    }

    // todo consider to make private
    public void checkTasksBecomeReady() {
        reCheckTask();
        synchronized (this) {
            if (!isRunning) {
                return;
            }
            for (ConcurrencyDomainImpl domain : domains) {
                if (domain.numberOfTasksInProgress < domain.getLimit()) {
                    executor.execute(() -> processNextTask(domain));
                }
            }
        }
    }

    // todo consider to make private
    public void reloadTasksFromDb() {
        List<Integer> limits;
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
            LOG.info(infoMessage);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to load tasks from db. Limits: " + limits, e);
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
        LOG.info("Load tasks.size: " + tasks.size());
        LOG.info("Loaded in_progress: " + tasks.stream().filter(Task::isInProgress).count());
        LOG.info("Limits: " + limitsCopy);
        LOG.info("Loads: " + loadedCounts);
        HashSet<Long> inProgressIds = new HashSet<>();
        for (Task task : inProgressTasks) {
            inProgressIds.add(task.getId());
        }
        LinkedList<Task> copy = new LinkedList<>();
        HashMap<Long, Task> inMemoryTasks = getInMemoryTasks();
        for (Task task : tasks) {
            Task memo = inMemoryTasks.get(task.getId());
            if (memo != null && memo.getVersion() > task.getVersion()) {
                LOG.info("Newer task " + task.getId() + " " + task.getModified());
                reCheckTasks.add(task);
            } else if (inProgressIds.contains(task.getId())) {
                // We should not put task to the list, if it is in_progress
                LOG.info("Skipped In progress " + task.getId() + " " + task.getModified());
            } else if (task.isInProgress()) {
                // If task in db was in progress, and we don't have updated copy in memory, we
                // need to check it a bit later.
                LOG.info("Skipped DB In progress " + task.getId() + " " + task.getModified());
                reCheckTasks.add(task);
            } else {
                copy.add(task);
            }
        }
        for (ConcurrencyDomainImpl domain : domains) {
            HashMap<Duration, HashSet<String>> delayedStatuses = groupStatusesByDelay(domain, false);
            HashMap<Duration, HashSet<String>> retryStatuses = groupStatusesByDelay(domain, true);
            for (SubQueue queue : domain.queues) {
                List<Task> newBuffer = takeTasksForQueue(copy, queue, delayedStatuses, retryStatuses);
                queue.buffer.clear();
                queue.buffer.addAll(newBuffer);
                int limit = limitsCopy.pollFirst();
                int loaded = loadedCounts.pollFirst();
                queue.queueSize = (loaded < limit / 2 ? Math.max(limit / 2, minSingleQueueSize) : limit);
                queue.hasMoreInDb = (limit <= loaded);
            }
        }
        LOG.info("Tasks to reload later: " + reCheckTasks.stream().map(Task::getId).collect(Collectors.toList()));
    }

    private LinkedList<Integer> calculateActualLoadedCounts(List<Task> tasks) {
        LinkedList<Integer> counts = new LinkedList<>();
        LinkedList<Task> copy = new LinkedList<>(tasks);
        for (ConcurrencyDomainImpl domain : domains) {
            HashMap<Duration, HashSet<String>> delayedStatuses = groupStatusesByDelay(domain, false);
            HashMap<Duration, HashSet<String>> retryStatuses = groupStatusesByDelay(domain, true);
            for (SubQueue queue : domain.queues) {
                counts.add(takeTasksForQueue(copy, queue, delayedStatuses, retryStatuses).size());
            }
        }
        return counts;
    }

    /** Groups the domain's statuses by their delay, or by their retry delay when {@code retry} is true. */
    private static HashMap<Duration, HashSet<String>> groupStatusesByDelay(ConcurrencyDomainImpl domain,
            boolean retry) {
        HashMap<Duration, HashSet<String>> result = new HashMap<>();
        for (String status : domain.getStatuses()) {
            Duration delay = retry ? domain.getRetryDelay(status) : domain.getDelay(status);
            result.computeIfAbsent(delay, key -> new HashSet<>()).add(status);
        }
        return result;
    }

    /** Removes the tasks that belong to {@code queue} from {@code tasks} and returns them in their original order. */
    private static List<Task> takeTasksForQueue(LinkedList<Task> tasks, SubQueue queue,
            HashMap<Duration, HashSet<String>> delayedStatuses, HashMap<Duration, HashSet<String>> retryStatuses) {
        HashSet<String> statuses;
        if (queue.failingTaskQueue) {
            statuses = retryStatuses.get(queue.delay);
        } else {
            statuses = delayedStatuses.get(queue.delay);
        }
        List<Task> taken = new ArrayList<>();
        Iterator<Task> iterator = tasks.iterator();
        while (iterator.hasNext()) {
            Task task = iterator.next();
            boolean isFailing = task.getFailingSince() != null;
            if (isFailing == queue.failingTaskQueue && statuses.contains(task.getStatus())) {
                iterator.remove();
                taken.add(task);
            }
        }
        return taken;
    }

    private HashMap<Long, Task> getInMemoryTasks() {
        HashMap<Long, Task> inMemoryTasks = new HashMap<>();
        for (ConcurrencyDomainImpl domain : domains) {
            for (SubQueue queue : domain.queues) {
                for (Task task : queue.buffer) {
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
        return sb.toString();
    }

    private OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant == null ? null : OffsetDateTime.ofInstant(instant, ZoneId.systemDefault());
    }

    private Instant toInstant(OffsetDateTime odt) {
        return odt == null ? null : odt.toInstant();
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

    public abstract void runTask(Task task) throws Exception;

    public abstract void runInTransaction(Callable<Object> code) throws Exception;

    public void callDoerMethod(Task task, Callable<Object> loader, Callable<Object> caller, Callable<Object> saver,
            String className, String methodName, Duration errorTimeout, String onErrorStatus) throws Exception {
        String initialStatus = task.getStatus();
        long t0 = System.currentTimeMillis();
        self.runInTransaction(() -> {
            task.setInProgress(true);
            updateAndBumpVersionOrThrow(task);
            loader.call();
            return null;
        });
        Exception exception;
        try {
            caller.call();
            exception = null;
        } catch (Exception e) {
            exception = e;
            LOG.log(Level.WARNING, "Doer method error", e);
        }
        if (exception == null) {
            try {
                self.runInTransaction(() -> {
                    task.setFailingSince(null);
                    task.setInProgress(false);
                    updateAndBumpVersionOrThrow(task);
                    saver.call();
                    int t = (int) (System.currentTimeMillis() - t0);
                    writeTaskLog(task.getId(), initialStatus, task.getStatus(), className, methodName, null, null, t);
                    return null;
                });
                return;
            } catch (TaskVersionConflictException e) {
                throw e;
            } catch (Exception e) {
                exception = e;
                LOG.log(Level.WARNING, "Task data saver error", e);
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
            updateAndBumpVersionOrThrow(task);
            String exceptionType = finalException.getClass().getName();
            String extraJson = createExtraJson(task, finalException);
            int t = (int) (System.currentTimeMillis() - t0);
            writeTaskLog(task.getId(), initialStatus, task.getStatus(), className, methodName, exceptionType, extraJson,
                    t);
            return null;
        });
    }

    /**
     * Updates a task in coordination with the scheduler. Must be called outside a transaction.
     * <p>
     * Waits up to {@code waitDuration} for the task to stop being in progress, polling in short transactions
     * that hold no connection between attempts. The attempt that locks the task continues in the same
     * transaction: calls {@code updater}, saves the task and writes a task_logs row. If the task is still in
     * progress after {@code waitDuration}, it is hijacked when {@code allowHijacking} is true. After the commit
     * the scheduler is notified, and only then the method returns.
     *
     * @param updater runs in Doer's transaction with the task row locked and sees the task as it is in the
     *                database; may change the task only with {@link Task#setStatus(String)}
     * @return the task as committed
     * @throws TaskNotFoundException   no task with this id
     * @throws TaskInProgressException still in progress after waitDuration, and allowHijacking is false
     */
    public Task facilitateCoordinatedUpdate(long taskId, Duration waitDuration, boolean allowHijacking,
            TaskUpdater updater) throws Exception {
        Objects.requireNonNull(updater, "updater");
        return facilitateCoordinatedUpdate(taskId, waitDuration, allowHijacking, null,
                (task, data) -> updater.update(task));
    }

    /**
     * Same as {@link #facilitateCoordinatedUpdate(long, Duration, boolean, TaskUpdater)}, but when
     * {@code dataType} is not null, the second argument of {@code updater} is loaded with the {@link TaskDataLoader}
     * for {@code dataType} before the update, and saved with the {@link TaskDataSaver} for {@code dataType} (if
     * declared) after the task is written, in the same transaction.
     *
     * @throws IllegalArgumentException no {@link TaskDataLoader} for {@code dataType}
     */
    public <T> Task facilitateCoordinatedUpdate(long taskId, Duration waitDuration, boolean allowHijacking,
            Class<T> dataType, TaskAndDataUpdater<T> updater) throws Exception {
        Objects.requireNonNull(updater, "updater");
        CallerInfo caller = StackWalker.getInstance()
                .walk(frames -> frames
                        .map(CallerInfo::fromFrame)
                        .filter(c -> !c.looksLikeProxy())
                        .filter(c -> !"facilitateCoordinatedUpdate".equals(c.methodName()))
                        .findFirst())
                .orElse(new CallerInfo("Unknown", "Unknown", "unknown"));
        long waitNanos = waitDuration == null || waitDuration.isNegative() ? 0 : waitDuration.toNanos();
        long deadline = System.nanoTime() + waitNanos;
        long sleepMs = 50;
        while (true) {
            long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            boolean isLastAttempt = remainingMs <= 0;
            Task task = attemptCoordinatedUpdate(taskId, isLastAttempt, allowHijacking, dataType, updater,
                    caller.className(), caller.methodName());
            if (task != null) {
                triggerTaskReloadFromDb(taskId);
                return task;
            }
            long timeoutMs = Math.max(1, Math.min(sleepMs, remainingMs));
            Thread.sleep(timeoutMs);
            sleepMs = Math.min(sleepMs * 2, 1000);
        }
    }

    /**
     * One transaction: locks the task row, writes the hijack log (if hijacked), loads, calls
     * {@code updater}, writes the task, saves the data and writes the task log.
     *
     * @param allowInProgressRowLock when false, an in-progress row is not locked and null is returned
     * @return the committed task, or null when the task is in progress and allowInProgressRowLock is false
     */
    private <T> Task attemptCoordinatedUpdate(long taskId, boolean allowInProgressRowLock, boolean allowHijacking,
            Class<T> dataType, TaskAndDataUpdater<T> updater, String className, String methodName) throws Exception {
        Task[] result = new Task[1];
        self.runInTransaction(() -> {
            Task task = selectTaskForUpdate(taskId, allowInProgressRowLock);
            if (task == null) {
                if (allowInProgressRowLock) {
                    throw new TaskNotFoundException(taskId);
                }
                return null;
            }
            if (task.isInProgress()) {
                if (!allowHijacking) {
                    throw new TaskInProgressException(taskId);
                }
                Instant inProgressSince = task.getModified();
                String extraJson = inProgressSince == null ? null
                        : "{\"inProgressSince\": \"" + inProgressSince + "\"}";
                writeTaskLog(taskId, task.getStatus(), task.getStatus(), className, methodName, "TaskHijacked",
                        extraJson, null);
            }
            String initialStatus = task.getStatus();
            long t0 = System.currentTimeMillis();
            T data = dataType == null ? null : dataType.cast(_load(task, dataType));
            updater.update(task, data);
            task.setInProgress(false);
            task.setFailingSince(null);
            updateAndBumpVersionOrThrow(task);
            if (dataType != null) {
                _save(task, dataType, data);
            }
            int t = (int) (System.currentTimeMillis() - t0);
            writeTaskLog(taskId, initialStatus, task.getStatus(), className, methodName, null, null, t);
            result[0] = task;
            return null;
        });
        return result[0];
    }

    /**
     * Loads with the {@link TaskDataLoader} for {@code type}. Implemented by the generated service.
     *
     * @throws IllegalArgumentException no {@link TaskDataLoader} for {@code type}
     */
    protected abstract Object _load(Task task, Class<?> type) throws Exception;

    /**
     * Saves {@code data} with the {@link TaskDataSaver} for {@code type}, if declared; otherwise does nothing.
     * Implemented by the generated service.
     */
    protected abstract void _save(Task task, Class<?> type, Object data) throws Exception;

    /**
     * Locks the task row in the current transaction.
     *
     * @param allowInProgress when false, an in-progress row is skipped (not locked) and null is returned
     */
    protected Task selectTaskForUpdate(long taskId, boolean allowInProgress) throws SQLException {
        String sql = allowInProgress
                ? "SELECT * FROM tasks WHERE id = ? FOR UPDATE"
                : "SELECT * FROM tasks WHERE id = ? AND NOT in_progress FOR UPDATE";
        try (Connection con = getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setLong(1, taskId);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() ? readTask(rs) : null;
            }
        }
    }

    public void triggerStalledTaskReset() {
        executor.execute(() -> {
            try {
                self.resetStalledInProgressTasks(stalledTaskTimeout);
            } catch (SQLException e) {
                LOG.log(Level.WARNING, "Error resetting stalled tasks", e);
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
                LOG.info("Reset stalled in_progress tasks. Updated " + updated + " rows");
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
        for (ConcurrencyDomainImpl domain : domains) {
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
        boolean taskChangedConcurrently = false;
        String threadName = Thread.currentThread().getName();
        Thread.currentThread().setName("doer-task-" + task.getId());
        try {
            runTask(task);
            processedSuccessfully = true;
        } catch (TaskVersionConflictException e) {
            // Other node or facilitateCoordinatedUpdate changed this task. Reload it from db
            taskChangedConcurrently = true;
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to run task TaskId: " + task.getId(), e);
        } finally {
            Thread.currentThread().setName(threadName);
            synchronized (this) {
                domain.numberOfTasksInProgress--;
                inProgressTasks.remove(task);
                if (taskChangedConcurrently) {
                    triggerTaskReloadFromDb(task.getId());
                }
                if (processedSuccessfully) {
                    boolean nextProcessingStarted = false;
                    ConcurrencyDomainImpl newDomain = selectConcurrencyDomain(task.getStatus());
                    if (newDomain != null) {
                        newDomain.putTaskToQueue(task);
                        int nNew = newDomain.getLimit() - domain.numberOfTasksInProgress;
                        for (int i = 0; i < nNew; i++) {
                            executor.execute(() -> processNextTask(newDomain));
                            nextProcessingStarted = true;
                        }
                    }
                    if (newDomain != domain) {
                        int nOld = domain.getLimit() - domain.numberOfTasksInProgress;
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

    private record CallerInfo(String fullClassName, String className, String methodName) {
        boolean looksLikeProxy() {
            return methodName.contains("$") ||
                    fullClassName.contains("$") ||
                    fullClassName.endsWith("_Subclass") ||
                    fullClassName.endsWith("_ClientProxy") ||
                    fullClassName.endsWith("_Bean") ||
                    fullClassName.startsWith("io.quarkus.") ||
                    fullClassName.startsWith("io.smallrye.") ||
                    fullClassName.startsWith("org.jboss.") ||
                    fullClassName.startsWith("org.apache.webbeans.") ||
                    fullClassName.startsWith("org.springframework.") ||
                    fullClassName.startsWith("org.glassfish.") ||
                    fullClassName.startsWith("com.sun.ejb.") ||
                    fullClassName.startsWith("jakarta.") ||
                    fullClassName.startsWith("javax.") ||
                    fullClassName.startsWith("java.lang.reflect.") ||
                    fullClassName.startsWith("jdk.internal.") ||
                    fullClassName.startsWith("sun.reflect.") ||
                    fullClassName.startsWith("jdk.proxy");
        }

        static CallerInfo fromFrame(StackWalker.StackFrame frame) {
            String fullClassName = frame.getClassName();
            return new CallerInfo(fullClassName, fullClassName.replaceAll(".*\\.", ""), frame.getMethodName());
        }
    }

}
