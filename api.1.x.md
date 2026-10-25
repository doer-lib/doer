# Doer API 1.x

All public types are in the package `com.doer`.

## Annotations

### Doer methods

| Annotation | Target | Members | Description |
|---|---|---|---|
| `@AcceptStatus` | method | `value`, `delay` | Marks a Doer method that runs for tasks with the specified status value. A method can accept multiple statuses by using multiple annotations, but each status can be accepted by only one method. If delay is specified, Doer waits for the specified duration before running the method ("30s", "5 min", "2h", "1 day"). |
| `@AcceptStatuses` | method | `value` | Container of repeated `@AcceptStatus`. Write `@AcceptStatus` several times instead. |
| `@RetryPolicy` | method | `interval`, `duration`, `fallbackStatus` | Defines how a Doer method is retried after throwing an exception. `interval` is the delay between attempts. `duration` specifies how long to keep retrying; if omitted, retries continue forever. `fallbackStatus` is the status assigned to the task when `duration` has elapsed, or `null` if not specified. `fallbackStatus` requires `duration`. |

A doer method takes a `Task` and any task data it needs as parameters, and sets the next status with `task.setStatus(...)`. A task with the `null` status is finished.

```java
@AcceptStatus(GOODS_RESERVED)
@RetryPolicy(interval = "5s", duration = "10 min", fallbackStatus = PAYMENT_FAILED)
public void payOrder(Task task, Order order) {
    // ...
    task.setStatus(ORDER_PAID);
}
```

### Task data and failures

| Annotation | Method signature | Description |
|---|---|---|
| `@TaskDataLoader` | `T load(TaskSnapshot task)` | Loads task data of type `T`. One loader per type. Every doer method parameter other than `Task` needs a loader of its type. Called within the transaction that marks the task as in progress. |
| `@TaskDataSaver` | `void save(TaskSnapshot task, T data)` | Saves task data of type `T` after the Doer method succeeds. Called within the transaction that writes the new status. A saver is optional for read-only data that the method uses but does not modify; consider using a Java record for such data. |
| `@ExceptionDescriber` | `void describe(TaskSnapshot task, E exception, JsonObjectBuilder json)` | Called to add details of an exception of type `E` (or a subclass) to the provided `JsonObjectBuilder`. Doer saves the resulting JSON to the `task_logs.extra_json` column. Doer invokes all matching describers for the exception, its cause, and its suppressed exceptions. |

### Concurrency

| Annotation | Target | Members | Description |
|---|---|---|---|
| `@ConcurrencyLimit` | class, method | `value` (int, ≥ 1) | Sets the maximum number of tasks that all Doer methods in a concurrency group can run in parallel on a single node. The default is `2`. Can be applied to a class to limit its group or to a Doer method to set a limit for its exclusive group. |
| `@ConcurrencyGroup` | class, method | `value` (Class) | Joins a class or a doer method to the concurrency group of another class. By default, the doer methods of a class run in the group of that class. |

```java
@ConcurrencyLimit(5)
public class Warehouse { }

@ConcurrencyGroup(Warehouse.class)
public class Shipping {
    @AcceptStatus(ORDER_PAID)
    public void shipOrder(Task task, Order order) { ... }   // group of Warehouse, limit 5
}
```

## Types

| Type | Kind | Description |
|---|---|---|
| `DoerService` | abstract class | Entry point injected and used by the application. Its implementation is generated at compile time based on annotations in the application code. |
| `Task` | class | Represents a task row with columns `id`, `created`, `modified`, `status`, `in_progress`, `failing_since`, and `version`. Application code advances task execution by changing `status`. Doer uses `in_progress` and `version` to coordinate execution across nodes without conflicts, and `modified` and `failing_since` to manage delayed execution, retries, and `fallbackStatus`. |
| `TaskSnapshot` | record | A standalone, immutable snapshot of a task, passed to loaders, savers, and describers. |
| `TaskUpdater` | functional interface | Defines task processing logic in `facilitateCoordinatedUpdate`. |
| `TaskAndDataUpdater<T>` | functional interface | Defines task and data processing logic in `facilitateCoordinatedUpdate`. |

### `DoerService`

| Method | Description |
|---|---|
| `start()` / `stop()` | Starts and stops processing tasks. A stopped service can be started again. |
| `insert(Task)` | Inserts a new task. |
| `loadTask(long)` / `loadTasks(Collection<Long>)` | Reads tasks. |
| `loadTasksForUpdate(Collection<Long>)` | Reads and locks task rows in the caller's transaction. |
| `updateAndBumpVersion(Task)` | Writes a task if its `version` has not changed in the database; returns `false` otherwise. |
| `facilitateCoordinatedUpdate(taskId, waitDuration, allowHijacking, TaskUpdater)` | Updates a task in coordination with the scheduler: waits until the task is not in progress (or hijacks it when `allowHijacking` is true), then runs the updater in one transaction with the task row locked. |
| `facilitateCoordinatedUpdate(taskId, waitDuration, allowHijacking, dataType, TaskAndDataUpdater<T>)` | The same, and also loads the data of `dataType` before the update and saves it after, in the same transaction. |
| `setQueueReloadInterval(Duration)` | How often the queues are reloaded from the database (default 10 min). |
| `setMinQueueSize(int)` / `setMaxTotalQueueSize(int)` | Limits of the in-memory queues (defaults 10 and 10000). |
| `setDataSource(DataSource)` / `setExecutor(Executor)` | Replaces the injected data source or executor programmatically. |

## Exceptions

All are unchecked and have `getTaskId()`.

| Exception | Thrown when |
|---|---|
| `TaskNotFoundException` | There is no task with this id. |
| `TaskInProgressException` | The task is still in progress after `waitDuration` of `facilitateCoordinatedUpdate`, and hijacking is not allowed. |
| `TaskVersionConflictException` | A task update finds a different version in the database, for example when a doer method attempts to update a task that has been hijacked. |

## Failures and retries

An uncaught exception in a doer method, loader, or saver is recorded in `task_logs`. The task retains its status and gets a `failingSince` timestamp. The method is retried according to `@RetryPolicy` until an attempt succeeds.

| Annotation | Retries | After the retries |
|---|---|---|
| `@RetryPolicy(interval = "5s", duration = "10 min", fallbackStatus = PAYMENT_FAILED)` | every 5 s for 10 min | the task gets `PAYMENT_FAILED` |
| `@RetryPolicy(interval = "5s", duration = "10 min")` | every 5 s for 10 min | the task gets `null` (finished) |
| `@RetryPolicy(interval = "5m")` | every 5 min, forever | — |
| no `@RetryPolicy` | every 5 min for 1 day | the task gets `null` (finished) |

### Methods without `@RetryPolicy`

A doer method without `@RetryPolicy` is retried every 5 minutes for 1 day. After that the task gets the `null` status and is no longer processed. The status change is written to `task_logs`.

In 0.x, methods without an `@OnException` annotation were retried every 5 minutes indefinitely. In 1.x, `@RetryPolicy` replaces `@OnException`. To keep the same behavior, annotate the method with `@RetryPolicy(interval = "5m")`.