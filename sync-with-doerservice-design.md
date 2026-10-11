# Synchronizing client code with DoerService

Status: implemented

Client code that changes a task (REST endpoint, webhook, admin tool) must not collide with the scheduler running the same task. Doer does the waiting, locking, hijacking, logging and scheduler notification. The client decides:
- how long to wait;
- whether to hijack;
- what to do with the locked task.

## API

```java
// DoerService; the generated service overrides both with @Transactional(NEVER)
public Task facilitateCoordinatedUpdate(long taskId, Duration waitDuration, boolean allowHijacking,
        TaskUpdater updater) throws Exception;

public <T> Task facilitateCoordinatedUpdate(long taskId, Duration waitDuration, boolean allowHijacking,
        Class<T> dataType, TaskAndDataUpdater<T> updater) throws Exception;

@FunctionalInterface
public interface TaskUpdater { void update(Task task) throws Exception; }

@FunctionalInterface
public interface TaskAndDataUpdater<T> { void update(Task task, T data) throws Exception; }
```

The `TaskUpdater` overload delegates to the other one with `dataType = null` (no loader, no saver).

- **Call it outside a transaction.** Inside one, it fails with `TransactionalException`.
- **`updater`** runs in Doer's transaction with the task row locked, and may change the task only with `task.setStatus(...)`. It sees the task exactly as it is in the database: a hijacked task still has `isInProgress() == true`. It may throw checked exceptions.
- **Returns** the task as committed.
- **Throws:**
  - `TaskNotFoundException` if there is no task with this id;
  - `TaskInProgressException` if the task is still in progress after `waitDuration` and `allowHijacking` is `false`.

  Both are `RuntimeException`s and carry `getTaskId()`.

### Overload with a loaded parameter

When `dataType` is not null, the second argument of the updater is loaded with the `@TaskDataLoader` for `dataType` before the updater, and saved with the `@TaskDataSaver` for `dataType` after the task row is written, the same ones that doer methods use. `DoerService` declares two abstract steps, and the generated service implements them with one branch per type, calling the specific loader and saver directly:

```java
// generated _GeneratedDoerService
@Override
protected Object _load(Task task, Class<?> type) throws Exception {
    if (Car.class.equals(type)) {
        return doerResource.loadCar(task);               // @TaskDataLoader
    }
    throw new IllegalArgumentException("No @TaskDataLoader for " + type.getName());
}

@Override
protected void _save(Task task, Class<?> type, Object data) throws Exception {
    if (Car.class.equals(type)) {
        doerResource.storeCar(task, (Car) data);         // @TaskDataSaver; no branch → nothing to do
        return;
    }
}
```

Loader and saver are separate steps so that the task row is written between them, as in doer methods.

- **`dataType` is required**, because the generic type of a `TaskAndDataUpdater` is erased at runtime. It must be a plain class: a generic type such as `List<Car>` cannot be passed as a `Class`, so loaders for generic types are skipped here.
- **A missing `@TaskDataLoader`** is detected only at runtime (`IllegalArgumentException`). For doer methods it is a compile error.
- **Why the dispatch on `type` stays in generated code:**
  - typed overloads per loader (`Class<Order>`, `Class<Car>`) all erase to the same signature;
  - methods with a separate name per type would exist only on the generated class, while clients inject `DoerService`.

## Usage

```java
public void cancelOrder(UUID orderId) throws Exception {        // no @Transactional
    Order order = orderRepository.findById(orderId);
    doerService.facilitateCoordinatedUpdate(order.getTaskId(), Duration.ofSeconds(10), false,
            this::cancelLockedOrder);
}

private void cancelLockedOrder(Task task) {
    orderRepository.markCancelledByTaskId(task.getId());   // joins Doer's transaction
    task.setStatus(ORDER_CANCELLED);
}
```

With a loaded parameter:

```java
doerService.facilitateCoordinatedUpdate(taskId, Duration.ofSeconds(10), false, Order.class,
        (task, order) -> {
            order.setCancelled(true);              // saved by the @TaskDataSaver for Order
            task.setStatus(ORDER_CANCELLED);
        });
```

## Algorithm

```
facilitateCoordinatedUpdate:
    until waitDuration is gone:                            // each attempt: new transaction
        task = attemptCoordinatedUpdate(allowInProgressRowLock = false)
        if task != null: go to NOTIFY
        sleep (50 ms, doubling up to 1 s)                  // no connection held
    task = attemptCoordinatedUpdate(allowInProgressRowLock = true)                 // last attempt
    NOTIFY: triggerTaskReloadFromDb(taskId), return task   // after the commit

attemptCoordinatedUpdate (one transaction):
    1. SELECT * FROM tasks WHERE id = ? AND NOT in_progress FOR UPDATE   (not found → return null)
       or, on the last attempt,
    2. SELECT * FROM tasks WHERE id = ? FOR UPDATE                       (not found → TaskNotFoundException)
    3. if task.in_progress:
           if !allowHijacking → TaskInProgressException
           INSERT INTO task_logs: status → status, 'TaskHijacked', extra_json {"inProgressSince": ...}
    4. data = loader(task)                       // only if dataType != null
    5. updater.update(task, data)                // task as in the database, in_progress unchanged
    6. UPDATE tasks ... in_progress = FALSE, failing_since = NULL, version + 1
    7. saver(task, data)                         // only if dataType != null and a saver is declared
       INSERT INTO task_logs: old → new status
    8. commit
```

A task that does not exist is reported only by the last attempt, so with a positive `waitDuration` the `TaskNotFoundException` comes after `waitDuration`.

**Names in `task_logs`.** `class_name` / `method_name` are those of the method that called `facilitateCoordinatedUpdate`. The public method finds them once with `StackWalker`: the first frame that is neither a `facilitateCoordinatedUpdate` frame nor a generated/framework frame (class or method name with `$`, class name ending with `_Subclass` / `_ClientProxy` / `_Bean`, or class in a container, interceptor or reflection package such as `io.quarkus.`, `org.jboss.`, `java.lang.reflect.`) and passes them to `attemptCoordinatedUpdate` as Strings. In the usage example these are `OrderService` / `cancelOrder`.

## Rules

- **Waiting never blocks the running doer method.** In PostgreSQL, `NOT in_progress FOR UPDATE` skips an in-progress row instead of locking it, so the running doer method can still finish.
- **One transaction for everything.** If the loader, updater or saver throws, everything rolls back: the hijack log, changes to the loaded data, the save and the log rows. The exception then reaches the client. The generated `runInTransaction` uses `rollbackOn = Exception.class`, so checked exceptions from loaders and savers also roll back.
- **The hijacked doer method loses.** Its final update fails the version check (`TaskVersionConflictException`), so its status change and savers are rolled back. Its side effects outside the database are not. `processTask` then calls `triggerTaskReloadFromDb`, so the task is re-queued under its new status even when it was hijacked on the same node.
- **The scheduler is notified before the call returns.** `triggerTaskReloadFromDb` runs after the commit, outside any transaction.

## Removed

`runWithTask`, `CodeZero`, `CodeOne`, `_callLoader` and `_callUnLoader`. This API replaces them.
