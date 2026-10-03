# Synchronizing client code with DoerService

Status: implemented

Client code that changes a task (REST endpoint, webhook, admin tool) must not collide with the scheduler running the same task. Doer does the waiting, locking, hijacking, logging and scheduler notification. The client decides:
- how long to wait;
- whether to hijack;
- what to do with the locked task.

## API

```java
// DoerService; the generated service overrides both with @Transactional(NEVER)
public Task facilitateCoordinatedTaskUpdate(long taskId, Duration waitTimeout, boolean allowHijack,
        Consumer<Task> updater) throws Exception;

public <T> Task facilitateCoordinatedTaskUpdate(long taskId, Duration waitTimeout, boolean allowHijack,
        Class<T> type, BiConsumer<Task, T> updater) throws Exception;
```

- **Call it outside a transaction.** Inside one, it fails with `TransactionalException`.
- **`updater`** runs in Doer's transaction with the task row locked, and may change the task only with `task.setStatus(...)`. `Consumer` and `BiConsumer` cannot throw checked exceptions, so the updater wraps them in a `RuntimeException`.
- **Returns** the task as committed.
- **Throws:**
  - `TaskNotFoundException` if there is no task with this id;
  - `TaskInProgressException` if the task is still in progress after `waitTimeout` and `allowHijack` is `false`.

  Both are `RuntimeException`s and carry `getTaskId()`.

### Overload with a loaded parameter

The `BiConsumer` overload loads the second argument with the `@DoerLoader` for `type`, and saves it afterwards with the `@DoerUnloader` for `type`, the same ones that doer methods use. `DoerService` declares an abstract step, and the generated service implements it with one branch per loader type, calling the specific loader and unloader directly:

```java
// generated _GeneratedDoerService
@Override
protected <T> void _updateWithLoaded(Task task, Class<T> type, BiConsumer<Task, T> updater) throws Exception {
    if (Car.class.equals(type)) {
        Car data = doerResource.loadCar(task);           // @DoerLoader
        updater.accept(task, type.cast(data));
        doerResource.storeCar(task, data);               // @DoerUnloader, only if declared
        return;
    }
    throw new IllegalArgumentException("No @DoerLoader for " + type.getName());
}
```

- **`type` is required**, because the generic type of a `BiConsumer` is erased at runtime. It must be a plain class: a generic type such as `List<Car>` cannot be passed as a `Class`, so loaders for generic types are skipped here.
- **A missing `@DoerLoader`** is detected only at runtime (`IllegalArgumentException`). For doer methods it is a compile error.
- **Why the dispatch on `type` stays in generated code:**
  - typed overloads per loader (`Class<Order>`, `Class<Car>`) all erase to the same signature;
  - methods with a separate name per type would exist only on the generated class, while clients inject `DoerService`.

## Usage

```java
public void cancelOrder(UUID orderId) throws Exception {        // no @Transactional
    Order order = orderRepository.findById(orderId);
    doerService.facilitateCoordinatedTaskUpdate(order.getTaskId(), Duration.ofSeconds(10), false,
            this::cancelLockedOrder);
}

private void cancelLockedOrder(Task task) {
    orderRepository.markCancelledByTaskId(task.getId());   // joins Doer's transaction
    task.setStatus(ORDER_CANCELLED);
}
```

With a loaded parameter:

```java
doerService.facilitateCoordinatedTaskUpdate(taskId, Duration.ofSeconds(10), false, Order.class,
        (task, order) -> {
            order.setCancelled(true);              // saved by the @DoerUnloader for Order
            task.setStatus(ORDER_CANCELLED);
        });
```

## Algorithm

```
until waitTimeout:                                       // each attempt: new transaction
    tx: task = SELECT * FROM tasks WHERE id = ? AND NOT in_progress FOR UPDATE
        if found: update(task), commit, go to NOTIFY
        else: end tx
    no task with this id → TaskNotFoundException
    sleep (50 ms, doubling up to 1 s)                    // no connection held while sleeping

tx: task = SELECT * FROM tasks WHERE id = ? FOR UPDATE   // final attempt, locks even if in_progress
    if task.in_progress:
        if !allowHijack → TaskInProgressException
        UPDATE in_progress = FALSE
        task_logs row: status → status, exception_type 'TaskHijacked', extra_json {"inProgressSince": ...}
    update(task), commit

NOTIFY: triggerTaskReloadFromDb(taskId), return task
```

`update(task)`:
1. Call `updater.accept(task)`. The overload instead calls `_updateWithLoaded(task, type, updater)`: loader, updater, unloader.
2. Save the task with `in_progress = false` and `failing_since = null`.
3. Write a `task_logs` row (old → new status).

**Names in `task_logs`.** `class_name` / `method_name` are those of the method that called `facilitateCoordinatedTaskUpdate`, found with `StackWalker`. Doer takes the frame after the outermost `facilitateCoordinatedTaskUpdate` frame, so CDI proxies and interceptors are skipped. In the usage example these are `OrderService` / `cancelOrder`.

## Rules

- **Waiting never blocks the running doer method.** In PostgreSQL, `NOT in_progress FOR UPDATE` skips an in-progress row instead of locking it, so the running doer method can still finish.
- **One transaction for everything.** If the loader, updater or unloader throws, everything rolls back: the hijack, changes to the loaded data, the save and the log rows. The exception then reaches the client. The generated `runInTransaction` uses `rollbackOn = Exception.class`, so checked exceptions from loaders and unloaders also roll back.
- **The hijacked doer method loses.** Its final update fails the version check, so its status change and unloaders are rolled back. Its side effects outside the database are not. `processTask` then calls `triggerTaskReloadFromDb`, so the task is re-queued under its new status even when it was hijacked on the same node.
- **The scheduler is notified before the call returns.** `triggerTaskReloadFromDb` runs after the commit, outside any transaction.

## Removed

`runWithTask`, `CodeZero`, `CodeOne`, `_callLoader` and `_callUnLoader`. This API replaces them.
