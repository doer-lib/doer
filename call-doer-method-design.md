# Taking control of a task by id

Status: proposal · Applies to: `DoerService`, `DoerProcessor`

## Problem

Client code that has only a task id (REST endpoint, webhook, admin tool) sometimes needs to change the task outside the scheduler. Today the only option is `runWithTask(...)`, which runs the client's code in Doer's own transactions. The client cannot change its business data and the task atomically, and it cannot decide what to do when the task is already in progress.

## API

| Method | Transaction | Purpose |
|--------|-------------|---------|
| `Task selectTaskForUpdate(long taskId)` | `@Transactional(MANDATORY)`: **requires** an active transaction | `SELECT ... FOR UPDATE`. Locks the task row until the caller's transaction ends. Returns `null` if the task does not exist. |
| `boolean updateAndBumpVersion(Task task)` (existing) | Joins the caller's transaction | Saves the task with an optimistic `version` check. |
| `long writeTaskLog(...)` (existing) | Joins the caller's transaction | Records the status change in `task_logs`, so manual changes appear in the task history like doer method runs. |
| `void triggerTaskReloadFromDb(long taskId)` (existing) | `@Transactional(NOT_SUPPORTED)`: **requires no** active transaction | Tells the scheduler that the task changed. Must be called **after** the client's transaction has committed. Otherwise the scheduler reads the old row. |

## Usage

```java
@Transactional(Transactional.TxType.REQUIRED)
public Long cancelOrder(UUID orderId) throws Exception {
    Order order = orderRepository.findById(orderId);
    if (order == null) {
        return null;
    }
    Task task = doerService.selectTaskForUpdate(order.getTaskId());
    if (task == null) {
        return null;
    }
    String initialStatus = task.getStatus();
    orderRepository.markCancelled(orderId);    // business data, same transaction
    task.setStatus(ORDER_CANCELLED);
    task.setFailingSince(null);
    // Hijacking: if a doer method is running on this task, its result is discarded
    // (its completion update fails the version check). To back off instead, return
    // null here when task.isInProgress().
    task.setInProgress(false);
    if (!doerService.updateAndBumpVersion(task)) {
        throw new OptimisticLockException("Task changed concurrently");
    }
    doerService.writeTaskLog(task.getId(), initialStatus, task.getStatus(),
            "OrderService", "cancelOrder", null, null, null);
    return task.getId();
}

// Caller, outside any transaction:
Long taskId = orders.cancelOrder(orderId);
if (taskId != null) {
    doerService.triggerTaskReloadFromDb(taskId);
}
```

## Semantics

- **Locking.** While the client holds the row lock, the scheduler on any node blocks on its own `UPDATE ... WHERE version = ?`. After the client commits, the version has changed, so the scheduler gets `OptimisticLockException` and skips the task. A doer method can never overwrite the client's change.
- **Task in progress.** A doer method is running somewhere. The client either:
  - **backs off**: rolls back or returns without changes; or
  - **hijacks**: sets a new status and `in_progress = false`, then saves. The running method's completion update then fails the version check, so its status change and unloaders are rolled back. Side effects it has already made outside the database are **not** undone. Only hijack when that is acceptable.

## Implementation

- `DoerService.selectTaskForUpdate`: same as `loadTask`, plus `FOR UPDATE`.
- Generated `DoerServiceImpl` overrides that delegate to `super`:
  - `selectTaskForUpdate` with `@Transactional(MANDATORY)`;
  - `triggerTaskReloadFromDb` with `@Transactional(NOT_SUPPORTED)`.
- `processTask`: on `OptimisticLockException`, call `triggerTaskReloadFromDb` after removing the task from `inProgressTasks`. Without this, a task hijacked while running on the same node waits for the next full reload, because `reloadQueuedTask` skips tasks that are in progress locally.
- Deprecate `runWithTask` in favour of this API.

## Tests

- `DoerServiceITCase`:
  - lock, save and trigger → status persisted and task re-queued;
  - a second `selectTaskForUpdate` blocks until the first commits;
  - a doer method that finishes after a hijack gets `OptimisticLockException` and the client's status is kept;
  - `selectTaskForUpdate` without a transaction fails.
- `GeneratorITCase`: generated overrides carry `MANDATORY` / `NOT_SUPPORTED`.
