# Task queues of DoerService

Status: implemented

The in-memory queues of a node: which tasks a node keeps, in what order it takes them, how it loads them from the database, and what happens to a task when the user's code fails or another node changes it.

## Classes

```
DoerService            threads, scheduler, loads from the database, re-check
 └─ TaskQueues         all queue state of the node: domains, tasks in progress, LIMITs of the next load
     └─ ConcurrencyDomain    limit, statuses, sub-queues
         └─ SubQueue         the head of one select of SelectTasks.sql
```

`TaskQueues`, `ConcurrencyDomain` and `SubQueue` are not thread-safe: DoerService uses them only while holding its monitor. The database work stays in DoerService, outside the monitor.

## Sub-queues

A concurrency domain has one sub-queue per select of `SelectTasks.sql`:

| Sub-queue | Tasks | Order | Ready |
|---|---|---|---|
| asap | not failing, status without a delay | `created, id` | at once |
| delayed, one per delay | not failing, status with this delay | `modified, created, id` | `modified + delay` |
| retry, one per retry interval | failing, status with this interval | `modified, created, id` | `modified + interval` |

A sub-queue holds the first tasks of its select (its buffer), at most `capacity`, which is also the LIMIT of the next load; `hasMoreInDb` tells that the database has more. A sub-queue is *drained* when its buffer is empty and the database has more.

A domain takes the next task from the ready heads of its sub-queues: the one created first.

**Order of the sub-queues = order of the LIMIT parameters.** DoerService gives each sub-queue its LIMIT and its rows by position. Both `DoerProcessor.generateSelectTaskSql` and the constructor of `ConcurrencyDomain` use the same order: domains by name; in a domain asap, delayed by delay, retry by interval, the shortest first. `SelectTasksOrderITCase` compares the sub-queues of a generated service with the selects of its SQL. The generator passes the retry interval of every status, so the runtime does not need to know the default interval.

## Loading from the database

One query loads all sub-queues (each select with its LIMIT) and all in_progress rows. The buffer of each sub-queue is replaced with its rows, except:
- a task in progress on this node;
- a row older than the copy of the task in a queue, or in progress in the database: it goes to re-check.

The in_progress rows are not counted as rows of a sub-queue: they come from a select without a LIMIT.

A task queued while a load runs (`triggerTaskReloadFromDb`, `facilitateCoordinatedUpdate`) may have changed after the select of the load, and the load would drop it with the buffers it replaces. So it is queued at once and also reloaded when the load ends (`reloadAfterLoad`). A load that is only scheduled, or the next one of `RUNNING_THEN_AGAIN`, has not selected yet and sees the task.

A drained sub-queue asks for a load only when no load is scheduled or running: that load refills it. Otherwise each start of a task during a load would ask for one more load with the same LIMITs.

LIMITs adapt to how fast a sub-queue is used (`TaskQueues.nextLimits`, `SubQueue.replace`):
- fewer rows than half of the LIMIT → the capacity is halved;
- a drained sub-queue, or 80% of `queueReloadInterval` since the last load → the sub-queues used less than half are halved;
- drained before 80% of the interval → the first drained sub-queue is doubled, unless all sub-queues would exceed `maxAllQueuesSize`.

No capacity goes below `minSingleQueueSize`.

## Failures of the user's code

`callDoerMethod` runs a doer method in three steps, all visible in the method: a transaction with `in_progress = true` and the loaders, the doer method, a transaction with the savers and the new status. A failure of a loader, of the method or of a saver is recorded as a failure of the doer method, in a third transaction: the initial status (or `onErrorStatus` after `errorTimeout`), `failing_since`, a task_logs row with the exception.

- The failed transaction is rolled back, and the task in memory is restored from a copy made before it. So a failed loader leaves no in_progress mark, a failed saver saves neither its data nor the new status, and the failure transaction updates the right version.
- A failure of the user's code is an `Exception`, or an error after which the thread can go on: `LinkageError`, `ServiceConfigurationError`, `OutOfMemoryError`, `StackOverflowError`, `AssertionError`. Other errors are not caught. `TaskVersionConflictException` is thrown on.
- The generated `createExtraJson` catches the same failures of the exception describers, so the failure of the method is still recorded.
- Loaders, savers and describers are logged as SEVERE, the doer method as WARNING.
- After an `InterruptedException` the interrupt flag is set again, once the failure is recorded.

## Several nodes

Every node loads all tasks that no node runs, so two nodes may start the same task; the second one gets `TaskVersionConflictException`. Rule: **the node that changed a task queues it** — the node that ran the step, or the node of `facilitateCoordinatedUpdate`.

- On a version conflict, at the start or at the end of a run, the node drops the task; it does not reload it.
- `facilitateCoordinatedUpdate` queues a copy of the task it has committed (its fields come from `UPDATE … RETURNING *`), with no query of its own; the caller gets the task itself. A saver must not change the task.
- When a node is asked to reload a task it runs itself (`facilitateCoordinatedUpdate`, or a full load that skips it), it reloads the task when the run ends (`TaskQueues.reloadWhenFinished`). Otherwise a task changed right after the node took it from its queue would be dropped until the next full load.
- Re-check is unchanged: after a full load a node still queues the tasks whose step another node has finished meanwhile.

`ClusterTest` runs two nodes on one database in memory: each step runs once, with at most one conflict per task.
