# Scheduler of DoerService

Status: implemented

The scheduler decides when a node does the work that is not started by an event: runs a task whose delay or retry interval has passed, reloads the queues, resets stalled tasks, re-checks the tasks skipped by a load. It replaced the idle monitor of DoerService (`monitorIdleState`, `getNextCheckTime`, `onCheckTime`, `reCheckTask`). This document describes what the scheduler must do and how it works.

## Problems of the idle monitor

1. **It runs only when the whole node is idle.** `monitorIdleState` returns when any task is in progress. While a long task runs in domain A, a task of domain B waits past its delay until something else calls `processNextTask(B)`. While the node is never idle, there is no periodic reload and no reset of stalled tasks — the reset is needed most on a busy node.
2. **Database work under the monitor.** `onCheckTime` holds the lock of DoerService and calls `reCheckTask`, which loads the tasks one by one (N+1 queries). Meanwhile no task can start or finish.
3. **Re-check is not planned.** `getNextCheckTime` does not know about `reCheckTasks`: they are checked on whatever check comes next, up to a minute later, or only after the next load on a busy node. The intervals (2 s between checks, 10 s after `modified`) are hard-coded; the throttle uses the local clock, the window the database clock.
4. **Two copies of "what is due".** `getNextCheckTime` and `onCheckTime` decide it separately and already differ (`lastStalledTaskChecked == null` means "now" for one and "never" for the other). `onCheckTime` does a reload or a stalled-task reset, not both.
5. **A parked thread of the pool.** The monitor waits up to a minute in a thread of the container's `Executor` and renames it.
6. **Small ones.** After `stop()` the woken monitor still runs `onCheckTime`, which may reset stalled tasks of a stopped service. `InterruptedException` loses the interrupt flag. Tasks reset as stalled are queued only by the next periodic load.

## Required behavior

While the service runs with the scheduler enabled (`start(true)`):

- **R1. Ready tasks.** A queued task is started no later than about 100 ms after it becomes ready (`modified + delay` or `modified + retry interval`), when its domain has a free slot — no matter what other domains do.
- **R2. Periodic load.** The queues are reloaded `queueReloadInterval` after the last load (`lastSelectTime`), and at the start. A load that failed is retried after `queueReloadInterval`, as today: `lastSelectTime` is set before the select.
- **R3. Stalled tasks.** `resetStalledInProgressTasks(stalledTaskTimeout)` runs every `stalledTaskCheckInterval` (when not null), busy node or not. When it resets any task, the queues are reloaded, so the reset tasks are queued at once.
- **R4. Re-check.** The tasks skipped by a load (newer in memory, or in progress in the database) are checked every `reCheckInterval` (2 s) until each one is resolved:
  - gone from the database → dropped;
  - newer version in the database → replaces the queued copy (or is reloaded when it finishes, if it runs here);
  - unchanged for `reCheckWindow` (10 s) after its `modified` → dropped; the next load sees it.
  A new load replaces the whole re-check list, as today.
- **R5. Safety net.** The scheduler wakes up at least once a minute, even when nothing is planned, so a missed event costs at most a minute.
- **R6. No work after stop.** After `stop()` the scheduler starts nothing: no task, no load, no stalled-task reset, no re-check.
- **R7. The lock.** The plan is computed under the lock of DoerService; every query runs outside it.

With `start(false)` there is no timer: tasks start on events only (load, finish, `triggerTaskReloadFromDb`), and the tests or the application call `checkTasksBecomeReady()` to do the due work by hand.

## Architecture

```
DoerService
 ├─ nextWakeUp(now)      under the lock: the earliest time something is due
 ├─ onTick()             in a thread of the executor: does what is due
 ├─ reschedule()         under the lock: moves the timer earlier when the plan changed
 └─ WakeUpTimer          one daemon thread "doer-scheduler"; only calls executor.execute(this::onTick)
```

### WakeUpTimer

A package-private class around a `ScheduledThreadPoolExecutor` with one daemon thread (`setRemoveOnCancelPolicy(true)`):

- `wakeUpNoLaterThan(Duration delay)` — keeps the planned wake-up if it comes earlier, otherwise cancels it and plans a new one;
- `shutdown()` — cancels the wake-up and stops the thread.

Its thread does no work of Doer: it only hands `onTick` to the executor of the container. So the database, the transactions and the context of the container are used only in the threads of the container, as today. The thread is created in `start(true)` and stopped in `stop()`; a service started again gets a new one.

So that an application that forgets `stop()` does not leak the thread on redeploy, the generated service stops the service when the container destroys the bean:

```java
@PreDestroy
public void _destroy() {
    stop();
}
```

- `jakarta.annotation.PreDestroy` is in jakarta.annotation-api, which the generated code already needs for `@Generated`. CDI (Quarkus, WildFly, Open Liberty, Jetty with Weld) and Spring 6 call it. DoerService itself stays free of container annotations, as with `@Inject`.
- Usually the application has called `stop()` before (the `Shutdown` event, `SmartLifecycle.stop()`), so `stop()` must be idempotent.
- The whole `stop()`, not only the timer: tasks already handed to the executor start nothing after the bean is destroyed.
- No `@Transactional` on it: stopping opens no transaction.
- The container may destroy its executor before DoerService; `onTick` handed to it is rejected and logged (see below).
- Without a container (tests, `new`) `stop()` is still called by hand.

When the executor rejects `onTick` (the container shuts down, or its queue is full), the rejection is logged and the next wake-up is planned after the safety net interval (a minute), so a full queue does not stop the scheduler.

### nextWakeUp

One method under the lock, with the times in the database clock (`getDbNow()`). The earliest of:

| Due work | Time | When |
|---|---|---|
| load | `lastSelectTime + queueReloadInterval`, or now if never loaded | no load is scheduled or running |
| stalled-task reset | `lastStalledTaskChecked + stalledTaskCheckInterval` | `stalledTaskCheckInterval != null` |
| ready task | earliest ready time of the heads of the domain's sub-queues | the domain has a free slot |
| re-check | `lastReCheck + reCheckInterval`, or now | the re-check list is not empty and no re-check is running |
| safety net | `now + 60 s` | always |

The delay of the timer is `nextWakeUp - now`, at least 100 ms, so that work that is due but cannot be done (a load already running) does not spin.

`TaskQueues.earliestReadyTimeOfFreeDomains()` replaced `earliestReadyTime()`: it skips the domains without a free slot.

### onTick

`reloadTime()`, `stalledTaskResetTime()` and `reCheckTime()` give the time each work is due (null for none). `onTick` does the work whose time has come, `nextWakeUp` takes the earliest time, so the two cannot diverge. A tick does everything that is due, not one action per tick:

1. Under the lock: return if not running. Mark the due work as started, so a tick that comes again soon does not start it twice: `triggerQueuesReload()` (its state machine ignores a second request); `lastStalledTaskChecked` and `triggerStalledTaskReset()` (in its own `executor.execute`); `lastReCheck`, `reCheckRunning` and a copy of the ids to re-check.
2. Under the lock: in each domain with a ready head, `processNextTask` once per free slot, as when a task finishes. So the ready tasks start together, not one per tick.
3. Outside the lock: the re-check.
4. Under the lock: `reschedule()`.

`checkTasksBecomeReady()` becomes a call of `onTick()`. It stays public: the e2e `ValidationResource` uses it.

### reschedule

Called under the lock at the end of every change that can make something due earlier:

- `start(true)`, the end of a tick — also of the tick that follows each load (`checkTasksBecomeReady`);
- a task replaced with a row of the database (`replaceQueuedTask`: `reloadQueuedTask` and the re-check);
- the end of a run in `processNextTask`: the task may be queued again with a delay, and a slot is free — the domain may have a delayed head.

A stalled-task reset that reset any task triggers a load; the tick after the load plans the rest.

It is cheap (a pass over the sub-queues) and only moves the timer earlier, so calling it too often costs nothing. `wait`/`notify` and `monitorThread` go away.

### Re-check

`reCheckTasks` becomes a plain list guarded by the lock, with the number of the load that filled it (`loadGeneration`).

1. Under the lock: copy the ids, remember `loadGeneration`, set `reCheckRunning`.
2. Outside the lock: one `loadTasks(ids)`.
3. Under the lock: clear `reCheckRunning`. If a new load came meanwhile (`loadGeneration` changed), drop the result. Otherwise resolve each task as in R4; a newer task is put to the queues with the loaded row (`replaceQueuedTask`: `TaskQueues.replaceTask`, or `reloadWhenFinished` when it runs here), with no second load. A newer task in progress in the database is dropped, as before: the next load sees it. The tick then calls `reschedule()`.

A failed query is logged; the list stays and is checked on the next interval.

`reCheckInterval` and `reCheckWindow` become package-private fields next to `stalledTaskCheckInterval`, without public setters (see api-v1-design.md: no new public API). The throttle uses the clock of the database, as everything else in the plan.

### What is removed

`monitorIdleState`, `getNextCheckTime`, `onCheckTime`, `reCheckTask`, `monitorThread`, the renaming of the thread to `doer-idle-monitor`, `wait`/`notify` on DoerService, the `executor.execute(this::monitorIdleState)` calls in `processNextTask`. `monitorEnabled` too: the timer exists only between `start(true)` and `stop()`, and `wakeUpTimer == null` means there is no timer.

## Tests

`DoerServiceTest`, with real threads; all but the re-check ones failed with the idle monitor:

- `scheduler__should_start_delayed_task_of_free_domain_while_other_domain_is_busy` (R1);
- `scheduler__should_reload_queues_periodically_while_task_is_in_progress` (R2);
- `scheduler__should_reset_stalled_tasks_while_task_is_in_progress`, `scheduler__should_reload_queues_after_stalled_tasks_reset` (R3);
- `scheduler__should_queue_task_finished_on_other_node_soon_after_load` (R4);
- `scheduler__should_re_check_skipped_tasks_with_one_query_outside_lock` (R4, R7: `TstDoerService` counts the queries made under the lock);
- `scheduler__should_drop_re_check_of_older_load` (`loadGeneration`);
- `stop__should_stop_planned_work`: a due reset does not run after `stop()`, and the `doer-scheduler` thread ends (R6);
- `scheduler__should_run_task_queued_while_idle_after_its_delay`, renamed from `monitor__…`: waits for the planned wake-up of `WakeUpTimer` instead of a thread named `doer-idle-monitor`.

`TaskQueuesTest`: `earliestReadyTimeOfFreeDomains`. `GeneratorITCase.generated_service__should_stop_scheduler_when_bean_is_destroyed`: `_destroy()` has `@PreDestroy` and ends the `doer-scheduler` thread; a second `stop()` does nothing.

Each row of the table of `nextWakeUp` is covered by a test above, except the safety net.

## Steps

1. The failing tests.
2. `WakeUpTimer`, `nextWakeUp`, `onTick`, `reschedule`; the idle monitor removed.
3. Re-check: one query outside the lock, `loadGeneration`.
4. R3 reload after a reset; `triggerStalledTaskReset` does nothing after `stop()`; `@PreDestroy` in the generated service; "idle monitor" replaced with "scheduler" in queues-refactoring-design.md. The lost interrupt flag went away with the wait of the monitor. e2e-design.md keeps "idle monitor": it tells what was fixed then.

## Out of scope

- Throttling after an infrastructure failure. When `runTask` throws, `processNextTask` starts only one next task of the domain, but the next tick fills all free slots again. A later state of the domain (a pause until a time, a lower limit for a while) would make `freeSlots()` return less; the end of the pause would be one more time in `nextWakeUp`.
- A load that failed is retried after `queueReloadInterval` (R2), with no backoff.
- A task whose run ended with an infrastructure failure (`runTask` threw: the failure of the user's code could not be recorded, e.g. the database is down, or an error `callDoerMethod` does not catch) is dropped from memory; the next periodic load brings it back. A failure of the user's code is recorded and the task is queued again for its retry interval, as today.
- Every node resets stalled tasks of all nodes; the reset is idempotent.
