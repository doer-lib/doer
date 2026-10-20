package com.doer;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The timer of the scheduler: one daemon thread "doer-scheduler" that calls {@code onWakeUp} at the planned time.
 * {@code onWakeUp} only hands the work to the executor of the container, so the work of Doer runs in the threads of
 * the container. {@code onWakeUp} is called without the lock of the timer.
 */
final class ScheduledWakeUpTimer implements WakeUpTimer {

    private final ScheduledThreadPoolExecutor timer;
    private final Runnable onWakeUp;
    private ScheduledFuture<?> planned;
    /** {@link System#nanoTime} of the planned wake-up. */
    private long plannedAt;
    /** Number of the last planned wake-up: a wake-up that was replaced meanwhile does not clear the new one. */
    private long plannedNumber;

    ScheduledWakeUpTimer(Runnable onWakeUp) {
        this.onWakeUp = onWakeUp;
        timer = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "doer-scheduler");
            thread.setDaemon(true);
            return thread;
        });
        timer.setRemoveOnCancelPolicy(true);
    }

    @Override
    public synchronized void wakeUpNoLaterThan(Duration delay) {
        long at = System.nanoTime() + delay.toNanos();
        if (planned != null) {
            if (plannedAt - at <= 0) {
                return;
            }
            planned.cancel(false);
        }
        long number = ++plannedNumber;
        plannedAt = at;
        planned = timer.schedule(() -> wakeUp(number), delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void wakeUp(long number) {
        synchronized (this) {
            if (number != plannedNumber) {
                return;
            }
            planned = null;
        }
        onWakeUp.run();
    }

    @Override
    public synchronized Duration plannedDelay() {
        return planned == null ? null : Duration.ofNanos(plannedAt - System.nanoTime());
    }

    @Override
    public synchronized void shutdown() {
        planned = null;
        timer.shutdownNow();
    }
}
