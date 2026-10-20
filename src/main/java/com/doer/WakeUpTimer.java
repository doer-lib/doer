package com.doer;

import java.time.Duration;

/**
 * The timer of the scheduler of DoerService: wakes the scheduler up at the planned time. {@link ScheduledWakeUpTimer}
 * in production; the tests fire a manual one. Thread-safe.
 */
interface WakeUpTimer {

    /** Plans a wake-up after {@code delay}, unless one is planned earlier. */
    void wakeUpNoLaterThan(Duration delay);

    /** Time left to the planned wake-up, null when none is planned. */
    Duration plannedDelay();

    /** Cancels the planned wake-up and stops the timer. */
    void shutdown();
}
