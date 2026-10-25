package com.doer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Timer of the scheduler for the tests: remembers the planned wake-up in the time of {@code clock}, and wakes up only
 * when the test fires it.
 */
final class ManualWakeUpTimer implements WakeUpTimer {

    private final Runnable onWakeUp;
    private final Clock clock;
    private Instant plannedAt;

    ManualWakeUpTimer(Runnable onWakeUp, Clock clock) {
        this.onWakeUp = onWakeUp;
        this.clock = clock;
    }

    @Override
    public synchronized void wakeUpNoLaterThan(Duration delay) {
        Instant at = clock.instant().plus(delay);
        if (plannedAt == null || at.isBefore(plannedAt)) {
            plannedAt = at;
        }
    }

    @Override
    public synchronized Duration plannedDelay() {
        return plannedAt == null ? null : Duration.between(clock.instant(), plannedAt);
    }

    @Override
    public synchronized void shutdown() {
        plannedAt = null;
    }

    /** Time of the planned wake-up, null when none is planned. */
    synchronized Instant plannedAt() {
        return plannedAt;
    }

    /** Wakes up when the planned time has come by the clock; false when it has not, or none is planned. */
    boolean fireIfDue() {
        synchronized (this) {
            if (plannedAt == null || clock.instant().isBefore(plannedAt)) {
                return false;
            }
            plannedAt = null;
        }
        onWakeUp.run();
        return true;
    }
}
