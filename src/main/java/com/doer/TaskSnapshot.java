package com.doer;

import java.time.Instant;

/**
 * Immutable copy of a task, given to the {@link TaskDataLoader}, {@link TaskDataSaver} and {@link ExceptionDescriber}
 * methods: they see the task as it is at their call, and can not change it.
 */
public record TaskSnapshot(
        Long id,
        String status,
        boolean inProgress,
        Instant created,
        Instant modified,
        Instant failingSince,
        int version) {

    public static TaskSnapshot of(Task task) {
        return new TaskSnapshot(task.getId(), task.getStatus(), task.isInProgress(), task.getCreated(),
                task.getModified(), task.getFailingSince(), task.getVersion());
    }
}
