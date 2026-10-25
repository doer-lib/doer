package com.doer;

import java.time.Instant;

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
