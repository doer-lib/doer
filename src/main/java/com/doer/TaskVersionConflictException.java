package com.doer;

public class TaskVersionConflictException extends RuntimeException {
    private final long taskId;

    public TaskVersionConflictException(long taskId) {
        super("Task version conflict. TaskId: " + taskId);
        this.taskId = taskId;
    }

    public long getTaskId() {
        return taskId;
    }
}
