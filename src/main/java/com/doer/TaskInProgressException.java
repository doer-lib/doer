package com.doer;

public class TaskInProgressException extends RuntimeException {
    private final long taskId;

    public TaskInProgressException(long taskId) {
        super("Task is in progress. TaskId: " + taskId);
        this.taskId = taskId;
    }

    public long getTaskId() {
        return taskId;
    }
}
