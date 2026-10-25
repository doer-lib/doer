package com.doer;

public class TaskNotFoundException extends RuntimeException {
    private final long taskId;

    public TaskNotFoundException(long taskId) {
        super("Task not found. TaskId: " + taskId);
        this.taskId = taskId;
    }

    public long getTaskId() {
        return taskId;
    }
}
