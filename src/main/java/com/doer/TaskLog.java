package com.doer;

import java.time.Instant;

/**
 * A row of task_logs: a change of a task by a doer method, a coordinated update, a hijack or a reset of a stalled
 * task. The database sets {@code seqNo} and {@code created}: they are null in a log to write, and
 * {@link DoerService#insertTaskLog(TaskLog)} returns the log with them.
 *
 * @param seqNo number of the log among the logs of the task: 1, 2, ... in the order they were written
 */
public record TaskLog(
        Long taskId,
        Integer seqNo,
        Instant created,
        String initialStatus,
        String finalStatus,
        String className,
        String methodName,
        Integer durationMs,
        String exceptionType,
        String extraJson) {

    /** A log to write: without {@code seqNo} and {@code created}. */
    public TaskLog(Long taskId, String initialStatus, String finalStatus, String className, String methodName,
            Integer durationMs, String exceptionType, String extraJson) {
        this(taskId, null, null, initialStatus, finalStatus, className, methodName, durationMs, exceptionType,
                extraJson);
    }
}
