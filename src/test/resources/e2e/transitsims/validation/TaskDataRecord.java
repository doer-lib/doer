package transitsims.validation;

/** A record as task data, loaded by {@link TaskDataMethods}; it has no saver. */
public record TaskDataRecord(long taskId, String loadedBy) {
}
