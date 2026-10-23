package com.doer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method of a bean that adds details of an exception to the task log.
 * The method takes a {@link TaskSnapshot}, the exception of the type it
 * describes ({@link Throwable} or any subclass), and a
 * {@code jakarta.json.JsonObjectBuilder}, and returns void.
 * <p>
 * When a doer method ({@link AcceptStatus}), its {@link TaskDataLoader} or its
 * {@link TaskDataSaver} fails, Doer writes a task_logs row for the failure. Its
 * {@code exception_type} column holds the class name of the exception, and its
 * {@code extra_json} column holds the JSON object that the describers have
 * filled in. The describer gets the task as the failure is written.
 *
 * <h2>Example</h2>
 * <pre>
 * &#64;ExceptionDescriber
 * public void describeBankError(TaskSnapshot task, BankException e, JsonObjectBuilder json) {
 *     json.add("bankCode", e.getCode());
 * }
 * </pre>
 *
 * <h2>How the JSON is built</h2>
 * Doer adds the message of the exception as {@code "message"}, then calls every
 * describer whose type the exception is an instance of, describers of base
 * classes first, all with the same builder. It does the same for the cause and
 * the suppressed exceptions, recursively, and adds their objects as
 * {@code "cause"} and {@code "suppressed"}. When nothing is added,
 * {@code extra_json} is null.
 * <p>
 * When a describer fails, the failure is logged, and the task log is written
 * with what has been added so far: the failure of the doer method is still
 * recorded.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.CLASS)
public @interface ExceptionDescriber {
}
