package com.doer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a doer method: {@link DoerService} runs it for tasks in the status
 * {@link #value()}. Repeat the annotation to accept several statuses with one
 * method. A status can be accepted by only one doer method.
 * <p>
 * The method takes the {@link Task} and the task data it needs as parameters,
 * and sets the next status with {@link Task#setStatus(String)}. Task data is
 * loaded with a {@link TaskDataLoader} before the method runs and saved with a
 * {@link TaskDataSaver} after it. When the method fails, the task keeps its
 * status and is retried as {@link RetryPolicy} says.
 *
 * <pre>
 * &#64;AcceptStatus("New order created")
 * &#64;AcceptStatus(value = "Payment pending", delay = "5 min")
 * public void processOrder(Task task, OrderData order) {
 *     // ...
 *     task.setStatus("Goods allocated");
 * }
 * </pre>
 *
 * @see RetryPolicy
 */
@Repeatable(AcceptStatuses.class)
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.CLASS)
public @interface AcceptStatus {
    /** The status, at most 50 characters, without leading or trailing whitespace. */
    String value();

    /**
     * How long the task waits after it was last modified (usually when it got the
     * status) before the method runs, e.g. "30s", "5 min", "2h", "1 day"; "" means
     * as soon as possible. Must be greater than zero.
     */
    String delay() default "";
}
