package com.doer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * How a doer method ({@link AcceptStatus}) is retried when it fails.
 * <p>
 * A failed task keeps its status, and the method runs again {@link #interval()}
 * after the failed attempt. When an attempt fails after {@link #duration()} has
 * passed since the first failure, the task gets {@link #fallbackStatus()}
 * instead. A successful attempt ends the retries.
 * <p>
 * A doer method without this annotation is retried every 5 minutes for 1 day,
 * then the task gets the null status (it is finished).
 *
 * <pre>
 * &#64;AcceptStatus("Goods reserved")
 * &#64;RetryPolicy(interval = "5s", duration = "10 min", fallbackStatus = "Payment failed")
 * public void payOrder(Task task, Order order) {
 *     // ...
 *     task.setStatus("Order paid");
 * }
 * </pre>
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.METHOD)
public @interface RetryPolicy {
    /** Delay between attempts, e.g. "5s". Required. */
    String interval();

    /** How long to keep retrying; "" means forever. */
    String duration() default "";

    /** Status to set when duration has passed; "" means null. Requires duration. */
    String fallbackStatus() default "";
}
