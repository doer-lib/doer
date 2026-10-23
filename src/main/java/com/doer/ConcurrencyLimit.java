package com.doer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Sets how many tasks the doer methods of a concurrency group can run in
 * parallel on each node. A group without a declared limit runs 2 tasks in
 * parallel. The value must be at least 1.
 * <p>
 * On a class, it sets the limit of the group the class identifies: the group of
 * its doer methods and of the classes and methods that join it with
 * {@link ConcurrencyGroup}. Such a class can have {@code @ConcurrencyGroup} only
 * of itself.
 * <p>
 * On a doer method ({@link AcceptStatus}), it gives the method an exclusive
 * group with this limit, separate from the group of its class: no other method
 * runs in it. The method must not have {@code @ConcurrencyGroup}.
 *
 * <h2>Example</h2>
 * <pre>
 * &#64;ConcurrencyLimit(5)
 * public class Payments {
 *     &#64;AcceptStatus("Goods reserved")
 *     public void payOrder(Task task, Order order) { ... }     // group of Payments: 5
 *
 *     &#64;AcceptStatus("Refund requested")
 *     &#64;ConcurrencyLimit(1)
 *     public void refundOrder(Task task, Order order) { ... }  // exclusive group: 1
 * }
 * </pre>
 */
@Retention(RetentionPolicy.CLASS)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface ConcurrencyLimit {
    /** Maximum number of tasks of this group that run at the same time on one node. */
    int value();
}
