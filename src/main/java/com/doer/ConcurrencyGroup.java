package com.doer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Joins a class or a doer method to the concurrency group of another class.
 * <p>
 * Each class with doer methods ({@link AcceptStatus}) identifies a concurrency
 * group, and all its doer methods run in it. With this annotation, any class or
 * doer method joins the group of the class given in {@link #value()} instead.
 * When that class itself has {@code @ConcurrencyGroup}, the reference is
 * followed, up to a class without it or with {@code @ConcurrencyGroup} of
 * itself; a cycle of references is a compile error.
 * <p>
 * The {@link ConcurrencyLimit} of a group sets how many tasks the doer methods
 * of the group can run in parallel on each node (2 when the limit is not
 * declared).
 *
 * <h2>Example</h2>
 * <pre>
 * &#64;ConcurrencyLimit(5)
 * public class Warehouse {
 * }
 *
 * &#64;ConcurrencyGroup(Warehouse.class)
 * public class Shipping {
 *     &#64;AcceptStatus("Order paid")
 *     public void shipOrder(Task task, Order order) { ... }  // group of Warehouse
 * }
 *
 * public class Payments {
 *     &#64;AcceptStatus("Goods reserved")
 *     public void payOrder(Task task, Order order) { ... }   // group of Payments
 *
 *     &#64;AcceptStatus("Order returned")
 *     &#64;ConcurrencyGroup(Warehouse.class)
 *     public void restock(Task task, Order order) { ... }    // group of Warehouse
 * }
 * </pre>
 */
@Retention(RetentionPolicy.CLASS)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface ConcurrencyGroup {
    /** Class that identifies the concurrency group the annotated method or class runs in. */
    Class<?> value();
}
