package com.doer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a concurrency group with a limit. On a class, the class identifies the group, and other classes and
 * methods join it with {@code @ConcurrencyGroup}. On a doer method, it creates a group used only by that method; the
 * method must have {@code @AcceptStatus} and must not have {@code @ConcurrencyGroup}. A group without a declared
 * limit runs 2 tasks at a time.
 */
@Retention(RetentionPolicy.CLASS)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface ConcurrencyLimit {
    /** Maximum number of tasks of this group that run at the same time on one node. */
    int value();
}
