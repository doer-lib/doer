package com.doer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Runs the annotated class or doer method in the concurrency group identified by a class. Classes and methods of one
 * group share its {@link ConcurrencyLimit}. When the referenced class itself has {@code @ConcurrencyGroup} of another
 * class, the reference is followed.
 * <p>
 * On a class with {@code @ConcurrencyLimit} the value must be the annotated class itself. A method with this
 * annotation must have {@code @AcceptStatus} and must not have {@code @ConcurrencyLimit}.
 */
@Retention(RetentionPolicy.CLASS)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface ConcurrencyGroup {
    /** Class that identifies the concurrency group the annotated method or class runs in. */
    Class<?> value();
}
