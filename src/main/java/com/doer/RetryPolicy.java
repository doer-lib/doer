package com.doer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

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
