package com.doer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Container of repeated {@link AcceptStatus} annotations: write {@code @AcceptStatus} several times instead. */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.CLASS)
public @interface AcceptStatuses {
    AcceptStatus[] value();
}
