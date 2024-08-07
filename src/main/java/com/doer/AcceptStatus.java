package com.doer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Repeatable(AcceptStatuses.class)
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.CLASS)
public @interface AcceptStatus {
    String value();

    String delay() default "";
}
