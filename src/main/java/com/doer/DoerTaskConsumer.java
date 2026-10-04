package com.doer;

@FunctionalInterface
public interface DoerTaskConsumer {
    void apply(Task task) throws Exception;
}
