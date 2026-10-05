package com.doer;

@FunctionalInterface
public interface TaskAndDataUpdater<T> {
    void update(Task task, T data) throws Exception;
}
