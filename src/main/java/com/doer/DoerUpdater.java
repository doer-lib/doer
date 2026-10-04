package com.doer;

@FunctionalInterface
public interface DoerUpdater<T> {
    void applyUpdate(Task task, T data) throws Exception;
}
