package com.doer;

@FunctionalInterface
public interface TaskUpdater {
    void update(Task task) throws Exception;
}
