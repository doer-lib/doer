package com.doer;

public interface CodeOne<T> {
    void call(Task task, T data) throws Exception;
}
