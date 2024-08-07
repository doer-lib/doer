package com.doer;

public class OptimisticLockException extends RuntimeException {
    Long taskId;
    public OptimisticLockException(String message) {
        super(message);
    }
}
