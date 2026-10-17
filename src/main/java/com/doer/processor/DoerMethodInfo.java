package com.doer.processor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.lang.model.element.Element;

class DoerMethodInfo {
    static final Comparator<DoerMethodInfo> BY_SIGNATURE = Comparator
            .comparing((DoerMethodInfo m) -> m.className)
            .thenComparing(m -> m.methodName)
            .thenComparing(m -> m.parameterTypes.toString());

    /** A status from {@code @AcceptStatus}; the delay fields are null when the task is run as soon as possible. */
    record Accept(String status, String delayText, Duration delay) {
    }

    String className;
    String methodName;
    List<String> parameterTypes = new ArrayList<>();

    List<Accept> acceptList = new ArrayList<>();

    /** Null when the method has no {@code @RetryPolicy} (defaults are used). */
    String retryIntervalText;
    String retryDurationText;
    Duration retryInterval;
    /** Null means retry forever. */
    Duration retryDuration;
    /** Status set when retryDuration has passed; null means the status is set to null. */
    String fallbackStatus;

    /** Resolved concurrency domain name. */
    String domainName;

    List<String> emitList = new ArrayList<>();

    Element element;

    boolean hasRetryPolicy() {
        return retryIntervalText != null;
    }

    @Override
    public String toString() {
        return className + "." + methodName + "(" + parameterTypes + ")" + "\n"
                + " IN: " + acceptList.stream().map(Accept::status).toList() + "\n"
                + " out: " + emitList;
    }
}
