package com.doer.processor;

import com.doer.AcceptStatus;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.lang.model.element.Element;

class DoerMethodInfo {
    String className;
    String methodName;
    List<String> parameterTypes = new ArrayList<>();

    List<AcceptStatus> acceptList = new ArrayList<>();

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

    public String getDomainName() {
        return domainName;
    }

    @Override
    public String toString() {
        return className + "." + methodName + "(" + parameterTypes + ")" + "\n"
                + " IN: " + acceptList.stream().map(a -> a.value()).collect(Collectors.toList()) + "\n"
                + " out: " + emitList;
    }
}
