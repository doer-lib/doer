package com.doer.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.lang.model.element.Element;

import com.doer.AcceptStatus;
import com.doer.DoerConcurrency;
import com.doer.OnException;

class DoerMethodInfo {
    String className;
    String methodName;
    List<String> parameterTypes = new ArrayList<>();

    List<AcceptStatus> acceptList = new ArrayList<>();
    OnException onException;
    DoerConcurrency concurrency;

    List<String> emitList = new ArrayList<>();

    Element element;

    public String getDomainName() {
        if (concurrency == null) {
            return className;
        } else {
            return className + "." + methodName;
        }
    }

    @Override
    public String toString() {
        return className + "." + methodName + "(" + parameterTypes + ")" + "\n"
                + " IN: " + acceptList.stream().map(a -> a.value()).collect(Collectors.toList()) + "\n"
                + " out: " + emitList;
    }
}
