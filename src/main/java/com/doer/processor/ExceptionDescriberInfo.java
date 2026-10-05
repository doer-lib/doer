package com.doer.processor;

import java.util.List;

class ExceptionDescriberInfo {
    String type;
    List<String> typeParents;
    String className;
    String methodName;

    @Override
    public String toString() {
        return className + "." + methodName + " -> " + type;
    }
}
