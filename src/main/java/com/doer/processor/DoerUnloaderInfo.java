package com.doer.processor;

class DoerUnloaderInfo {
    String type;
    String className;
    String methodName;

    @Override
    public String toString() {
        return className + "." + methodName + " -> " + type;
    }
}
