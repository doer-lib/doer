package com.doer.processor;

class TaskDataLoaderInfo {
    String type;
    String className;
    String methodName;

    @Override
    public String toString() {
        return className + "." + methodName + " -> " + type;
    }
}
