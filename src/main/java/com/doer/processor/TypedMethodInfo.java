package com.doer.processor;

/** A bean method bound to a type: a task data loader or saver. */
record TypedMethodInfo(String className, String methodName, String type) {
}
