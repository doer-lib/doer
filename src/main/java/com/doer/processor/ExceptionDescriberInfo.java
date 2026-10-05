package com.doer.processor;

import java.util.List;

/** @param typeParents superclasses of {@code type} that have their own describer, nearest first */
record ExceptionDescriberInfo(String className, String methodName, String type, List<String> typeParents) {
}
