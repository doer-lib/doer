package com.doer.processor;

import com.doer.Task;
import com.sun.source.tree.BlockTree;
import com.sun.source.tree.CaseTree;
import com.sun.source.tree.ClassTree;
import com.sun.source.tree.ConditionalExpressionTree;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.LambdaExpressionTree;
import com.sun.source.tree.LiteralTree;
import com.sun.source.tree.MemberSelectTree;
import com.sun.source.tree.MethodInvocationTree;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.ParenthesizedTree;
import com.sun.source.tree.SwitchExpressionTree;
import com.sun.source.tree.Tree;
import com.sun.source.tree.TypeCastTree;
import com.sun.source.tree.YieldTree;
import com.sun.source.util.TreePath;
import com.sun.source.util.TreePathScanner;
import com.sun.source.util.Trees;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;

/**
 * Finds constant statuses passed to {@link Task#setStatus(String)} in method
 * bodies and adds them to {@link DoerMethodInfo#emitList}.
 * <p>
 * Must scan only classes that javac has already attributed (after the ANALYZE task event), so that
 * {@link Trees#getElement} only reads symbols. Attributing method bodies from an annotation processor,
 * before the classes it generates exist, breaks compilation of anonymous classes on javac 17-25.
 */
class SetStatusFinder {

    private final Trees trees;
    private final List<DoerMethodInfo> methods;

    /** @param methods doer methods; methods that set a status but are not doer methods are added to it */
    SetStatusFinder(ProcessingEnvironment processingEnv, List<DoerMethodInfo> methods) {
        this.trees = Trees.instance(processingEnv);
        this.methods = methods;
    }

    /** Scans a top-level class with its nested classes. */
    void scanTopLevelType(TypeElement type) {
        if (type == null || trees.getPath(type) == null) {
            // package-info and module-info have no class tree
            return;
        }
        scanType(type);
    }

    private void scanType(TypeElement type) {
        if (isClassGenerated(type)) {
            return;
        }
        for (Element enclosed : type.getEnclosedElements()) {
            if (enclosed instanceof TypeElement) {
                scanType((TypeElement) enclosed);
            } else if (enclosed instanceof ExecutableElement) {
                scanExecutable((ExecutableElement) enclosed);
            }
        }
    }

    private boolean isClassGenerated(TypeElement e) {
        for (AnnotationMirror annotationMirror : e.getAnnotationMirrors()) {
            if (annotationMirror.getAnnotationType().toString().endsWith(".Generated")) {
                return true;
            }
        }
        return false;
    }

    private void scanExecutable(ExecutableElement e) {
        TreePath methodPath = trees.getPath(e);
        if (methodPath == null) {
            // Enum and record can have methods without body
            // We just skip them
            return;
        }
        BlockTree body = ((MethodTree) methodPath.getLeaf()).getBody();
        if (body == null || !body.toString().contains(".setStatus(")) {
            return;
        }
        List<String> statuses = new ArrayList<>();
        new StatusCallScanner().scan(new TreePath(methodPath, body), statuses);
        if (statuses.isEmpty()) {
            return;
        }

        String className = e.getEnclosingElement().asType().toString();
        String methodName = e.getSimpleName().toString();
        List<String> parameterTypes = e.getParameters()
                .stream()
                .map(VariableElement::asType)
                .map(TypeMirror::toString)
                .collect(Collectors.toList());
        DoerMethodInfo doerMethod = findDoerMethodInfo(className, methodName, parameterTypes);
        if (doerMethod.element == null) {
            doerMethod.element = e;
        }
        doerMethod.emitList.addAll(statuses);
    }

    private DoerMethodInfo findDoerMethodInfo(String className, String methodName, List<String> parameterTypes) {
        for (DoerMethodInfo method : methods) {
            if (className.equals(method.className)
                    && methodName.equals(method.methodName)
                    && parameterTypes.equals(method.parameterTypes)) {
                return method;
            }
        }
        DoerMethodInfo newMethod = new DoerMethodInfo();
        newMethod.className = className;
        newMethod.methodName = methodName;
        newMethod.parameterTypes = parameterTypes;
        methods.add(newMethod);
        return newMethod;
    }

    // Walks the whole method body and collects statuses of every Task.setStatus call.
    private class StatusCallScanner extends TreePathScanner<Void, List<String>> {
        @Override
        public Void visitMethodInvocation(MethodInvocationTree node, List<String> statuses) {
            if (isTaskSetStatusCall(node)) {
                collectConstantValues(new TreePath(getCurrentPath(), node.getArguments().get(0)), statuses);
            }
            return super.visitMethodInvocation(node, statuses);
        }

        private boolean isTaskSetStatusCall(MethodInvocationTree node) {
            ExpressionTree methodSelect = node.getMethodSelect();
            if (methodSelect.getKind() != Tree.Kind.MEMBER_SELECT || node.getArguments().size() != 1) {
                return false;
            }
            MemberSelectTree memberSelect = (MemberSelectTree) methodSelect;
            if (!"setStatus".contentEquals(memberSelect.getIdentifier())) {
                return false;
            }
            ExpressionTree receiver = memberSelect.getExpression();
            TreePath receiverPath = new TreePath(new TreePath(getCurrentPath(), methodSelect), receiver);
            Element element = trees.getElement(receiverPath);

            String className;
            if (element == null) {
                // WORKAROUND for Java 1.8
                // javac 1.8 may not provide element here.
                // So we use heuristics to find if it is Task object whose setStatus is being called.
                String name = receiver.toString();
                boolean looksLikeTask = name.toLowerCase().endsWith("task") || name.equals("t");
                className = looksLikeTask ? Task.class.getName() : "UnknownType";
            } else if (element.getKind() == ElementKind.METHOD) {
                className = ((ExecutableElement) element).getReturnType().toString();
            } else if (element.getKind() == ElementKind.CONSTRUCTOR) {
                className = element.getEnclosingElement().toString();
            } else {
                className = element.asType().toString();
            }
            return Task.class.getName().equals(className);
        }
    }

    // Adds all constant values the status expression may evaluate to.
    // Non-constant expressions (method calls, concatenation with variables, ...) are ignored.
    private void collectConstantValues(TreePath path, List<String> statuses) {
        Tree expression = path.getLeaf();
        switch (expression.getKind()) {
            case NULL_LITERAL:
                statuses.add(null);
                break;
            case STRING_LITERAL:
                statuses.add(((LiteralTree) expression).getValue().toString());
                break;
            case PARENTHESIZED:
                collectConstantValues(new TreePath(path, ((ParenthesizedTree) expression).getExpression()), statuses);
                break;
            case TYPE_CAST:
                collectConstantValues(new TreePath(path, ((TypeCastTree) expression).getExpression()), statuses);
                break;
            case CONDITIONAL_EXPRESSION:
                ConditionalExpressionTree conditional = (ConditionalExpressionTree) expression;
                collectConstantValues(new TreePath(path, conditional.getTrueExpression()), statuses);
                collectConstantValues(new TreePath(path, conditional.getFalseExpression()), statuses);
                break;
            case IDENTIFIER:
            case MEMBER_SELECT:
                addConstantVariableValue(trees.getElement(path), statuses);
                break;
            case SWITCH_EXPRESSION:
                new SwitchResultScanner(statuses).scan(path, null);
                break;
            default:
                break;
        }
    }

    private void addConstantVariableValue(Element element, List<String> statuses) {
        if (element == null) {
            // workaround for java 1.8
            // no workaround found
            return;
        }
        ElementKind kind = element.getKind();
        if ((kind == ElementKind.FIELD || kind == ElementKind.LOCAL_VARIABLE)
                && "java.lang.String".equals(element.asType().toString())) {
            Object value = ((VariableElement) element).getConstantValue();
            if (value != null) {
                statuses.add(value.toString());
            }
        }
    }

    // Collects results of a single switch expression: arrow case expressions and yield values.
    private class SwitchResultScanner extends TreePathScanner<Void, Void> {
        private final List<String> statuses;
        private SwitchExpressionTree root;

        SwitchResultScanner(List<String> statuses) {
            this.statuses = statuses;
        }

        @Override
        public Void visitSwitchExpression(SwitchExpressionTree node, Void p) {
            if (root != null) {
                // Yields of nested switch expressions belong to them
                return null;
            }
            root = node;
            return scan(node.getCases(), p);
        }

        @Override
        public Void visitCase(CaseTree node, Void p) {
            Tree body = node.getBody();
            if (node.getCaseKind() == CaseTree.CaseKind.RULE && body instanceof ExpressionTree) {
                collectConstantValues(new TreePath(getCurrentPath(), body), statuses);
                return null;
            }
            return super.visitCase(node, p);
        }

        @Override
        public Void visitYield(YieldTree node, Void p) {
            collectConstantValues(new TreePath(getCurrentPath(), node.getValue()), statuses);
            return null;
        }

        @Override
        public Void visitLambdaExpression(LambdaExpressionTree node, Void p) {
            return null;
        }

        @Override
        public Void visitClass(ClassTree node, Void p) {
            return null;
        }
    }
}
