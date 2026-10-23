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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;

/**
 * Finds constant statuses passed to {@link Task#setStatus(String)} in method
 * bodies and adds them to {@link DoerMethodInfo#emitList}.
 * <p>
 * When the status is a parameter of a private method, the constants passed to it are added to the methods that
 * call it. A private method can be called only in its top-level class, so its calls are found in one scan of that
 * class. Only calls in method and constructor bodies (with their lambdas and anonymous classes) are traced: not
 * method references, {@code new} of private constructors, field initializers and initializer blocks. Values the
 * private method assigns to the parameter itself are ignored: the statuses are the ones its callers pass.
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

    /** A parameter of a private method that is passed to {@link Task#setStatus(String)}. */
    private record StatusParameter(ExecutableElement method, int index) {
    }

    /** A call of a private method, in the body of the caller. */
    private record PrivateCall(ExecutableElement caller, TreePath invocation) {
    }

    /** Scans a top-level class with its nested classes. */
    void scanTopLevelType(TypeElement type) {
        if (type == null || trees.getPath(type) == null) {
            // package-info and module-info have no class tree
            return;
        }
        Map<ExecutableElement, TreePath> bodies = new LinkedHashMap<>();
        collectBodies(type, bodies);
        Set<StatusParameter> parameters = new LinkedHashSet<>();
        bodies.forEach((executable, body) -> scanExecutable(executable, body, parameters));
        if (!parameters.isEmpty()) {
            traceStatusParameters(bodies, parameters);
        }
    }

    private void collectBodies(TypeElement type, Map<ExecutableElement, TreePath> bodies) {
        if (isClassGenerated(type)) {
            return;
        }
        for (Element enclosed : type.getEnclosedElements()) {
            if (enclosed instanceof TypeElement nested) {
                collectBodies(nested, bodies);
            } else if (enclosed instanceof ExecutableElement executable) {
                TreePath methodPath = trees.getPath(executable);
                if (methodPath == null) {
                    // Implicit methods of enums and records have no tree
                    continue;
                }
                BlockTree body = ((MethodTree) methodPath.getLeaf()).getBody();
                if (body != null) {
                    bodies.put(executable, new TreePath(methodPath, body));
                }
            }
        }
    }

    private boolean isClassGenerated(TypeElement e) {
        return e.getAnnotationMirrors().stream()
                .anyMatch(a -> a.getAnnotationType().toString().endsWith(".Generated"));
    }

    private void scanExecutable(ExecutableElement e, TreePath body, Set<StatusParameter> parameters) {
        if (!body.getLeaf().toString().contains(".setStatus(")) {
            return;
        }
        List<String> statuses = new ArrayList<>();
        new StatusCallScanner(statuses, parameters).scan(body, null);
        addEmits(e, statuses);
    }

    /**
     * Adds the constants passed to the status parameters to the methods that pass them. When a call passes
     * a parameter of a private caller, that parameter is traced in turn.
     */
    private void traceStatusParameters(Map<ExecutableElement, TreePath> bodies, Set<StatusParameter> parameters) {
        Map<ExecutableElement, List<PrivateCall>> calls = new HashMap<>();
        bodies.forEach((caller, body) -> new PrivateCallScanner(caller, calls).scan(body, null));
        Deque<StatusParameter> queue = new ArrayDeque<>(parameters);
        Set<StatusParameter> traced = new HashSet<>(parameters);
        while (!queue.isEmpty()) {
            StatusParameter parameter = queue.remove();
            for (PrivateCall call : calls.getOrDefault(parameter.method(), List.of())) {
                ExpressionTree argument = ((MethodInvocationTree) call.invocation().getLeaf()).getArguments()
                        .get(parameter.index());
                List<String> statuses = new ArrayList<>();
                Set<StatusParameter> callerParameters = new LinkedHashSet<>();
                collectConstantValues(new TreePath(call.invocation(), argument), statuses, callerParameters);
                addEmits(call.caller(), statuses);
                for (StatusParameter callerParameter : callerParameters) {
                    if (traced.add(callerParameter)) {
                        queue.add(callerParameter);
                    }
                }
            }
        }
    }

    private void addEmits(ExecutableElement e, List<String> statuses) {
        if (statuses.isEmpty()) {
            return;
        }

        String className = e.getEnclosingElement().asType().toString();
        String methodName = e.getSimpleName().toString();
        List<String> parameterTypes = e.getParameters()
                .stream()
                .map(VariableElement::asType)
                .map(TypeMirror::toString)
                .toList();
        DoerMethodInfo doerMethod = findDoerMethodInfo(className, methodName, parameterTypes);
        if (doerMethod.element == null) {
            doerMethod.element = e;
        }
        doerMethod.emitList.addAll(statuses);
    }

    private DoerMethodInfo findDoerMethodInfo(String className, String methodName, List<String> parameterTypes) {
        return methods.stream()
                .filter(m -> className.equals(m.className)
                        && methodName.equals(m.methodName)
                        && parameterTypes.equals(m.parameterTypes))
                .findFirst()
                .orElseGet(() -> {
                    DoerMethodInfo newMethod = new DoerMethodInfo();
                    newMethod.className = className;
                    newMethod.methodName = methodName;
                    newMethod.parameterTypes = parameterTypes;
                    methods.add(newMethod);
                    return newMethod;
                });
    }

    // Walks the whole method body and collects statuses of every Task.setStatus call.
    private class StatusCallScanner extends TreePathScanner<Void, Void> {
        private final List<String> statuses;
        private final Set<StatusParameter> parameters;

        StatusCallScanner(List<String> statuses, Set<StatusParameter> parameters) {
            this.statuses = statuses;
            this.parameters = parameters;
        }

        @Override
        public Void visitMethodInvocation(MethodInvocationTree node, Void p) {
            if (isTaskSetStatusCall(node)) {
                collectConstantValues(new TreePath(getCurrentPath(), node.getArguments().get(0)), statuses,
                        parameters);
            }
            return super.visitMethodInvocation(node, p);
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
            TreePath receiverPath = new TreePath(new TreePath(getCurrentPath(), methodSelect),
                    memberSelect.getExpression());
            TypeMirror receiverType = trees.getTypeMirror(receiverPath);
            return receiverType != null && Task.class.getName().equals(receiverType.toString());
        }
    }

    // Walks the whole method body and collects the calls of private methods.
    private class PrivateCallScanner extends TreePathScanner<Void, Void> {
        private final ExecutableElement caller;
        private final Map<ExecutableElement, List<PrivateCall>> calls;

        PrivateCallScanner(ExecutableElement caller, Map<ExecutableElement, List<PrivateCall>> calls) {
            this.caller = caller;
            this.calls = calls;
        }

        @Override
        public Void visitMethodInvocation(MethodInvocationTree node, Void p) {
            TreePath methodSelect = new TreePath(getCurrentPath(), node.getMethodSelect());
            if (trees.getElement(methodSelect) instanceof ExecutableElement method
                    && method.getModifiers().contains(Modifier.PRIVATE)) {
                calls.computeIfAbsent(method, m -> new ArrayList<>()).add(new PrivateCall(caller, getCurrentPath()));
            }
            return super.visitMethodInvocation(node, p);
        }
    }

    // Adds all constant values the status expression may evaluate to, and the parameters of private methods
    // it may be, whose values are found at the calls of the methods.
    // Other non-constant expressions (method calls, concatenation with variables, ...) are ignored.
    private void collectConstantValues(TreePath path, List<String> statuses, Set<StatusParameter> parameters) {
        Tree expression = path.getLeaf();
        switch (expression.getKind()) {
            case NULL_LITERAL -> statuses.add(null);
            case STRING_LITERAL -> statuses.add(((LiteralTree) expression).getValue().toString());
            case PARENTHESIZED -> collectConstantValues(
                    new TreePath(path, ((ParenthesizedTree) expression).getExpression()), statuses, parameters);
            case TYPE_CAST -> collectConstantValues(
                    new TreePath(path, ((TypeCastTree) expression).getExpression()), statuses, parameters);
            case CONDITIONAL_EXPRESSION -> {
                ConditionalExpressionTree conditional = (ConditionalExpressionTree) expression;
                collectConstantValues(new TreePath(path, conditional.getTrueExpression()), statuses, parameters);
                collectConstantValues(new TreePath(path, conditional.getFalseExpression()), statuses, parameters);
            }
            case IDENTIFIER, MEMBER_SELECT -> {
                // String constants: final fields and locals initialized with a constant expression
                if (trees.getElement(path) instanceof VariableElement variable) {
                    if (variable.getConstantValue() instanceof String value) {
                        statuses.add(value);
                    } else {
                        addStatusParameter(variable, parameters);
                    }
                }
            }
            case SWITCH_EXPRESSION -> new SwitchResultScanner(statuses, parameters).scan(path, null);
            default -> {
            }
        }
    }

    private static void addStatusParameter(VariableElement variable, Set<StatusParameter> parameters) {
        if (variable.getEnclosingElement() instanceof ExecutableElement method
                && method.getModifiers().contains(Modifier.PRIVATE)) {
            // Locals and lambda parameters are not in the parameters of their enclosing method
            int index = method.getParameters().indexOf(variable);
            boolean varargs = method.isVarArgs() && index == method.getParameters().size() - 1;
            if (index >= 0 && !varargs) {
                parameters.add(new StatusParameter(method, index));
            }
        }
    }

    // Collects results of a single switch expression: arrow case expressions and yield values.
    private class SwitchResultScanner extends TreePathScanner<Void, Void> {
        private final List<String> statuses;
        private final Set<StatusParameter> parameters;
        private SwitchExpressionTree root;

        SwitchResultScanner(List<String> statuses, Set<StatusParameter> parameters) {
            this.statuses = statuses;
            this.parameters = parameters;
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
                collectConstantValues(new TreePath(getCurrentPath(), body), statuses, parameters);
                return null;
            }
            return super.visitCase(node, p);
        }

        @Override
        public Void visitYield(YieldTree node, Void p) {
            collectConstantValues(new TreePath(getCurrentPath(), node.getValue()), statuses, parameters);
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
