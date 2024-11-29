package com.doer.processor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;

import com.doer.Task;
import com.sun.source.tree.*;
import com.sun.source.tree.Tree.Kind;
import com.sun.source.util.*;

public class SetStatusFinder {

    RoundEnvironment roundEnv;
    ProcessingEnvironment processingEnv;
    Trees trees;
    CompilationUnitTree compilationUnitTree;
    ArrayList<String> statusList;
    List<DoerMethodInfo> methods;

    public SetStatusFinder(RoundEnvironment roundEnv, ProcessingEnvironment processingEnv) {
        this.roundEnv = roundEnv;
        this.processingEnv = processingEnv;
        trees = Trees.instance(processingEnv);
    }

    public void updateDoerMethods(List<DoerMethodInfo> methods) {
        this.methods = methods;
        Set<? extends Element> rootElements = roundEnv.getRootElements();
        for (Element element : rootElements) {
            element.accept(new MyElementVisitor(), null);
        }
    }

    private DoerMethodInfo findDoerMethodInfo(String className, String methodName, List<String> parameterTypes) {
        for (DoerMethodInfo method : methods) {
            if (className.equals(method.className)) {
                if (methodName.equals(method.methodName)) {
                    if (parameterTypes.equals(method.parameterTypes)) {
                        return method;
                    }
                }
            }
        }
        DoerMethodInfo newMethod = new DoerMethodInfo();
        newMethod.className = className;
        newMethod.methodName = methodName;
        newMethod.parameterTypes = parameterTypes;
        methods.add(newMethod);
        return newMethod;
    }

    class MyElementVisitor implements ElementVisitor<Object, Object> {
        @Override
        public Object visit(Element e, Object p) {
            return null;
        }

        @Override
        public Object visitPackage(PackageElement e, Object p) {
            return null;
        }

        @Override
        public Object visitType(TypeElement e, Object p) {
            if (isClassGenerated(e)) {
                return null;
            }
            for (Element x : e.getEnclosedElements()) {
                x.accept(this, null);
            }
            return null;
        }

        private boolean isClassGenerated(TypeElement e) {
            for (AnnotationMirror annotationMirror : e.getAnnotationMirrors()) {
                DeclaredType annotationType = annotationMirror.getAnnotationType();
                if (annotationType.toString().endsWith(".Generated")) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Object visitVariable(VariableElement e, Object p) {
            return null;
        }

        @Override
        public Object visitExecutable(ExecutableElement e, Object p) {
            String className = e.getEnclosingElement().asType().toString();
            String methodName = e.getSimpleName().toString();
            List<String> parameterTypes = e.getParameters()
                    .stream()
                    .map(VariableElement::asType)
                    .map(TypeMirror::toString)
                    .collect(Collectors.toList());

            TreePath treePath = trees.getPath(e);
            if (treePath == null) {
                // Enum and record can have methods without body
                // We just skip them
                return null;
            }
            compilationUnitTree = treePath.getCompilationUnit();

            MethodTree methodTree = trees.getTree(e); // declaration and parameters
            BlockTree blockTree = methodTree.getBody(); // sequence of statements
            if (blockTree == null) {
                return null;
            }
            if (!blockTree.toString().contains(".setStatus(")) {
                return null;
            }
            statusList = new ArrayList<>();
            MyTreeVisitor codeVisitor = new MyTreeVisitor();
            blockTree.accept(codeVisitor, null);
            if (!statusList.isEmpty()) {
                DoerMethodInfo doerMethod = findDoerMethodInfo(className, methodName, parameterTypes);
                if (doerMethod.element == null) {
                    doerMethod.element = e;
                }
                doerMethod.emitList.addAll(statusList);
            }
            return null;
        }

        @Override
        public Object visitTypeParameter(TypeParameterElement e, Object p) {
            return null;
        }

        @Override
        public Object visitUnknown(Element e, Object p) {
            return null;
        }
    }

    // This visitor look through code and find all constant strings passed as
    // argument to Task.setStatus method.
    class MyTreeVisitor implements TreeVisitor<Object, Object> {

        // True when analyzing expression is status for setStatus method
        LinkedList<Boolean> stack = new LinkedList<>();
        {
            stack.push(false);
        }

        @Override
        public Object visitAnnotatedType(AnnotatedTypeTree node, Object p) {
            return null;
        }

        @Override
        public Object visitAnnotation(AnnotationTree node, Object p) {
            return null;
        }

        @Override
        public Object visitMethodInvocation(MethodInvocationTree node, Object p) {
            ExpressionTree methodSelect = node.getMethodSelect();
            boolean thisIsSetStatusCall = false;
            if (methodSelect.getKind() == Tree.Kind.MEMBER_SELECT) {
                MemberSelectTree memberSelect = (MemberSelectTree) methodSelect;
                String methodName = memberSelect.getIdentifier().toString();
                if ("setStatus".equals(methodName)) {
                    if (node.getArguments().size() == 1) {
                        ExpressionTree expression = memberSelect.getExpression();
                        TreePath path = TreePath.getPath(compilationUnitTree, expression);
                        Element element = trees.getElement(path);

                        String className;
                        if (element == null) {
                            // WORKAROUND for Java 1.8
                            // javac 1.8 may not provide element here.
                            // So we use euristics to find if it is Task object whose setStatus is being
                            // called.
                            boolean isNameEndsWithTask = expression.toString().toLowerCase().endsWith("task");
                            boolean isNameT = expression.toString().equals("t");
                            if (isNameEndsWithTask || isNameT) {
                                className = Task.class.getName();
                            } else {
                                className = "UnknownType";
                            }
                        } else if (element.getKind() == ElementKind.METHOD) {
                            className = ((ExecutableElement) element).getReturnType().toString();
                        } else if (element.getKind() == ElementKind.CONSTRUCTOR) {
                            className = element.getEnclosingElement().toString();
                        } else {
                            className = element.asType().toString();
                        }
                        if (Task.class.getName().equals(className)) {
                            thisIsSetStatusCall = true;
                        }
                    }
                }
            }

            if (thisIsSetStatusCall) {
                stack.push(false);
                methodSelect.accept(this, null);
                stack.pop();
                stack.push(true);
                ExpressionTree statusExpression = node.getArguments().get(0);
                statusExpression.accept(this, null);
                stack.pop();
            } else {
                if (methodSelect.getKind() == Tree.Kind.IDENTIFIER) {
                } else if (methodSelect.getKind() == Tree.Kind.MEMBER_SELECT) {
                    stack.push(false);
                    methodSelect.accept(this, null);
                    for (ExpressionTree expression : node.getArguments()) {
                        expression.accept(this, null);
                    }
                    stack.pop();
                } else {
                    // nop
                }
            }
            return null;
        }

        @Override
        public Object visitAssert(AssertTree node, Object p) {
            return null;
        }

        @Override
        public Object visitAssignment(AssignmentTree node, Object p) {
            ExpressionTree expression = node.getExpression();
            if (expression != null) {
                expression.accept(this, null);
            }
            return null;
        }

        @Override
        public Object visitCompoundAssignment(CompoundAssignmentTree node, Object p) {
            node.getExpression().accept(this, null);
            return null;
        }

        @Override
        public Object visitBinary(BinaryTree node, Object p) {
            node.getLeftOperand().accept(this, null);
            node.getRightOperand().accept(this, null);
            return null;
        }

        @Override
        public Object visitBlock(BlockTree node, Object p) {
            for (StatementTree statement : node.getStatements()) {
                statement.accept(this, null);
            }
            return null;
        }

        @Override
        public Object visitBreak(BreakTree node, Object p) {
            return null;
        }

        @Override
        public Object visitCase(CaseTree node, Object p) {
            if (node.getStatements() != null) {
                for (StatementTree statement : node.getStatements()) {
                    statement.accept(this, null);
                }
            }
            return null;
        }

        @Override
        public Object visitCatch(CatchTree node, Object p) {
            node.getBlock().accept(this, null);
            return null;
        }

        @Override
        public Object visitClass(ClassTree node, Object p) {
            for (Tree member : node.getMembers()) {
                member.accept(this, null);
            }
            return null;
        }

        @Override
        public Object visitConditionalExpression(ConditionalExpressionTree node, Object p) {
            stack.push(false);
            node.getCondition().accept(this, null);
            stack.pop();
            node.getTrueExpression().accept(this, null);
            node.getFalseExpression().accept(this, null);
            return null;
        }

        @Override
        public Object visitContinue(ContinueTree node, Object p) {
            return null;
        }

        @Override
        public Object visitDoWhileLoop(DoWhileLoopTree node, Object p) {
            node.getStatement().accept(this, null);
            return null;
        }

        @Override
        public Object visitErroneous(ErroneousTree node, Object p) {
            return null;
        }

        @Override
        public Object visitExpressionStatement(ExpressionStatementTree node, Object p) {

            ExpressionTree expression = node.getExpression();
            Kind kind = expression.getKind();
            if (kind == Tree.Kind.NULL_LITERAL) {
                if (stack.peek()) {
                    statusList.add(null);
                }
            } else if (kind == Tree.Kind.STRING_LITERAL) {
                if (stack.peek()) {
                    Object value = ((LiteralTree) expression).getValue();
                    statusList.add(value.toString());
                }
            } else {
                expression.accept(this, null);
            }
            return null;
        }

        @Override
        public Object visitEnhancedForLoop(EnhancedForLoopTree node, Object p) {
            node.getStatement().accept(this, null);
            return null;
        }

        @Override
        public Object visitForLoop(ForLoopTree node, Object p) {
            node.getStatement().accept(this, null);
            return null;
        }

        @Override
        public Object visitIdentifier(IdentifierTree node, Object p) {
            if (stack.peek()) {
                TreePath path = TreePath.getPath(compilationUnitTree, node);
                Element element = trees.getElement(path);
                if (element == null) {
                    // workaround for java 1.8
                    // no workaround found
                } else if (element.getKind() == ElementKind.FIELD) {
                    if ("java.lang.String".equals(element.asType().toString())) {
                        String value = (String) ((VariableElement) element).getConstantValue();
                        if (value != null) {
                            statusList.add(value);
                        }
                    }
                }
            }
            return null;
        }

        @Override
        public Object visitIf(IfTree node, Object p) {
            stack.push(false);
            node.getCondition().accept(this, null);
            if (node.getThenStatement() != null) {
                node.getThenStatement().accept(this, null);
            }
            if (node.getElseStatement() != null) {
                node.getElseStatement().accept(this, null);
            }
            stack.pop();
            return null;
        }

        @Override
        public Object visitImport(ImportTree node, Object p) {
            return null;
        }

        @Override
        public Object visitArrayAccess(ArrayAccessTree node, Object p) {
            return null;
        }

        @Override
        public Object visitLabeledStatement(LabeledStatementTree node, Object p) {
            return null;
        }

        @Override
        public Object visitLiteral(LiteralTree node, Object p) {
            Kind kind = node.getKind();
            if (stack.peek()) {
                if (kind == Tree.Kind.NULL_LITERAL) {
                    statusList.add(null);
                } else if (kind == Tree.Kind.STRING_LITERAL) {
                    Object value = node.getValue();
                    statusList.add(value.toString());
                }
            }
            return null;
        }

        @Override
        public Object visitBindingPattern(BindingPatternTree node, Object p) {
            return null;
        }

        @Override
        public Object visitDefaultCaseLabel(DefaultCaseLabelTree node, Object p) {
            return null;
        }

        @Override
        public Object visitMethod(MethodTree node, Object p) {
            if (node.getBody() != null) {
                node.getBody().accept(this, null);
            }
            return null;
        }

        @Override
        public Object visitModifiers(ModifiersTree node, Object p) {
            return null;
        }

        @Override
        public Object visitNewArray(NewArrayTree node, Object p) {
            return null;
        }

        @Override
        public Object visitNewClass(NewClassTree node, Object p) {
            stack.push(false);
            for (ExpressionTree expression : node.getArguments()) {
                expression.accept(this, null);
            }
            if (node.getClassBody() != null) {
                for (Tree expression : node.getClassBody().getMembers()) {
                    expression.accept(this, null);
                }
            }
            stack.pop();
            return null;
        }

        @Override
        public Object visitLambdaExpression(LambdaExpressionTree node, Object p) {
            node.getBody().accept(this, null);
            return null;
        }

        @Override
        public Object visitPackage(PackageTree node, Object p) {
            return null;
        }

        @Override
        public Object visitParenthesized(ParenthesizedTree node, Object p) {
            node.getExpression().accept(this, null);
            return null;
        }

        @Override
        public Object visitReturn(ReturnTree node, Object p) {
            ExpressionTree expression = node.getExpression();
            if (expression != null) {
                expression.accept(this, null);
            }
            return null;
        }

        @Override
        public Object visitMemberSelect(MemberSelectTree node, Object p) {
            ExpressionTree expression = node.getExpression();
            Kind kind = expression.getKind();
            if (kind == Tree.Kind.IDENTIFIER) {
            } else if (kind == Tree.Kind.METHOD_INVOCATION || kind == Tree.Kind.NEW_CLASS) {
                stack.push(false);
                expression.accept(this, null);
                stack.pop();
            } else if (kind == Tree.Kind.MEMBER_SELECT) {
                MemberSelectTree memberSelect = (MemberSelectTree) expression;
                memberSelect.getExpression().accept(this, null);
            } else if (kind == Tree.Kind.PARENTHESIZED) {
                expression.accept(this, null);
            } else {
                // nop
            }
            if (stack.peek()) {
                TreePath path = TreePath.getPath(compilationUnitTree, node);
                Element element = trees.getElement(path);
                if (element.getKind() == ElementKind.FIELD) {
                    if ("java.lang.String".equals(element.asType().toString())) {
                        String value = (String) ((VariableElement) element).getConstantValue();
                        if (value != null) {
                            statusList.add(value);
                        }
                    }
                }
            }
            return null;
        }

        @Override
        public Object visitMemberReference(MemberReferenceTree node, Object p) {
            return null;
        }

        @Override
        public Object visitEmptyStatement(EmptyStatementTree node, Object p) {
            return null;
        }

        @Override
        public Object visitSwitch(SwitchTree node, Object p) {
            stack.push(false);
            for (CaseTree c : node.getCases()) {
                c.accept(this, null);
            }
            stack.pop();
            return null;
        }

        @Override
        public Object visitSwitchExpression(SwitchExpressionTree node, Object p) {
            return null;
        }

        @Override
        public Object visitSynchronized(SynchronizedTree node, Object p) {
            node.getBlock().accept(this, null);
            return null;
        }

        @Override
        public Object visitThrow(ThrowTree node, Object p) {
            return null;
        }

        @Override
        public Object visitCompilationUnit(CompilationUnitTree node, Object p) {
            return null;
        }

        @Override
        public Object visitTry(TryTree node, Object p) {
            node.getBlock().accept(this, null);
            for (CatchTree block : node.getCatches()) {
                block.getBlock().accept(this, null);
            }
            BlockTree finallyBlock = node.getFinallyBlock();
            if (finallyBlock != null) {
                finallyBlock.accept(this, null);
            }
            return null;
        }

        @Override
        public Object visitParameterizedType(ParameterizedTypeTree node, Object p) {
            return null;
        }

        @Override
        public Object visitUnionType(UnionTypeTree node, Object p) {
            return null;
        }

        @Override
        public Object visitIntersectionType(IntersectionTypeTree node, Object p) {
            return null;
        }

        @Override
        public Object visitArrayType(ArrayTypeTree node, Object p) {
            return null;
        }

        @Override
        public Object visitTypeCast(TypeCastTree node, Object p) {
            node.getExpression().accept(this, null);
            return null;
        }

        @Override
        public Object visitPrimitiveType(PrimitiveTypeTree node, Object p) {
            return null;
        }

        @Override
        public Object visitTypeParameter(TypeParameterTree node, Object p) {
            return null;
        }

        @Override
        public Object visitInstanceOf(InstanceOfTree node, Object p) {
            node.getExpression().accept(this, null);
            return null;
        }

        @Override
        public Object visitUnary(UnaryTree node, Object p) {
            return null;
        }

        @Override
        public Object visitVariable(VariableTree node, Object p) {
            if (node.getInitializer() != null) {
                stack.push(false);
                node.getInitializer().accept(this, null);
                stack.pop();
            }
            return null;
        }

        @Override
        public Object visitWhileLoop(WhileLoopTree node, Object p) {
            node.getStatement().accept(this, null);
            return null;
        }

        @Override
        public Object visitWildcard(WildcardTree node, Object p) {
            return null;
        }

        @Override
        public Object visitModule(ModuleTree node, Object p) {
            return null;
        }

        @Override
        public Object visitExports(ExportsTree node, Object p) {
            return null;
        }

        @Override
        public Object visitOpens(OpensTree node, Object p) {
            return null;
        }

        @Override
        public Object visitProvides(ProvidesTree node, Object p) {
            return null;
        }

        @Override
        public Object visitRequires(RequiresTree node, Object p) {
            return null;
        }

        @Override
        public Object visitUses(UsesTree node, Object p) {
            return null;
        }

        @Override
        public Object visitOther(Tree node, Object p) {
            return null;
        }

        @Override
        public Object visitYield(YieldTree node, Object p) {
            node.getValue().accept(this, null);
            return null;
        }

        @Override
        public Object visitStringTemplate(StringTemplateTree node, Object o) {
            return null;
        }

        @Override
        public Object visitAnyPattern(AnyPatternTree node, Object o) {
            return null;
        }

        @Override
        public Object visitConstantCaseLabel(ConstantCaseLabelTree node, Object o) {
            return null;
        }

        @Override
        public Object visitPatternCaseLabel(PatternCaseLabelTree node, Object o) {
            return null;
        }

        @Override
        public Object visitDeconstructionPattern(DeconstructionPatternTree node, Object o) {
            return null;
        }
    }
}
