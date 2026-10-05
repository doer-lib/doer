package com.doer.processor;

import com.doer.*;
import com.google.auto.service.AutoService;
import com.sun.source.util.JavacTask;
import com.sun.source.util.TaskEvent;
import com.sun.source.util.TaskListener;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.ExecutableType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic.Kind;
import javax.tools.FileObject;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;

@SupportedAnnotationTypes({ "com.doer.*" })
@AutoService(Processor.class)
public class DoerProcessor extends AbstractProcessor {

    /** Size of the tasks.status column. */
    static final int MAX_STATUS_LENGTH = 50;
    /** Limit of a concurrency domain without {@code @ConcurrencyLimit}. */
    static final int DEFAULT_LIMIT = 2;
    /** Retry interval of a doer method without {@code @RetryPolicy}. */
    static final Duration DEFAULT_RETRY_INTERVAL = Duration.ofMinutes(5);
    /** Retry duration of a doer method without {@code @RetryPolicy}. */
    static final Duration DEFAULT_RETRY_DURATION = Duration.ofDays(1);

    // Filled by process() in the round with doer annotations
    private final List<DoerMethodInfo> doerMethods = new ArrayList<>();
    private final List<TaskDataLoaderInfo> loaders = new ArrayList<>();
    private final List<TaskDataSaverInfo> savers = new ArrayList<>();
    private final List<ExceptionDescriberInfo> describers = new ArrayList<>();
    /** Declared @ConcurrencyLimit of each domain. */
    private final Map<String, Integer> limits = new HashMap<>();
    /** Domain names given in @ConcurrencyGroup. */
    private final Set<String> namedDomains = new HashSet<>();
    /** Created in init(); adds the statuses set in code to doerMethods. */
    private SetStatusFinder setStatusFinder;
    /**
     * Set by process() when it generated _GeneratedDoerService; false when there are no doer annotations or test
     * code is being compiled. The lists above can be empty in both cases, so they cannot tell this.
     */
    private boolean doerAnnotationsProcessed;
    /** Number of ANALYZE task events; 0 when javac did not compile the sources (e.g. -proc:only). */
    private int analyzedClasses;

    /**
     * doer.json and doer.dot need the statuses passed to Task.setStatus, which {@link SetStatusFinder} reads from
     * attributed method bodies. So they are generated after javac has attributed the classes (task events),
     * not during annotation processing: attributing them from the processor, before the generated classes exist,
     * breaks compilation of anonymous classes on javac 17-25. Without attribution (e.g. javac -proc:only) they are
     * generated without these statuses, with a warning.
     */
    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        setStatusFinder = new SetStatusFinder(processingEnv, doerMethods);
        JavacTask.instance(processingEnv).addTaskListener(new TaskListener() {
            @Override
            public void finished(TaskEvent e) {
                onJavacTaskFinished(e);
            }

        });
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        try {
            if (!annotations.isEmpty()) {
                if (isTestFilesAreBeingCompiling()) {
                    return false;
                }
                loadDoerMethods(roundEnv);
                loadLoaders(roundEnv);
                loadSavers(roundEnv);
                loadExceptionDescribers(roundEnv);
                loadConcurrencyDomains(roundEnv);

                generateDoerService();

                generateCreateSchemaSql();
                generateSelectTaskSql();
                generateCreateIndexSql();

                // doer.json and doer.dot are generated when compilation has finished (see init)
                doerAnnotationsProcessed = true;

                return true;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return false;
    }

    private void onJavacTaskFinished(TaskEvent e) {
        if (!doerAnnotationsProcessed) {
            return;
        }
        if (e.getKind() == TaskEvent.Kind.ANALYZE) {
            analyzedClasses++;
            setStatusFinder.scanTopLevelType(e.getTypeElement());
        } else if (e.getKind() == TaskEvent.Kind.COMPILATION) {
            if (analyzedClasses == 0) {
                processingEnv.getMessager().printMessage(Kind.WARNING, "doer.json and doer.dot do not "
                        + "contain the statuses set by Task.setStatus(...) in the code: the sources were not "
                        + "compiled (for example, javac -proc:only), so method bodies could not be analyzed.");
            }
            try {
                generateDoerJson();
                generateDoerDot();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private boolean isTestFilesAreBeingCompiling() {
        Elements elementUtils = processingEnv.getElementUtils();
        TypeElement typeElement = elementUtils.getTypeElement("com.doer.generated._GeneratedDoerService");
        if (typeElement != null) {
            String message = "The class _GeneratedDoerService is already present in dependencies. Looks like " +
                    "DoerProcessor is called during test code compilation, and should not generate any extra code";
            processingEnv.getMessager().printMessage(Kind.NOTE, message, typeElement);
            return true;
        }
        return false;
    }

    private void loadDoerMethods(RoundEnvironment roundEnv) {
        Set<Element> elements = new HashSet<>();
        elements.addAll(roundEnv.getElementsAnnotatedWith(AcceptStatus.class));
        elements.addAll(roundEnv.getElementsAnnotatedWith(AcceptStatuses.class));

        for (Element element : elements) {
            if (element.getKind() != ElementKind.METHOD) {
                processingEnv.getMessager().printMessage(Kind.ERROR,
                        "AcceptStatus annotation can be used only on public methods.", element);
                continue;
            }
            DoerMethodInfo info = new DoerMethodInfo();
            info.className = element.getEnclosingElement().asType().toString();
            checkUnnamedPackageError(element, info.className);
            info.methodName = element.getSimpleName().toString();
            info.parameterTypes = ((ExecutableType) element.asType()).getParameterTypes()
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());
            for (AcceptStatus annotation : element.getAnnotationsByType(AcceptStatus.class)) {
                validateStatus(annotation.value(), "@AcceptStatus value", element);
                if (!"".equals(annotation.delay())
                        && parseAnnotationDuration(annotation.delay(), "@AcceptStatus delay", element) == null) {
                    // Compilation fails anyway; skip it so that code generation does not parse it again
                    continue;
                }
                info.acceptList.add(annotation);
            }
            loadRetryPolicy(info, element);
            info.domainName = resolveDomainName(element);
            info.element = element;
            doerMethods.add(info);
        }
    }

    private void loadRetryPolicy(DoerMethodInfo info, Element element) {
        Messager messager = processingEnv.getMessager();
        RetryPolicy policy = element.getAnnotation(RetryPolicy.class);
        if (policy == null) {
            info.retryInterval = DEFAULT_RETRY_INTERVAL;
            info.retryDuration = DEFAULT_RETRY_DURATION;
            info.fallbackStatus = null;
            messager.printMessage(Kind.WARNING, "Doer method " + info.methodName + " has no @"
                    + RetryPolicy.class.getSimpleName() + ". On exception it is retried every 5 minutes for 1 day, "
                    + "then its status is set to null.\n"
                    + "To retry forever, add @" + RetryPolicy.class.getSimpleName() + "(interval = \"5m\")",
                    element);
            return;
        }
        info.retryIntervalText = policy.interval();
        info.retryInterval = parseAnnotationDuration(policy.interval(), "@RetryPolicy interval", element);
        if (info.retryInterval == null) {
            info.retryInterval = DEFAULT_RETRY_INTERVAL;
        }
        if (!"".equals(policy.duration())) {
            info.retryDurationText = policy.duration();
            info.retryDuration = parseAnnotationDuration(policy.duration(), "@RetryPolicy duration", element);
        }
        if (!"".equals(policy.fallbackStatus())) {
            validateStatus(policy.fallbackStatus(), "@RetryPolicy fallbackStatus", element);
            if ("".equals(policy.duration())) {
                messager.printMessage(Kind.ERROR, "@RetryPolicy fallbackStatus requires duration: without duration "
                        + "the task is retried forever and fallbackStatus is never used.", element);
            }
            info.fallbackStatus = policy.fallbackStatus();
        }
    }

    /** Reports a compile error and returns null when the string is not a duration like "5s" or "10 min". */
    private Duration parseAnnotationDuration(String text, String what, Element element) {
        try {
            return parseDuration(text);
        } catch (IllegalArgumentException e) {
            processingEnv.getMessager().printMessage(Kind.ERROR, what + " \"" + text + "\" is not a duration. "
                    + "Expected a number and a unit, e.g. \"5s\", \"10 min\", \"2h\", \"1 day\".", element);
            return null;
        }
    }

    private void validateStatus(String status, String what, Element element) {
        Messager messager = processingEnv.getMessager();
        if (status.isEmpty()) {
            messager.printMessage(Kind.ERROR, what + " must not be empty.", element);
        } else if (!status.equals(status.strip())) {
            messager.printMessage(Kind.ERROR, what + " \"" + escape(status)
                    + "\" must not start or end with whitespace.", element);
        } else if (status.length() > MAX_STATUS_LENGTH) {
            messager.printMessage(Kind.ERROR, what + " \"" + escape(status) + "\" is " + status.length()
                    + " characters long; the maximum is " + MAX_STATUS_LENGTH + ".", element);
        }
    }

    /**
     * Name of the concurrency domain a doer method (or a class) runs in. The first rule that applies wins:
     * method's {@code @ConcurrencyGroup}, {@code Class.method} when the method has {@code @ConcurrencyLimit},
     * class's {@code @ConcurrencyGroup}, class name.
     */
    private String resolveDomainName(Element element) {
        ConcurrencyGroup group = element.getAnnotation(ConcurrencyGroup.class);
        if (group != null) {
            return group.value();
        }
        String derived = derivedDomainName(element);
        return derived != null ? derived : resolveDomainName(element.getEnclosingElement());
    }

    /**
     * Name of the implicit domain the element runs in, or null when it runs in a named domain
     * ({@code @ConcurrencyGroup} on the method or its class).
     */
    private String derivedDomainName(Element element) {
        if (element.getAnnotation(ConcurrencyGroup.class) != null) {
            return null;
        }
        if (element.getKind() == ElementKind.METHOD) {
            if (element.getAnnotation(ConcurrencyLimit.class) != null) {
                return element.getEnclosingElement().asType().toString() + "." + element.getSimpleName();
            }
            return derivedDomainName(element.getEnclosingElement());
        }
        return element.asType().toString();
    }

    private String describeElement(Element element) {
        if (element.getKind() == ElementKind.METHOD) {
            return element.getEnclosingElement().asType().toString() + "." + element.getSimpleName();
        }
        return element.asType().toString();
    }

    /**
     * Validates {@code @ConcurrencyGroup} and {@code @ConcurrencyLimit}, fills {@code namedDomains} with the
     * names given in {@code @ConcurrencyGroup} and {@code limits} with the declared limit of each domain.
     */
    private void loadConcurrencyDomains(RoundEnvironment roundEnv) {
        Messager messager = processingEnv.getMessager();
        Elements elementUtils = processingEnv.getElementUtils();
        Set<String> usedDomains = new HashSet<>();
        Set<String> implicitDomains = new HashSet<>();
        for (DoerMethodInfo method : doerMethods) {
            usedDomains.add(method.domainName);
            String derived = derivedDomainName(method.element);
            if (derived != null) {
                implicitDomains.add(derived);
            }
        }

        for (Element element : roundEnv.getElementsAnnotatedWith(ConcurrencyGroup.class)) {
            String name = element.getAnnotation(ConcurrencyGroup.class).value();
            if (name.trim().isEmpty()) {
                messager.printMessage(Kind.ERROR, "@ConcurrencyGroup value must not be empty.", element);
                continue;
            }
            namedDomains.add(name);
            if (implicitDomains.contains(name)) {
                continue;
            }
            // A name that equals a class or Class.method name may only join the implicit domain of that
            // class or method, and only when that domain exists.
            TypeElement type = elementUtils.getTypeElement(name);
            String what = null;
            if (type != null) {
                what = "class " + name;
            } else if (name.contains(".")) {
                String className = name.substring(0, name.lastIndexOf('.'));
                String methodName = name.substring(name.lastIndexOf('.') + 1);
                TypeElement owner = elementUtils.getTypeElement(className);
                if (owner != null && owner.getEnclosedElements().stream().anyMatch(e ->
                        e.getKind() == ElementKind.METHOD && e.getSimpleName().contentEquals(methodName))) {
                    what = "method " + name;
                }
            }
            if (what != null) {
                messager.printMessage(Kind.ERROR, "@ConcurrencyGroup(\"" + name + "\") uses the name of " + what
                        + ", but no doer method runs in the implicit concurrency domain of that " + what.split(" ")[0]
                        + " (it is not a doer method or class, or it declares its own @ConcurrencyGroup).\n"
                        + "A class or method name can be used only to join an existing implicit domain. "
                        + "Use the @ConcurrencyGroup name of that " + what.split(" ")[0]
                        + " or a name that is not a class or method name.", element);
            }
        }

        Map<String, List<Element>> limitElements = new TreeMap<>();
        for (Element element : roundEnv.getElementsAnnotatedWith(ConcurrencyLimit.class)) {
            if (element.getAnnotation(ConcurrencyLimit.class).value() < 1) {
                messager.printMessage(Kind.ERROR, "@ConcurrencyLimit value must be at least 1.", element);
                continue;
            }
            limitElements.computeIfAbsent(resolveDomainName(element), k -> new ArrayList<>()).add(element);
        }
        for (Map.Entry<String, List<Element>> entry : limitElements.entrySet()) {
            String domainName = entry.getKey();
            List<Element> elements = entry.getValue();
            elements.sort(Comparator.comparing(this::describeElement));
            Set<Integer> values = elements.stream()
                    .map(e -> e.getAnnotation(ConcurrencyLimit.class).value())
                    .collect(Collectors.toSet());
            if (values.size() > 1) {
                String list = elements.stream()
                        .map(e -> "    " + describeElement(e) + ": " + e.getAnnotation(ConcurrencyLimit.class).value())
                        .collect(Collectors.joining("\n"));
                for (Element element : elements) {
                    messager.printMessage(Kind.ERROR, "Different @ConcurrencyLimit values for concurrency domain \""
                            + domainName + "\":\n" + list, element);
                }
                continue;
            }
            limits.put(domainName, values.iterator().next());
            if (!usedDomains.contains(domainName)) {
                for (Element element : elements) {
                    messager.printMessage(Kind.WARNING, "@ConcurrencyLimit has no effect: no doer method runs in "
                            + "concurrency domain \"" + domainName + "\".", element);
                }
            }
        }
    }

    private void checkUnnamedPackageError(Element element, String className) {
        if (!className.contains(".")) {
            String message = String.format("Class in unnamed package\n" +
                    "%s can not import classes from default package.\n" +
                    "See chapter 7.5 Import Declarations in Java Spec " +
                    "https://docs.oracle.com/javase/specs/jls/se11/html/jls-7.html#jls-7.5\n" +
                    "Please move your class %s to any package, so %s can import it.",
                    DoerService.class.getName(), className, DoerService.class.getName());
            processingEnv.getMessager().printMessage(Kind.ERROR, message, element);
        }
    }

    private void loadLoaders(RoundEnvironment roundEnv) {
        Messager messager = processingEnv.getMessager();
        Set<Element> elements = new HashSet<>();
        elements.addAll(roundEnv.getElementsAnnotatedWith(TaskDataLoader.class));
        for (Element element : elements) {
            if (element.getKind() != ElementKind.METHOD) {
                messager.printMessage(Kind.ERROR,
                        TaskDataLoader.class.getName() + " annotation can be used only on public methods.", element);
                continue;
            }
            ExecutableType executableType = (ExecutableType) element.asType();
            List<? extends TypeMirror> args = executableType.getParameterTypes();
            if (args.size() != 1 || !Task.class.getName().equals(args.get(0).toString())) {
                messager.printMessage(Kind.ERROR, TaskDataLoader.class.getName()
                        + " should have exactly 1 argument of type " +
                        Task.class.getName(), element);
                continue;
            }
            TaskDataLoaderInfo info = new TaskDataLoaderInfo();
            info.className = element.getEnclosingElement().asType().toString();
            checkUnnamedPackageError(element, info.className);
            info.methodName = element.getSimpleName().toString();
            info.type = ((ExecutableType) element.asType()).getReturnType().toString();
            // ((TypeElement)((DeclaredType)args.get(0)).asElement()).getQualifiedName()
            loaders.add(info);
        }
    }

    private void loadSavers(RoundEnvironment roundEnv) {
        Messager messager = processingEnv.getMessager();
        Set<Element> elements = new HashSet<>();
        elements.addAll(roundEnv.getElementsAnnotatedWith(TaskDataSaver.class));
        for (Element element : elements) {
            if (element.getKind() != ElementKind.METHOD) {
                messager.printMessage(Kind.ERROR,
                        TaskDataSaver.class.getName() + " annotation can be used only on public methods.", element);
                continue;
            }
            ExecutableType executableType = (ExecutableType) element.asType();
            List<? extends TypeMirror> args = executableType.getParameterTypes();
            if (args.size() != 2 || !Task.class.getName().equals(args.get(0).toString())
                    || ((ExecutableType) element.asType()).getReturnType().getKind() != TypeKind.VOID) {
                messager.printMessage(Kind.ERROR,
                        TaskDataSaver.class.getName()
                                + " should have exactly 2 arguments: Task and the task data to save, and should return void.",
                        element);
                continue;
            }
            TaskDataSaverInfo info = new TaskDataSaverInfo();
            info.className = element.getEnclosingElement().asType().toString();
            checkUnnamedPackageError(element, info.className);
            info.methodName = element.getSimpleName().toString();
            info.type = args.get(1).toString();
            savers.add(info);
        }
    }

    private void loadExceptionDescribers(RoundEnvironment roundEnv) {
        Messager messager = processingEnv.getMessager();
        Types types = processingEnv.getTypeUtils();
        Set<Element> elements = new HashSet<>();
        elements.addAll(roundEnv.getElementsAnnotatedWith(ExceptionDescriber.class));
        for (Element element : elements) {
            if (element.getKind() != ElementKind.METHOD) {
                messager.printMessage(Kind.ERROR,
                        ExceptionDescriber.class.getName() + " annotation can be used only on public methods.", element);
                continue;
            }
            ExecutableType executableType = (ExecutableType) element.asType();
            List<? extends TypeMirror> args = executableType.getParameterTypes();

            String jsonObjectBuilder = "jakarta.json.JsonObjectBuilder";
            if (args.size() != 3 || !Task.class.getName().equals(args.get(0).toString())
                    || !jsonObjectBuilder.equals(args.get(2).toString())
                    || ((ExecutableType) element.asType()).getReturnType().getKind() != TypeKind.VOID) {
                messager.printMessage(Kind.ERROR,
                        ExceptionDescriber.class.getName()
                                + " should mark void method that have exactly 3 arguments: Task, Exception and JsonObjectBuilder\n"
                                + "Example:\n"
                                + "@" + ExceptionDescriber.class.getName() + "\n"
                                + "public void myDescriber(Task task, Exception e, JsonObjectBuilder builder) {\n"
                                + "}",
                        element);
                continue;
            }
            TypeMirror exType = args.get(1);
            ExceptionDescriberInfo info = new ExceptionDescriberInfo();
            info.className = element.getEnclosingElement().asType().toString();
            checkUnnamedPackageError(element, info.className);
            info.methodName = element.getSimpleName().toString();
            info.type = exType.toString();
            info.typeParents = new LinkedList<>();
            Element exElement = types.asElement(exType);
            if (exElement != null && exElement.getKind() == ElementKind.CLASS) {
                TypeElement te = (TypeElement) exElement;
                info.typeParents.addAll(extractParentClasses(types, te.getSuperclass()));
            } else {
                messager.printMessage(Kind.ERROR, "Second parameter of @" + ExceptionDescriber.class.getSimpleName()
                        + " annotated method " + info.methodName + " should be of Throwable type", element);
            }
            describers.add(info);
        }

        HashSet<String> describerTypes = new HashSet<>();
        for (ExceptionDescriberInfo info : describers) {
            describerTypes.add(info.type);
        }
        for (ExceptionDescriberInfo info : describers) {
            Iterator<String> iterator = info.typeParents.iterator();
            while (iterator.hasNext()) {
                if (!describerTypes.contains(iterator.next())) {
                    iterator.remove();
                }
            }
        }
        // Base classes comes first, then alphabetically ordered by class name
        Collections.sort(describers, (a, b) -> {
            if (a.typeParents.contains(b.type)) {
                return 1;
            } else if (b.typeParents.contains(a.type)) {
                return -1;
            } else {
                return a.type.compareTo(b.type);
            }
        });
    }

    private List<String> extractParentClasses(Types types, TypeMirror typeMirror) {
        ArrayList<String> result = new ArrayList<>();
        result.add(typeMirror.toString());
        Element el = types.asElement(typeMirror);
        if (el != null && el.getKind() == ElementKind.CLASS) {
            result.addAll(extractParentClasses(types, ((TypeElement) el).getSuperclass()));
        }
        return result;
    }

    void generateDoerService() throws IOException {
        HashMap<String, String> shortcuts = createTypeShortcuts();
        HashMap<String, String> fieldNames = createFieldNames();

        Stream<String> classes1 = doerMethods.stream().map(s -> s.className);
        Stream<String> classes2 = loaders.stream().map(s -> s.className);
        Stream<String> classes3 = savers.stream().map(s -> s.className);
        Stream<String> classes4 = describers.stream().map(s -> s.className);
        List<String> beans = Stream.of(classes1, classes2, classes3, classes4).flatMap(i -> i)
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        Set<String> missingLoadersReported = new HashSet<>();

        JavaFileObject builderFile = processingEnv.getFiler()
                .createSourceFile("com.doer.generated._GeneratedDoerService");
        try (PrintWriter out = new PrintWriter(builderFile.openWriter())) {
            out.println("package com.doer.generated;");
            List<String> names = new ArrayList<>(shortcuts.keySet());
            Collections.sort(names);
            for (String fullName : names) {
                if (!fullName.equals(shortcuts.get(fullName))) {
                    out.println("import " + fullName + ";");
                }
            }
            out.println();
            out.println("@ApplicationScoped");
            out.println("@Generated(value = \"" + getClass().getName() + "\", date = \"" + LocalDate.now() + "\")");
            out.println("public class _GeneratedDoerService extends DoerService {");
            out.println();
            for (String bean : beans) {
                out.println("    " + shortcuts.get(bean) + " " + fieldNames.get(bean) + ";");
            }
            out.println();
            out.println("    public _GeneratedDoerService() {");
            out.println("        super();");
            out.println("        initializeDomains();");
            out.println("    }");
            out.println();
            out.println("    @Override");
            out.println("    @Inject");
            out.println("    public void setSelfReference(DoerService self) {");
            out.println("        super.setSelfReference(self);");
            out.println("    }");
            out.println();
            out.println("    @Override");
            out.println("    @Inject");
            out.println("    public void setDataSource(DataSource dataSource) {");
            out.println("        super.setDataSource(dataSource);");
            out.println("    }");
            out.println();
            out.println("    @Override");
            out.println("    @Inject");
            out.println("    public void setExecutor(Executor executor) {");
            out.println("        super.setExecutor(executor);");
            out.println("    }");
            out.println();
            for (String bean : beans) {
                out.println("    @Inject");
                out.println("    public void _inject_" + fieldNames.get(bean) +
                        "(" + shortcuts.get(bean) + " value) {");
                out.println("        this." + fieldNames.get(bean) + " = value;");
                out.println("    }");
                out.println();
            }
            out.println("    @Override");
            out.println("    @Transactional(value = Transactional.TxType.REQUIRES_NEW, rollbackOn = Exception.class)");
            out.println("    public void runInTransaction(Callable<Object> code) throws Exception {");
            out.println("        code.call();");
            out.println("    }");
            out.println();
            out.println("    @Override");
            out.println("    @Transactional(Transactional.TxType.REQUIRED)");
            out.println("    public int resetStalledInProgressTasks(Duration timeout) throws SQLException {");
            out.println("        return super.resetStalledInProgressTasks(timeout);");
            out.println("    }");
            out.println();
            out.println("    @Override");
            out.println("    @Transactional(Transactional.TxType.NOT_SUPPORTED)");
            out.println("    public void reloadTasksFromDb() {");
            out.println("        super.reloadTasksFromDb();");
            out.println("    }");
            out.println();
            out.println("    @Override");
            out.println("    @Transactional(Transactional.TxType.REQUIRED)");
            out.println(
                    "    public List<Task> loadTasksFromDatabase(List<Integer> limits) throws SQLException, IOException {");
            out.println("        return super.loadTasksFromDatabase(limits);");
            out.println("    }");
            out.println();
            out.println("    @Override");
            out.println("    public String createExtraJson(Task task, Exception exception) {");
            out.println("        JsonObjectBuilder builder = Json.createObjectBuilder();");
            out.println("        try {");
            out.println("            fillExtraJson(task, exception, builder);");
            out.println("        } catch (Exception e) {");
            out.println("            LOG.log(java.util.logging.Level.WARNING, \"ExtraJson creation error\", e);");
            out.println("        }");
            out.println("        JsonObject jsonObject = builder.build();");
            out.println("        if (jsonObject.isEmpty()) {");
            out.println("            return null;");
            out.println("        }");
            out.println("        HashMap<String, Object> config = new HashMap<>();");
            out.println("        if (jsonObject.size() > 1) {");
            out.println("            config.put(JsonGenerator.PRETTY_PRINTING, true);");
            out.println("        }");
            out.println("        JsonWriterFactory factory = Json.createWriterFactory(config);");
            out.println("        StringWriter sw = new StringWriter();");
            out.println("        try (JsonWriter writer = factory.createWriter(sw)) {");
            out.println("            writer.writeObject(jsonObject);");
            out.println("        }");
            out.println("        return sw.toString();");
            out.println("    }");
            out.println();
            out.println("    private void fillExtraJson(Task task, Throwable exception, JsonObjectBuilder builder) throws Exception {");
            out.println("        if (exception == null) {");
            out.println("            return;");
            out.println("        }");
            out.println();
            out.println("        if (exception.getMessage() != null && !\"\".equals(exception.getMessage().trim())) {");
            out.println("            builder.add(\"message\", limitTo1024(exception.getMessage().trim()));");
            out.println("        }");
            for (ExceptionDescriberInfo describer : describers) {
                String exceptionType = shortcuts.get(describer.type);
                String fieldName = fieldNames.get(describer.className);
                out.println("        if (exception instanceof " + exceptionType + ") {");
                out.println("            " + fieldName + "." + describer.methodName + "(task, (" + exceptionType
                        + ") exception, builder);");
                out.println("        }");
            }
            out.println();
            out.println("        JsonObjectBuilder causeBuilder = Json.createObjectBuilder();");
            out.println("        fillExtraJson(task, exception.getCause(), causeBuilder);");
            out.println("        JsonObject causeExtraJson = causeBuilder.build();");
            out.println("        if (!causeExtraJson.isEmpty()) {");
            out.println("            builder.add(\"cause\", causeExtraJson);");
            out.println("        }");
            out.println("        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();");
            out.println("        for (Throwable throwable : exception.getSuppressed()) {");
            out.println("            JsonObjectBuilder supperssedBuilder = Json.createObjectBuilder();");
            out.println("            fillExtraJson(task, throwable, supperssedBuilder);");
            out.println("            JsonObject jsonObject = supperssedBuilder.build();");
            out.println("            if (!jsonObject.isEmpty()) {");
            out.println("                arrayBuilder.add(jsonObject);");
            out.println("            }");
            out.println("        }");
            out.println("        JsonArray array = arrayBuilder.build();");
            out.println("        if (!array.isEmpty()) {");
            out.println("            builder.add(\"suppressed\", array);");
            out.println("        }");
            out.println("    }");
            out.println();
            out.println("    @Override");
            out.println("    @Transactional(Transactional.TxType.NEVER)");
            out.println("    public Task facilitateCoordinatedUpdate(long taskId, Duration waitDuration, boolean allowHijacking,");
            out.println("            TaskUpdater updater) throws Exception {");
            out.println("        return super.facilitateCoordinatedUpdate(taskId, waitDuration, allowHijacking, updater);");
            out.println("    }");
            out.println();
            out.println("    @Override");
            out.println("    @Transactional(Transactional.TxType.NEVER)");
            out.println("    public <T> Task facilitateCoordinatedUpdate(long taskId, Duration waitDuration, boolean allowHijacking,");
            out.println("            Class<T> dataType, TaskAndDataUpdater<T> updater) throws Exception {");
            out.println("        return super.facilitateCoordinatedUpdate(taskId, waitDuration, allowHijacking, dataType, updater);");
            out.println("    }");
            out.println();
            out.println("    @Override");
            out.println("    protected Object _load(Task task, Class<?> type) throws Exception {");
            Set<String> generatedTypes = new HashSet<>();
            for (TaskDataLoaderInfo loader : loaders) {
                if (!isPlainClassType(loader.type) || !generatedTypes.add(loader.type)) {
                    continue;
                }
                out.println("        if (" + shortcuts.get(loader.type) + ".class.equals(type)) {");
                out.println("            return " + fieldNames.get(loader.className) + "." + loader.methodName + "(task);");
                out.println("        }");
            }
            out.println("        throw new IllegalArgumentException(\"No @TaskDataLoader for \" + type.getName());");
            out.println("    }");
            out.println();
            out.println("    @Override");
            out.println("    protected void _save(Task task, Class<?> type, Object data) throws Exception {");
            generatedTypes.clear();
            for (TaskDataSaverInfo saver : savers) {
                if (!isPlainClassType(saver.type) || !generatedTypes.add(saver.type)) {
                    continue;
                }
                String typeName = shortcuts.get(saver.type);
                out.println("        if (" + typeName + ".class.equals(type)) {");
                out.println("            " + fieldNames.get(saver.className) + "." + saver.methodName
                        + "(task, (" + typeName + ") data);");
                out.println("            return;");
                out.println("        }");
            }
            out.println("    }");
            out.println();
            out.println("    @Override");
            out.println("    @Transactional(Transactional.TxType.NOT_SUPPORTED)");
            out.println("    public void runTask(Task task) throws Exception {");
            int maxNumberOfParam = doerMethods.stream()
                    .mapToInt(s -> s.parameterTypes.size())
                    .max()
                    .orElse(0);
            out.println("        Object[] args = new Object[" + maxNumberOfParam + "];");
            out.println("        String status = task.getStatus();");
            List<DoerMethodInfo> sortedDoerMethods = new ArrayList<>(doerMethods);
            Comparator<DoerMethodInfo> methodsComparator = Comparator.comparing(DoerMethodInfo::getDomainName)
                    .thenComparing(i -> i.methodName)
                    .thenComparing(i -> i.parameterTypes.toString());
            Collections.sort(sortedDoerMethods, methodsComparator);
            for (int i = 0; i < sortedDoerMethods.size(); i++) {
                DoerMethodInfo info = sortedDoerMethods.get(i);
                if (i == 0) {
                    out.print("       ");
                } else {
                    out.print(" else");
                }

                String firstStatus = info.acceptList.get(0).value();
                out.print(" if (\"" + escape(firstStatus) + "\".equals(status)");
                for (int extraStatusIndex = 1; extraStatusIndex < info.acceptList.size(); extraStatusIndex++) {
                    String extraStatus = info.acceptList.get(extraStatusIndex).value();
                    out.println(" ||");
                    out.print("                \"" + escape(extraStatus) + "\".equals(status)");
                }
                out.println(") {");
                String beanName = fieldNames.get(info.className);
                String shortClassName = info.className.replaceAll(".*\\.", "");

                String retryDurationLiteral = createDurationLiteral(info.retryDuration);
                String fallbackStatusLiteral = (info.fallbackStatus == null ? "null"
                        : "\"" + escape(info.fallbackStatus) + "\"");
                out.println("            callDoerMethod(task, () -> {");
                for (int paramIndex = 0; paramIndex < info.parameterTypes.size(); paramIndex++) {
                    String paramClass = info.parameterTypes.get(paramIndex);
                    if (!paramClass.equals(Task.class.getName())) {
                        TaskDataLoaderInfo loader = loaders.stream()
                                .filter(l -> l.type.equals(paramClass))
                                .findFirst()
                                .orElse(null);
                        if (loader == null) {
                            if (!missingLoadersReported.contains(paramClass)) {
                                missingLoadersReported.add(paramClass);
                                Messager messager = processingEnv.getMessager();
                                messager.printMessage(Kind.ERROR,
                                        "No @" + TaskDataLoader.class.getSimpleName() + " found for argument " + paramIndex
                                                + "\n" +
                                                "Please declare loader method:\n" +
                                                "@" + TaskDataLoader.class.getName() + "\n" +
                                                "public " + paramClass + " method(" + Task.class.getName()
                                                + " task) {}\n",
                                        info.element);
                            }
                            out.println("                    args[" + paramIndex + "] = " + null + ";");
                        } else {
                            String loaderBean = fieldNames.get(loader.className);
                            out.println("                    args[" + paramIndex + "] = " + loaderBean + "."
                                    + loader.methodName + "(task);");
                        }
                    }
                }
                out.println("                    return null;");
                out.println("                }, () -> {");
                List<String> argumentCodes = new ArrayList<>();
                for (int paramIndex = 0; paramIndex < info.parameterTypes.size(); paramIndex++) {
                    String paramClass = info.parameterTypes.get(paramIndex);
                    if (Task.class.getName().equals(paramClass)) {
                        argumentCodes.add("task");
                    } else {
                        argumentCodes.add("(" + shortcuts.get(paramClass) + ")args[" + paramIndex + "]");
                    }
                }
                out.println("                    " + beanName + "." + info.methodName + "("
                        + String.join(", ", argumentCodes) + ");");
                out.println("                    return null;");
                out.println("                }, () -> {");

                for (int paramIndex = info.parameterTypes.size() - 1; paramIndex >= 0; paramIndex--) {
                    String paramClass = info.parameterTypes.get(paramIndex);
                    if (!paramClass.equals(Task.class.getName())) {
                        TaskDataSaverInfo saver = savers.stream()
                                .filter(l -> l.type.equals(paramClass))
                                .findFirst()
                                .orElse(null);
                        if (saver != null) {
                            String saverBean = fieldNames.get(saver.className);
                            out.println("                    " + saverBean + "." + saver.methodName +
                                    "(task, (" + shortcuts.get(paramClass) + ")args[" + paramIndex + "]);");
                        }
                    }
                }

                out.println("                    return null;");
                out.println("                }, \"" + shortClassName + "\", \"" + info.methodName + "\",");
                out.println("                    " + retryDurationLiteral + ", " +
                        fallbackStatusLiteral + ");");
                out.print("        }");
            }
            out.println();
            out.println("    }");
            out.println();
            out.println("    protected void initializeDomains() {");
            Map<String, List<DoerMethodInfo>> domains = groupMethodsByDomain(doerMethods);
            List<String> domainNames = new ArrayList<>(domains.keySet());
            Collections.sort(domainNames);
            for (String domainName : domainNames) {
                Map<Duration, List<String>> byDelay = groupStatusesByDelay(domains.get(domainName));
                List<Duration> delays = new ArrayList<>(byDelay.keySet());
                Collections.sort(delays);

                Map<Duration, List<String>> byRetryDelay = groupStatusesByRetryDelay(domains.get(domainName));
                byRetryDelay.remove(DEFAULT_RETRY_INTERVAL);
                List<Duration> retryDelays = new ArrayList<>(byRetryDelay.keySet());
                Collections.sort(retryDelays);

                out.println("        {");
                out.println("            HashMap<String, Duration> delays = new HashMap<>();");
                out.println("            HashMap<String, Duration> retryDelays = new HashMap<>();");
                for (Duration delay : delays) {
                    List<String> statuses = new ArrayList<>(byDelay.get(delay));
                    Collections.sort(statuses);
                    for (String status : statuses) {
                        out.println("            delays.put(\"" + escape(status) + "\", " + createDurationLiteral(delay)
                                + ");");
                    }
                }
                for (Duration retryDelay : retryDelays) {
                    List<String> statuses = new ArrayList<>(byRetryDelay.get(retryDelay));
                    Collections.sort(statuses);
                    for (String status : statuses) {
                        out.println("            retryDelays.put(\"" + escape(status) + "\", "
                                + createDurationLiteral(retryDelay) + ");");
                    }
                }
                out.println("            setupConcurrencyDomain(\"" + domainName + "\", "
                        + limits.getOrDefault(domainName, DEFAULT_LIMIT)
                        + ", delays, retryDelays);");
                out.println("        }");
            }
            out.println("    }");
            out.println();
            out.println("}");
        }
    }

    void generateSelectTaskSql() throws IOException {
        Map<String, List<DoerMethodInfo>> domains = groupMethodsByDomain(doerMethods);

        FileObject selectTaskSql = processingEnv.getFiler()
                .createResource(StandardLocation.CLASS_OUTPUT, "com.doer.generated", "SelectTasks.sql");
        try (PrintWriter out = new PrintWriter(selectTaskSql.openWriter())) {
            boolean firstBlock = true;
            List<String> domainNames = new ArrayList<>(domains.keySet());
            Collections.sort(domainNames);
            for (String domainName : domainNames) {
                List<String> asapStatuses = new ArrayList<>();
                Map<Duration, List<String>> delayedStatuses = new LinkedHashMap<>();
                for (DoerMethodInfo method : domains.get(domainName)) {
                    for (AcceptStatus annotation : method.acceptList) {
                        if ("".equals(annotation.delay())) {
                            asapStatuses.add(annotation.value());
                        } else {
                            Duration duration = parseDuration(annotation.delay());
                            if (!delayedStatuses.containsKey(duration)) {
                                delayedStatuses.put(duration, new ArrayList<>());
                            }
                            delayedStatuses.get(duration).add(annotation.value());
                        }
                    }
                }
                Map<Duration, List<String>> retryingStatuses = new LinkedHashMap<>();
                for (DoerMethodInfo method : domains.get(domainName)) {
                    Duration retryDuration = method.retryInterval;
                    if (!retryingStatuses.containsKey(retryDuration)) {
                        retryingStatuses.put(retryDuration, new ArrayList<>());
                    }
                    for (AcceptStatus annotation : method.acceptList) {
                        retryingStatuses.get(retryDuration).add(annotation.value());
                    }
                }
                Collections.sort(asapStatuses);
                if (asapStatuses.size() > 0 || delayedStatuses.size() > 0) {
                    if (!firstBlock) {
                        out.println("UNION ALL");
                    }
                    firstBlock = false;

                    if (asapStatuses.size() > 0) {
                        out.println(
                                "(SELECT * FROM tasks WHERE NOT in_progress AND failing_since IS NULL AND status IN (");
                        out.println(createSqlValues(asapStatuses));
                        out.println(") ORDER BY created LIMIT ?)");
                        out.println("UNION ALL");
                    }
                    List<Duration> delays = new ArrayList<>(delayedStatuses.keySet());
                    Collections.sort(delays);
                    for (Duration delay : delays) {
                        List<String> delayed = delayedStatuses.get(delay);
                        Collections.sort(delayed);
                        out.println(
                                "(SELECT * FROM tasks WHERE NOT in_progress AND failing_since IS NULL AND status IN (");
                        out.println(createSqlValues(delayed));
                        out.println(") ORDER BY modified LIMIT ?)");
                        out.println("UNION ALL");
                    }
                    List<Duration> intervals = new ArrayList<>(retryingStatuses.keySet());
                    Collections.sort(intervals);
                    for (int i = 0; i < intervals.size(); i++) {
                        Duration interval = intervals.get(i);
                        List<String> retrying = retryingStatuses.get(interval);
                        Collections.sort(retrying);
                        out.println(
                                "(SELECT * FROM tasks WHERE NOT in_progress AND failing_since IS NOT NULL AND status IN (");
                        out.println(createSqlValues(retrying));
                        out.println(") ORDER BY modified LIMIT ?)");
                        if (i < intervals.size() - 1) {
                            out.println("UNION ALL");
                        }
                    }
                }
                out.println();
            }
            if (domainNames.size() > 0) {
                out.println("UNION ALL");
            }
            out.println("(SELECT * FROM tasks WHERE in_progress)");
            out.println();
        }
    }

    private Map<String, List<DoerMethodInfo>> groupMethodsByDomain(List<DoerMethodInfo> methods) {
        Map<String, List<DoerMethodInfo>> domains = new HashMap<>();
        for (DoerMethodInfo method : methods) {
            // Methods added by SetStatusFinder are not doer methods and have no domain; group them by class
            String domainName = (method.domainName != null ? method.domainName : method.className);
            domains.computeIfAbsent(domainName, k -> new ArrayList<>()).add(method);
        }
        return domains;
    }

    private Map<Duration, List<String>> groupStatusesByDelay(List<DoerMethodInfo> methods) {
        Map<Duration, List<String>> result = new HashMap<>();
        for (DoerMethodInfo method : methods) {
            for (AcceptStatus annotation : method.acceptList) {
                Duration duration;
                if ("".equals(annotation.delay())) {
                    duration = Duration.ZERO;
                } else {
                    duration = parseDuration(annotation.delay());
                }
                if (!result.containsKey(duration)) {
                    result.put(duration, new ArrayList<>());
                }
                result.get(duration).add(annotation.value());
            }
        }
        return result;
    }

    private Map<Duration, List<String>> groupStatusesByRetryDelay(List<DoerMethodInfo> methods) {
        Map<Duration, List<String>> result = new HashMap<>();
        for (DoerMethodInfo method : methods) {
            Duration retryDuration = method.retryInterval;
            if (!result.containsKey(retryDuration)) {
                result.put(retryDuration, new ArrayList<>());
            }
            for (AcceptStatus annotation : method.acceptList) {
                result.get(retryDuration).add(annotation.value());
            }
        }
        return result;
    }

    private void generateCreateIndexSql() throws IOException {
        FileObject selectTaskSql = processingEnv.getFiler()
                .createResource(StandardLocation.CLASS_OUTPUT, "com.doer.generated", "CreateIndexes.sql");

        List<String> delayedStatues = new ArrayList<>();
        for (DoerMethodInfo method : doerMethods) {
            for (AcceptStatus s : method.acceptList) {
                if (!"".equals(s.delay())) {
                    delayedStatues.add(s.value());
                }
            }
        }

        try (PrintWriter out = new PrintWriter(selectTaskSql.openWriter())) {
            out.println("CREATE INDEX IF NOT EXISTS tasks_status_idx ON tasks (status, created);");
            out.println(
                    "CREATE INDEX IF NOT EXISTS tasks_failing_idx ON tasks (status, modified) WHERE failing_since IS NOT NULL;");
            out.println(
                    "CREATE INDEX IF NOT EXISTS tasks_in_progress_idx ON tasks (status) WHERE in_progress;");

            if (!delayedStatues.isEmpty()) {
                out.println(
                        "CREATE INDEX IF NOT EXISTS tasks_delayed_idx ON tasks (status, modified) WHERE status IN (");
                out.println(createSqlValues(delayedStatues));
                out.println(");");
            }
            out.println();
        }
    }

    private void generateCreateSchemaSql() throws IOException {
        FileObject createSchemaSql = processingEnv.getFiler()
                .createResource(StandardLocation.CLASS_OUTPUT, "com.doer.generated", "CreateSchema.sql");

        try (PrintWriter out = new PrintWriter(createSchemaSql.openWriter())) {
            out.println();
            out.println("CREATE SEQUENCE id_generator START WITH 1000 INCREMENT BY 1;");
            out.println();
            out.println("CREATE TABLE tasks (");
            out.println("    id BIGINT DEFAULT nextval('id_generator'::regclass) PRIMARY KEY,");
            out.println("    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),");
            out.println("    modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),");
            out.println("    status VARCHAR(" + MAX_STATUS_LENGTH + "),");
            out.println("    in_progress BOOLEAN NOT NULL DEFAULT FALSE,");
            out.println("    failing_since TIMESTAMP WITH TIME ZONE,");
            out.println("    version INTEGER NOT NULL default 0");
            out.println(");");
            out.println();
            out.println("CREATE TABLE task_logs (");
            out.println("    id BIGINT DEFAULT nextval('id_generator'::regclass) PRIMARY KEY,");
            out.println("    task_id BIGINT NOT NULL,");
            out.println("    created TIMESTAMP WITH TIME ZONE DEFAULT now(),");
            out.println("    initial_status VARCHAR,");
            out.println("    final_status VARCHAR,");
            out.println("    class_name VARCHAR,");
            out.println("    method_name VARCHAR,");
            out.println("    duration_ms BIGINT,");
            out.println("    exception_type VARCHAR,");
            out.println("    extra_json JSON");
            out.println(");");
            out.println();
        }
    }

    private void generateDoerJson() throws IOException {
        FileObject doerJson = processingEnv.getFiler()
                .createResource(StandardLocation.CLASS_OUTPUT, "com.doer.generated", "doer.json");
        try (PrintWriter out = new PrintWriter(doerJson.openWriter())) {
            out.println("{");
            out.println("    \"generator\": \"" + getClass().getName() + "\",");
            out.println("    \"generated\": \"" + Instant.now() + "\",");
            out.println("    \"domains\": [");
            // Same domains as the setupConcurrencyDomain calls in the generated service
            List<String> domainNames = doerMethods.stream()
                    .filter(m -> !m.acceptList.isEmpty())
                    .map(m -> m.domainName)
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());
            for (String name : domainNames) {
                boolean last = name.equals(domainNames.get(domainNames.size() - 1));
                out.printf("        {%s: %s, %s: %s, %s: %s}%s%n",
                        jstr("name"), jstr(name),
                        jstr("limit"), limits.getOrDefault(name, DEFAULT_LIMIT),
                        jstr("implicit"), !namedDomains.contains(name),
                        (last ? "" : ","));
            }
            out.println("    ],");
            out.println("    \"doer_methods\": [");
            List<DoerMethodInfo> sortedMethods = new ArrayList<>(doerMethods);
            Collections.sort(sortedMethods, Comparator
                    .comparing((DoerMethodInfo m) -> m.className)
                    .thenComparing(m -> m.methodName)
                    .thenComparing(m -> m.parameterTypes.toString()));
            for (DoerMethodInfo method : sortedMethods) {
                boolean last = (sortedMethods.get(sortedMethods.size() - 1) == method);
                out.println("        {");
                if (method.domainName != null) {
                    out.printf("            %s: %s,%n", jstr("domain"), jstr(method.domainName));
                }
                out.printf("            %s: %s, %s: %s,%n", jstr("class"), jstr(method.className), jstr("method"),
                        jstr(method.methodName));
                if (method.hasRetryPolicy()) {
                    out.printf("            %s: %s,", jstr("interval"), jstr(method.retryIntervalText));
                    if (method.retryDurationText != null) {
                        out.printf(" %s: %s,", jstr("duration"), jstr(method.retryDurationText));
                    }
                    if (method.fallbackStatus != null) {
                        out.printf(" %s: %s,", jstr("fallback_status"), jstr(method.fallbackStatus));
                    }
                    out.println();
                }
                out.print("            \"args\": [");
                for (int i = 0; i < method.parameterTypes.size(); i++) {
                    out.print(jstr(method.parameterTypes.get(i)));
                    if (i < method.parameterTypes.size() - 1) {
                        out.print(", ");
                    }
                }
                out.println("],");
                out.println("            \"accepts\": [");
                List<String> statuses = new ArrayList<>();
                Map<String, String> delays = new HashMap<>();
                for (AcceptStatus s : method.acceptList) {
                    statuses.add(s.value());
                    if (!"".equals(s.delay())) {
                        delays.put(s.value(), s.delay());
                    }
                }
                Collections.sort(statuses);
                for (int i = 0; i < statuses.size(); i++) {
                    String status = statuses.get(i);
                    out.printf("                {%s: %s", jstr("status"), jstr(status));
                    if (delays.containsKey(status)) {
                        out.printf(", %s: %s}", jstr("delay"), jstr(delays.get(status)));
                    } else {
                        out.print("}");
                    }
                    if (i >= statuses.size() - 1) {
                        out.println();
                    } else {
                        out.println(",");
                    }
                }
                out.println("            ],");
                out.println("            \"emits\": [");
                List<String> emits = new ArrayList<>(new HashSet<>(method.emitList));
                Iterator<String> iterator = emits.iterator();
                boolean emitsNull = false;
                while (iterator.hasNext()) {
                    if (iterator.next() == null) {
                        emitsNull = true;
                        iterator.remove();
                    }
                }
                Collections.sort(emits);
                for (String status : emits) {
                    boolean lastStatus = status.equals(emits.get(emits.size() - 1));
                    out.printf("                %s", jstr(status));
                    if (!lastStatus) {
                        out.print(",");
                    }
                    out.println();
                }
                if (emitsNull) {
                    out.println("            ],");
                    out.println("            \"emits_null\": true");
                } else {
                    out.println("            ]");
                }
                out.println("        }" + (last ? "" : ","));
            }
            out.println("    ],");
            out.println("    \"loaders\": [");
            List<String> loaderTypes = new ArrayList<>();
            HashMap<String, TaskDataLoaderInfo> loaderMap = new HashMap<>();
            for (TaskDataLoaderInfo info : loaders) {
                loaderTypes.add(info.type);
                loaderMap.put(info.type, info);
            }
            Collections.sort(loaderTypes);
            for (int i = 0; i < loaderTypes.size(); i++) {
                TaskDataLoaderInfo loader = loaderMap.get(loaderTypes.get(i));
                out.printf("        {%s: %s, %s: %s, %s: %s}",
                        jstr("type"), jstr(loader.type),
                        jstr("class"), jstr(loader.className),
                        jstr("method"), jstr(loader.methodName));
                if (i >= loaderTypes.size() - 1) {
                    out.println();
                } else {
                    out.println(",");
                }
            }
            out.println("    ],");
            out.println("    \"savers\": [");

            List<String> saverTypes = new ArrayList<>();
            HashMap<String, TaskDataSaverInfo> saverMap = new HashMap<>();
            for (TaskDataSaverInfo info : savers) {
                saverTypes.add(info.type);
                saverMap.put(info.type, info);
            }
            Collections.sort(saverTypes);
            for (int i = 0; i < saverTypes.size(); i++) {
                TaskDataSaverInfo saver = saverMap.get(saverTypes.get(i));
                out.printf("        {%s: %s, %s: %s, %s: %s}",
                        jstr("type"), jstr(saver.type),
                        jstr("class"), jstr(saver.className),
                        jstr("method"), jstr(saver.methodName));
                if (i >= saverTypes.size() - 1) {
                    out.println();
                } else {
                    out.println(",");
                }
            }
            out.println("    ],");
            out.println("    \"exception_describers\": [");
            for (int i = 0; i < describers.size(); i++) {
                ExceptionDescriberInfo describer = describers.get(i);
                out.printf("        {%s: %s, %s: %s, %s: %s}",
                        jstr("type"), jstr(describer.type),
                        jstr("class"), jstr(describer.className),
                        jstr("method"), jstr(describer.methodName));
                if (i >= describers.size() - 1) {
                    out.println();
                } else {
                    out.println(",");
                }
            }
            out.println("    ]");
            out.println("}");
        }
    }

    private void generateDoerDot() throws IOException {
        String[][] colors = {
                // color, fillcolor, fontcolor
                {"#343A40", "#E9ECEF", "#212529"},
                {"#28A745", "#D4EDDA", "#155724"},
                {"#007BFF", "#D1ECF1", "#0C5460"},
                {"#6F42C1", "#E9D8FD", "#4A148C"},
                {"#FF5733", "#FFC300", "#000000"},
                {"#FD7E14", "#FFF3CD", "#856404"},
                {"#DC3545", "#F8D7DA", "#721C24"},
                {"#17A2B8", "#E3F7FA", "#084C61"},
                {"#20C997", "#D8F3E8", "#116D5E"},
                {"#FFC107", "#FFF9DB", "#856404"},
        };
        FileObject doerJson = processingEnv.getFiler()
                .createResource(StandardLocation.CLASS_OUTPUT, "com.doer.generated", "doer.dot");
        try (PrintWriter out = new PrintWriter(doerJson.openWriter())) {
            out.println("digraph alg {");
            out.println("    rankdir=TD;");
            out.println("    graph [overlap=true];");
            out.println("    node [");
            out.println("        fontname=Helvetica,");
            out.println("        fontsize=10,");
            out.println("        shape=box,");
            out.println("        style=filled,");
            out.println("        margin=\"0.1,0.1\",");
            out.println("        height=0.3");
            out.println("    ];");

            AtomicInteger methodNodeIndexer = new AtomicInteger(100);
            HashMap<DoerMethodInfo, String> methodNodeNames = new HashMap<>();
            AtomicInteger statusNodeIndexer = new AtomicInteger(500);
            HashMap<String, String> statusNodeNames = new HashMap<>();
            HashMap<DoerMethodInfo, String> terminationStatusNodeNames = new HashMap<>();

            List<DoerMethodInfo> sortedDoerMethods = new ArrayList<>(doerMethods);
            sortedDoerMethods.sort(Comparator
                    .comparing((DoerMethodInfo m) -> m.className)
                    .thenComparing(m -> m.methodName)
                    .thenComparing(m -> m.parameterTypes.toString()));

            Map<String, List<DoerMethodInfo>> domainMethods = groupMethodsByDomain(sortedDoerMethods);
            List<String> sortedDomainNames = new ArrayList<>(domainMethods.keySet());
            Collections.sort(sortedDomainNames);
            for (int nameIndex = 0; nameIndex < sortedDomainNames.size(); nameIndex++) {
                out.println();
                String[] colorScheme = colors[nameIndex % colors.length];
                String borderColor = colorScheme[0];
                String fillColor = colorScheme[1];
                String fontColor = colorScheme[2];
                out.printf("node [color=\"%s\", fillcolor=\"%s\", fontcolor=\"%s\"];%n",
                        borderColor, fillColor, fontColor);

                String domainName = sortedDomainNames.get(nameIndex);
                for (DoerMethodInfo method : domainMethods.get(domainName)) {
                    String fullName = method.className + "." + method.methodName;
                    String nodeName = methodNodeNames.computeIfAbsent(method,
                            key -> "m" + methodNodeIndexer.incrementAndGet());
                    out.printf("%s [label=\"%s\", tooltip=\"%s\"];%n",
                            nodeName, method.methodName, fullName);
                }
            }

            out.println();
            out.println("node [shape=circle,fixedsize=true,color=\"black\",fillcolor=\"white\"];");

            Set<String> accepted = new HashSet<>();
            Set<String> emitted = new HashSet<>();
            Set<String> onerror = new HashSet<>();
            for (DoerMethodInfo method : sortedDoerMethods) {
                emitted.addAll(method.emitList);
                for (AcceptStatus acceptStatus : method.acceptList) {
                    accepted.add(acceptStatus.value());
                }
                if (hasFallbackEdge(method) && method.fallbackStatus != null) {
                    onerror.add(method.fallbackStatus);
                }
            }
            emitted.remove(null);
            Set<String> allStatuses = new HashSet<>();
            allStatuses.addAll(accepted);
            allStatuses.addAll(emitted);
            allStatuses.addAll(onerror);
            List<String> sortedStatuses = new ArrayList<>(allStatuses);
            Comparator<String> comparator = Comparator.<String, Integer>comparing(s -> {
                if (accepted.contains(s) && !emitted.contains(s) && !onerror.contains(s)) {
                    return 1;
                } else if (accepted.contains(s) && emitted.contains(s) && !onerror.contains(s)) {
                    return 2;
                } else if (accepted.contains(s) && emitted.contains(s) && onerror.contains(s)) {
                    return 3;
                } else if (accepted.contains(s) && !emitted.contains(s) && onerror.contains(s)) {
                    return 4;
                } else if (!accepted.contains(s) && emitted.contains(s) && !onerror.contains(s)) {
                    return 5;
                } else if (!accepted.contains(s) && emitted.contains(s) && onerror.contains(s)) {
                    return 6;
                } else if (!accepted.contains(s) && !emitted.contains(s) && onerror.contains(s)) {
                    return 7;
                } else {
                    return 8;
                }
            }).thenComparing(s -> s);
            sortedStatuses.sort(comparator);
            for (String status : sortedStatuses) {
                String nodeName = statusNodeNames.computeIfAbsent(status,
                        key -> "s" + statusNodeIndexer.incrementAndGet());
                out.printf("%s [label=\" \", tooltip=\"%s\"];%n", nodeName, escape(status));
            }

            for (DoerMethodInfo method : sortedDoerMethods) {
                if (method.emitList.contains(null) || (hasFallbackEdge(method) && method.fallbackStatus == null)) {
                    String nodeName = terminationStatusNodeNames.computeIfAbsent(method,
                            key -> "n" + statusNodeIndexer.incrementAndGet());
                    out.printf("%s [label=\"❌\", shape=none, fillcolor=\"none\", fontcolor=\"red\", fontsize=20, tooltip=\"null\"];%n", nodeName);
                }
            }

            Set<String> errorOnlyStatuses = new HashSet<>(onerror);
            errorOnlyStatuses.removeAll(emitted);
            out.println();
            out.println("edge [arrowhead=\"vee\",fontname=\"Helvetica\",fontsize=\"8\",penwidth=0.8];");
            for (DoerMethodInfo method : sortedDoerMethods) {
                String methodNodeName = methodNodeNames.get(method);
                List<AcceptStatus> acceptList = new ArrayList<>(method.acceptList);
                acceptList.sort(Comparator.comparing(AcceptStatus::value));
                for (AcceptStatus acceptStatus : acceptList) {
                    String status = acceptStatus.value();
                    String statusNodeName = statusNodeNames.get(status);
                    if (!"".equals(acceptStatus.delay())) {
                        String label = "delay " + acceptStatus.delay();
                        String toolTip = acceptStatus.delay();
                        out.printf("%s -> %s[arrowtail=dot,dir=both,label=\"%s\", tooltip=\"%s\"];%n",
                                statusNodeName, methodNodeName, escape(label), escape(toolTip));
                    } else if (errorOnlyStatuses.contains(status)) {
                        out.printf("%s -> %s[color=\"red\"];%n",
                                statusNodeName, methodNodeName);
                    } else {
                        out.printf("%s -> %s;%n", statusNodeName, methodNodeName);
                    }
                }
                List<String> emitList = new ArrayList<>(new HashSet<>(method.emitList));
                emitList.sort(Comparator.nullsLast(Comparator.naturalOrder()));
                for (String status : emitList) {
                    String statusNodeName = (status != null ? statusNodeNames.get(status) :
                            terminationStatusNodeNames.get(method));
                    out.printf("%s -> %s;%n", methodNodeName, statusNodeName);
                }
                if (hasFallbackEdge(method)) {
                    String statusNodeName = (method.fallbackStatus != null ? statusNodeNames.get(method.fallbackStatus)
                            : terminationStatusNodeNames.get(method));
                    String toolTip = "[after retry] every " + method.retryIntervalText
                            + " during " + method.retryDurationText;
                    out.printf("%s -> %s[color=\"red\", tooltip=\"%s\"];%n",
                            methodNodeName, statusNodeName, escape(toolTip));
                }
            }
            out.println("}");
        }
    }

    /**
     * Only an explicit {@code @RetryPolicy} with a duration is drawn; the default policy applies to every
     * unannotated method and would clutter the diagram.
     */
    private boolean hasFallbackEdge(DoerMethodInfo method) {
        return method.hasRetryPolicy() && method.retryDurationText != null;
    }

    protected String createDurationLiteral(Duration duration) {
        if (duration == null) {
            return "null";
        } else if (duration.isZero()) {
            return "Duration.ZERO";
        } else {
            long seconds = duration.getSeconds();
            long secondsInDay = Duration.ofDays(1).getSeconds();
            long secondsInHour = Duration.ofHours(1).getSeconds();
            long secondsInMinute = Duration.ofMinutes(1).getSeconds();
            if (seconds % secondsInDay == 0) {
                return "Duration.ofDays(" + (seconds / secondsInDay) + ")";
            } else if (seconds % secondsInHour == 0) {
                return "Duration.ofHours(" + (seconds / secondsInHour) + ")";
            } else if (seconds % secondsInMinute == 0) {
                return "Duration.ofMinutes(" + (seconds / secondsInMinute) + ")";
            } else {
                return "Duration.ofSeconds(" + seconds + ")";
            }
        }
    }

    protected Duration parseDuration(String duration) {
        Matcher matcher = Pattern.compile("^\\s*(\\d+)\\s*(\\w+)\\s*$")
                .matcher(duration);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Failed to parse duration");
        }
        int amount = Integer.parseInt(matcher.group(1));
        String unit = matcher.group(2);
        TimeUnit timeUnit;
        switch (unit) {
            case "s":
            case "sec":
            case "second":
            case "seconds":
                timeUnit = TimeUnit.SECONDS;
                break;
            case "m":
            case "min":
            case "minute":
            case "minutes":
                timeUnit = TimeUnit.MINUTES;
                break;
            case "h":
            case "hour":
            case "hours":
                timeUnit = TimeUnit.HOURS;
                break;
            case "d":
            case "day":
            case "days":
                timeUnit = TimeUnit.DAYS;
                break;
            default:
                throw new IllegalArgumentException("Unknown duration unit type: " + unit);
        }
        return Duration.ofMillis(timeUnit.toMillis(amount));
    }

    String createSqlValues(List<String> values) {
        List<String> escapedValues = values.stream()
                .map(v -> "  '" + v.replaceAll("'", "''") + "'")
                .collect(Collectors.toList());
        return String.join(",\n", escapedValues);
    }

    private HashMap<String, String> createTypeShortcuts() {
        Elements elementUtils = processingEnv.getElementUtils();
        HashMap<String, String> shortnames = new HashMap<>();
        shortnames.put("com.doer.Task", "Task");
        shortnames.put("com.doer.DoerService", "DoerService");
        shortnames.put("java.lang.Override", "Override");
        shortnames.put("java.lang.Exception", "Exception");
        shortnames.put("java.lang.Throwable", "Throwable");
        shortnames.put("jakarta.transaction.Transactional", "Transactional");
        shortnames.put("jakarta.inject.Inject", "Inject");
        shortnames.put("jakarta.enterprise.context.ApplicationScoped", "ApplicationScoped");
        shortnames.put("jakarta.annotation.Generated", "Generated");
        shortnames.put("jakarta.json.JsonObjectBuilder", "JsonObjectBuilder");
        shortnames.put("jakarta.json.JsonObject", "JsonObject");
        shortnames.put("jakarta.json.JsonArrayBuilder", "JsonArrayBuilder");
        shortnames.put("jakarta.json.JsonArray", "JsonArray");
        shortnames.put("jakarta.json.Json", "Json");
        shortnames.put("jakarta.json.JsonWriterFactory", "JsonWriterFactory");
        shortnames.put("jakarta.json.JsonWriter", "JsonWriter");
        shortnames.put("jakarta.json.stream.JsonGenerator", "JsonGenerator");

        shortnames.put("java.util.concurrent.Callable", "Callable");
        shortnames.put("javax.sql.DataSource", "DataSource");
        shortnames.put("java.sql.Connection", "Connection");
        shortnames.put("java.sql.SQLException", "SQLException");
        shortnames.put("java.io.IOException", "IOException");
        shortnames.put("java.util.List", "List");
        shortnames.put("java.util.HashMap", "HashMap");
        shortnames.put("java.util.Collections", "Collections");
        shortnames.put("java.util.ArrayList", "ArrayList");
        shortnames.put("java.io.StringWriter", "StringWriter");
        shortnames.put("java.time.Duration", "Duration");
        shortnames.put("java.util.concurrent.Executor", "Executor");
        shortnames.put("com.doer.TaskUpdater", "TaskUpdater");
        shortnames.put("com.doer.TaskAndDataUpdater", "TaskAndDataUpdater");

        Stream<String> classes1 = doerMethods.stream().map(s -> s.className);
        Stream<String> classes2 = loaders.stream().map(s -> s.className);
        Stream<String> classes3 = savers.stream().map(s -> s.className);
        Stream<String> classes4 = doerMethods.stream().flatMap(s -> s.parameterTypes.stream());
        Stream<String> classes5 = describers.stream().flatMap(s -> Stream.of(s.type, s.className));
        Stream<String> classes6 = Stream.concat(loaders.stream().map(s -> s.type), savers.stream().map(s -> s.type))
                .filter(this::isPlainClassType);
        Stream.of(classes1, classes2, classes3, classes4, classes5, classes6).flatMap(i -> i).forEach(cn -> {
            if (!shortnames.containsKey(cn)) {
                TypeElement element = elementUtils.getTypeElement(cn);
                if (element == null) {
                    processingEnv.getMessager().printMessage(Kind.ERROR, "Can not load type information about " + cn);
                    return;
                }
                String name = element.getSimpleName().toString();
                if (shortnames.values().contains(name)) {
                    shortnames.put(cn, cn);
                } else {
                    shortnames.put(cn, name);
                }
            }
        });
        return shortnames;
    }

    /** Class without type arguments, so it can be used as a {@code X.class} literal. */
    private boolean isPlainClassType(String type) {
        return !type.contains("<") && processingEnv.getElementUtils().getTypeElement(type) != null;
    }

    private HashMap<String, String> createFieldNames() {
        Elements elementUtils = processingEnv.getElementUtils();
        HashMap<String, String> fieldNames = new HashMap<>();
        Stream<String> classes1 = doerMethods.stream().map(s -> s.className);
        Stream<String> classes2 = loaders.stream().map(s -> s.className);
        Stream<String> classes3 = savers.stream().map(s -> s.className);
        Stream<String> classes4 = describers.stream().map(s -> s.className);
        Stream.of(classes1, classes2, classes3, classes4).flatMap(i -> i).forEach(cn -> {
            if (!fieldNames.containsKey(cn)) {
                TypeElement element = elementUtils.getTypeElement(cn);
                fieldNames.put(cn, createFieldName(element.getSimpleName().toString(), fieldNames));
            }
        });
        return fieldNames;
    }

    private String createFieldName(String shortName, HashMap<String, String> fieldNames) {
        List<String> notAllowedNames = Arrays.asList("task", "status", "args", "dataSource", "data", "type", "updater");
        List<String> javaKeywords = Arrays.asList(
                "abstract", "continue", "for", "new", "switch", "assert", "default", "goto", "package", "synchronized",
                "boolean", "do", "if", "private", "this", "break", "double", "implements", "protected", "throw",
                "byte", "else", "import", "public", "throws", "case", "enum", "instanceof", "return", "transient",
                "catch", "extends", "int", "short", "try", "char", "final", "interface", "static", "void",
                "class", "finally", "long", "strictfp", "volatile", "const", "float", "native", "super", "while");
        String name = shortName.substring(0, 1).toLowerCase() + shortName.substring(1);
        if (!(fieldNames.values().contains(name) || notAllowedNames.contains(name) || javaKeywords.contains(name))) {
            return name;
        }
        for (int i = 0; i < 10000; i++) {
            String varName = "var" + i;
            if (!fieldNames.values().contains(varName)) {
                return name;
            }
        }
        throw new RuntimeException("Can not create field name");
    }

    private String escape(String s) {
        return s.replaceAll(Pattern.quote("\\"), "\\\\")
                .replaceAll(Pattern.quote("\r"), "\\r")
                .replaceAll(Pattern.quote("\n"), "\\n")
                .replaceAll(Pattern.quote("\t"), "\\t");
    }

    private String jstr(String s) {
        if (s == null) {
            return "null";
        } else {
            return "\"" + escape(s).replaceAll(Pattern.quote("\""), "\\\"") + "\"";
        }
    }
}
