package com.doer.processor;

import com.doer.*;
import com.doer.processor.DoerMethodInfo.Accept;
import com.google.auto.service.AutoService;
import com.sun.source.util.JavacTask;
import com.sun.source.util.TaskEvent;
import com.sun.source.util.TaskListener;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ExecutableType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic.Kind;
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

    private static final Pattern DURATION_PATTERN = Pattern.compile("\\s*(\\d+)\\s*(\\w+)\\s*");
    /** Names of parameters and locals of the generated service that would hide a bean field. */
    private static final Set<String> RESERVED_FIELD_NAMES = Set.of(
            "task", "status", "args", "dataSource", "data", "type", "updater", "exception", "builder");
    /**
     * Types the generated service refers to by short names. Includes the java.lang ones, because an import of
     * a user class with the same short name would hide them.
     */
    private static final List<String> TYPES_USED_IN_GENERATED_CODE = List.of(
            "com.doer.Task", "com.doer.DoerService", "com.doer.TaskUpdater", "com.doer.TaskAndDataUpdater",
            "java.lang.Override", "java.lang.Exception", "java.lang.Throwable", "java.lang.String",
            "java.lang.Object", "java.lang.Class", "java.lang.Integer", "java.lang.IllegalArgumentException",
            "jakarta.transaction.Transactional", "jakarta.inject.Inject",
            "jakarta.enterprise.context.ApplicationScoped", "jakarta.annotation.Generated",
            "jakarta.json.JsonObjectBuilder", "jakarta.json.JsonObject", "jakarta.json.JsonArrayBuilder",
            "jakarta.json.JsonArray", "jakarta.json.Json", "jakarta.json.JsonWriterFactory",
            "jakarta.json.JsonWriter", "jakarta.json.stream.JsonGenerator",
            "java.util.concurrent.Callable", "java.util.concurrent.Executor", "javax.sql.DataSource",
            "java.sql.SQLException", "java.io.IOException", "java.io.StringWriter", "java.util.List",
            "java.util.HashMap", "java.time.Duration");

    // Filled by process() in the round with doer annotations
    private final List<DoerMethodInfo> doerMethods = new ArrayList<>();
    private final List<TypedMethodInfo> loaders = new ArrayList<>();
    private final List<TypedMethodInfo> savers = new ArrayList<>();
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
    /** True when a compile error was reported before or during annotation processing. */
    private boolean errorRaised;

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
        errorRaised |= roundEnv.errorRaised();
        if (annotations.isEmpty() || isCompilingTests()) {
            return false;
        }
        loadDoerMethods(roundEnv);
        loadLoaders(roundEnv);
        loadSavers(roundEnv);
        loadExceptionDescribers(roundEnv);
        loadConcurrencyDomains(roundEnv);
        try {
            generateDoerService();
            generateCreateSchemaSql();
            generateSelectTaskSql();
            generateCreateIndexSql();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // doer.json and doer.dot are generated when compilation has finished (see init)
        doerAnnotationsProcessed = true;
        return true;
    }

    private void onJavacTaskFinished(TaskEvent e) {
        if (!doerAnnotationsProcessed) {
            return;
        }
        if (e.getKind() == TaskEvent.Kind.ANALYZE) {
            analyzedClasses++;
            setStatusFinder.scanTopLevelType(e.getTypeElement());
        } else if (e.getKind() == TaskEvent.Kind.COMPILATION) {
            if (analyzedClasses == 0 && !errorRaised) {
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

    private boolean isCompilingTests() {
        TypeElement typeElement = processingEnv.getElementUtils()
                .getTypeElement("com.doer.generated._GeneratedDoerService");
        if (typeElement != null) {
            String message = "The class _GeneratedDoerService is already present in dependencies. Looks like " +
                    "DoerProcessor is called during test code compilation, and should not generate any extra code";
            processingEnv.getMessager().printMessage(Kind.NOTE, message, typeElement);
            return true;
        }
        return false;
    }

    private void loadDoerMethods(RoundEnvironment roundEnv) {
        Set<Element> elements = new LinkedHashSet<>(roundEnv.getElementsAnnotatedWith(AcceptStatus.class));
        elements.addAll(roundEnv.getElementsAnnotatedWith(AcceptStatuses.class));

        for (Element element : elements) {
            if (methodType(element, "AcceptStatus") == null) {
                continue;
            }
            List<String> parameterTypes = new ArrayList<>();
            for (VariableElement parameter : ((ExecutableElement) element).getParameters()) {
                parameterTypes.add(supportedDoerArgumentType(parameter.asType(),
                        "parameter " + parameter.getSimpleName() + " of @AcceptStatus method", parameter));
            }
            String className = ownerClassName(element);
            if (className == null || parameterTypes.contains(null)) {
                continue;
            }
            DoerMethodInfo info = new DoerMethodInfo();
            info.className = className;
            info.methodName = element.getSimpleName().toString();
            info.parameterTypes = parameterTypes;
            for (AcceptStatus annotation : element.getAnnotationsByType(AcceptStatus.class)) {
                validateStatus(annotation.value(), "@AcceptStatus value", element);
                if (annotation.delay().isEmpty()) {
                    info.acceptList.add(new Accept(annotation.value(), null, null));
                    continue;
                }
                Duration delay = parseAnnotationDuration(annotation.delay(), "@AcceptStatus delay", element);
                if (delay != null && delay.isZero()) {
                    // A zero delay would be a delayed queue next to the asap queue with the same delay: the
                    // generated SelectTasks.sql would have more LIMIT parameters than the service has queues
                    error("@AcceptStatus delay \"" + escape(annotation.delay()) + "\" must be greater than zero. "
                            + "Remove delay to process the task as soon as possible.", element);
                    delay = null;
                }
                // Compilation fails when the delay is invalid; keep the status without a delay so that the
                // generated code stays valid and the status is still checked
                info.acceptList.add(delay != null ? new Accept(annotation.value(), annotation.delay(), delay)
                        : new Accept(annotation.value(), null, null));
            }
            loadRetryPolicy(info, element);
            info.domainName = resolveDomainName(element);
            info.element = element;
            doerMethods.add(info);
        }
        checkStatusesAcceptedOnce();
    }

    /** Reports an error on every doer method that accepts a status also accepted by another doer method. */
    private void checkStatusesAcceptedOnce() {
        Map<String, Set<DoerMethodInfo>> methodsByStatus = new TreeMap<>();
        for (DoerMethodInfo method : doerMethods) {
            for (Accept accept : method.acceptList) {
                methodsByStatus.computeIfAbsent(accept.status(), k -> new LinkedHashSet<>()).add(method);
            }
        }
        methodsByStatus.forEach((status, methods) -> {
            if (methods.size() < 2) {
                return;
            }
            List<DoerMethodInfo> sorted = methods.stream().sorted(DoerMethodInfo.BY_SIGNATURE).toList();
            String list = sorted.stream()
                    .map(m -> "    " + m.className + "." + m.element)
                    .collect(Collectors.joining("\n"));
            for (DoerMethodInfo method : sorted) {
                error("Status \"" + escape(status)
                        + "\" is accepted by more than one doer method; a status can be accepted by only one "
                        + "@AcceptStatus method:\n" + list, method.element);
            }
        });
    }

    private void loadRetryPolicy(DoerMethodInfo info, Element element) {
        RetryPolicy policy = element.getAnnotation(RetryPolicy.class);
        if (policy == null) {
            info.retryInterval = DEFAULT_RETRY_INTERVAL;
            info.retryDuration = DEFAULT_RETRY_DURATION;
            return;
        }
        info.retryIntervalText = policy.interval();
        info.retryInterval = parseAnnotationDuration(policy.interval(), "@RetryPolicy interval", element);
        if (info.retryInterval == null) {
            info.retryInterval = DEFAULT_RETRY_INTERVAL;
        }
        if (!policy.duration().isEmpty()) {
            info.retryDurationText = policy.duration();
            info.retryDuration = parseAnnotationDuration(policy.duration(), "@RetryPolicy duration", element);
        }
        if (!policy.fallbackStatus().isEmpty()) {
            validateStatus(policy.fallbackStatus(), "@RetryPolicy fallbackStatus", element);
            if (policy.duration().isEmpty()) {
                error("@RetryPolicy fallbackStatus requires duration: without duration "
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
            error(what + " \"" + text + "\" is not a duration. "
                    + "Expected a number and a unit, e.g. \"5s\", \"10 min\", \"2h\", \"1 day\".", element);
            return null;
        }
    }

    private void validateStatus(String status, String what, Element element) {
        if (status.isEmpty()) {
            error(what + " must not be empty.", element);
        } else if (!status.equals(status.strip())) {
            error(what + " \"" + escape(status) + "\" must not start or end with whitespace.", element);
        } else if (status.length() > MAX_STATUS_LENGTH) {
            error(what + " \"" + escape(status) + "\" is " + status.length()
                    + " characters long; the maximum is " + MAX_STATUS_LENGTH + ".", element);
        }
    }

    /** Type of the annotated method; reports an error and returns null when the element is not a method. */
    private ExecutableType methodType(Element element, String annotationName) {
        if (element.getKind() == ElementKind.METHOD) {
            return (ExecutableType) element.asType();
        }
        error(annotationName + " annotation can be used only on public methods.", element);
        return null;
    }

    /**
     * Name of the class when doer methods (@AcceptStatus methods) support the type as an argument, and so
     * {@code @TaskDataLoader} and {@code @TaskDataSaver} support it too; otherwise reports an error and returns null.
     * The generated code uses the class as {@code X} and as {@code X.class}.
     */
    private String supportedDoerArgumentType(TypeMirror type, String what, Element element) {
        Types types = processingEnv.getTypeUtils();
        if (type.getKind() == TypeKind.ERROR) {
            error("Type " + type + " of " + what + " can not be resolved.", element);
            return null;
        }
        String notSupported = "Type " + type + " of " + what + " is not supported by doer methods "
                + "(@AcceptStatus methods). ";
        if (type.getKind() != TypeKind.DECLARED || !types.isSameType(type, types.erasure(type))) {
            error(notSupported + "Only classes without type arguments are supported, "
                    + "e.g. Order, but not List<Order>, Order[] or int.", element);
            return null;
        }
        TypeElement typeElement = (TypeElement) types.asElement(type);
        String inaccessibility = inaccessibility(typeElement);
        if (inaccessibility != null) {
            error(notSupported + "The class " + inaccessibility + ", so the generated service in package "
                    + "com.doer.generated can not refer to it.", element);
            return null;
        }
        return typeElement.getQualifiedName().toString();
    }

    /**
     * Name of the class when it can be task data of {@code @TaskDataLoader} and {@code @TaskDataSaver};
     * otherwise reports an error and returns null.
     */
    private String supportedTaskDataType(TypeMirror type, String what, Element element) {
        String name = supportedDoerArgumentType(type, what, element);
        if (Task.class.getName().equals(name) || DoerService.class.getName().equals(name)) {
            error("Type " + name + " of " + what + " can not be task data.", element);
            return null;
        }
        return name;
    }

    /** Why code in another package can not refer to the class; null when it can. */
    private String inaccessibility(TypeElement type) {
        if (processingEnv.getElementUtils().getPackageOf(type).isUnnamed()) {
            return "is in the unnamed package";
        }
        for (Element e = type; e instanceof TypeElement t; e = t.getEnclosingElement()) {
            if (!t.getModifiers().contains(Modifier.PUBLIC)) {
                return t == type ? "is not public" : "is declared in not public class " + t.getQualifiedName();
            }
        }
        return null;
    }

    /**
     * Name of the class declaring the method; reports an error and returns null when the generated code can not
     * refer to the class.
     */
    private String ownerClassName(Element method) {
        TypeElement owner = (TypeElement) method.getEnclosingElement();
        String className = owner.getQualifiedName().toString();
        if (processingEnv.getElementUtils().getPackageOf(owner).isUnnamed()) {
            String message = String.format("Class in unnamed package\n" +
                    "%s can not import classes from default package.\n" +
                    "See chapter 7.5 Import Declarations in Java Spec " +
                    "https://docs.oracle.com/javase/specs/jls/se11/html/jls-7.html#jls-7.5\n" +
                    "Please move your class %s to any package, so %s can import it.",
                    DoerService.class.getName(), className, DoerService.class.getName());
            error(message, method);
            return null;
        }
        String inaccessibility = inaccessibility(owner);
        if (inaccessibility != null) {
            error("Class " + className + " " + inaccessibility + ", so the generated service in package "
                    + "com.doer.generated can not refer to it.", method);
            return null;
        }
        return className;
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
        if (element.getKind() == ElementKind.METHOD && element.getAnnotation(ConcurrencyLimit.class) == null) {
            return resolveDomainName(element.getEnclosingElement());
        }
        return describeElement(element);
    }

    /**
     * Name of the implicit domain the element runs in, or null when it runs in a named domain
     * ({@code @ConcurrencyGroup} on the method or its class).
     */
    private String derivedDomainName(Element element) {
        if (element.getAnnotation(ConcurrencyGroup.class) != null) {
            return null;
        }
        if (element.getKind() == ElementKind.METHOD && element.getAnnotation(ConcurrencyLimit.class) == null) {
            return derivedDomainName(element.getEnclosingElement());
        }
        return describeElement(element);
    }

    /** {@code Class.method} for a method, the class name for a class. */
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
                error("@ConcurrencyGroup value must not be empty.", element);
                continue;
            }
            namedDomains.add(name);
            if (implicitDomains.contains(name)) {
                continue;
            }
            // A name that equals a class or Class.method name may only join the implicit domain of that
            // class or method, and only when that domain exists.
            String kind = null;
            if (elementUtils.getTypeElement(name) != null) {
                kind = "class";
            } else if (name.contains(".")) {
                String className = name.substring(0, name.lastIndexOf('.'));
                String methodName = name.substring(name.lastIndexOf('.') + 1);
                TypeElement owner = elementUtils.getTypeElement(className);
                if (owner != null && owner.getEnclosedElements().stream().anyMatch(e ->
                        e.getKind() == ElementKind.METHOD && e.getSimpleName().contentEquals(methodName))) {
                    kind = "method";
                }
            }
            if (kind != null) {
                error("@ConcurrencyGroup(\"" + name + "\") uses the name of " + kind
                        + " " + name + ", but no doer method runs in the implicit concurrency domain of that " + kind
                        + " (it is not a doer method or class, or it declares its own @ConcurrencyGroup).\n"
                        + "A class or method name can be used only to join an existing implicit domain. "
                        + "Use the @ConcurrencyGroup name of that " + kind
                        + " or a name that is not a class or method name.", element);
            }
        }

        Map<String, List<Element>> limitElements = new TreeMap<>();
        for (Element element : roundEnv.getElementsAnnotatedWith(ConcurrencyLimit.class)) {
            if (element.getAnnotation(ConcurrencyLimit.class).value() < 1) {
                error("@ConcurrencyLimit value must be at least 1.", element);
                continue;
            }
            limitElements.computeIfAbsent(resolveDomainName(element), k -> new ArrayList<>()).add(element);
        }
        limitElements.forEach((domainName, elements) -> {
            elements.sort(Comparator.comparing(this::describeElement));
            Set<Integer> values = elements.stream()
                    .map(e -> e.getAnnotation(ConcurrencyLimit.class).value())
                    .collect(Collectors.toSet());
            if (values.size() > 1) {
                String list = elements.stream()
                        .map(e -> "    " + describeElement(e) + ": " + e.getAnnotation(ConcurrencyLimit.class).value())
                        .collect(Collectors.joining("\n"));
                for (Element element : elements) {
                    error("Different @ConcurrencyLimit values for concurrency domain \"" + domainName + "\":\n"
                            + list, element);
                }
                return;
            }
            limits.put(domainName, values.iterator().next());
            if (!usedDomains.contains(domainName)) {
                for (Element element : elements) {
                    warning("@ConcurrencyLimit has no effect: no doer method runs in concurrency domain \""
                            + domainName + "\".", element);
                }
            }
        });
    }

    private void loadLoaders(RoundEnvironment roundEnv) {
        for (Element element : roundEnv.getElementsAnnotatedWith(TaskDataLoader.class)) {
            ExecutableType method = methodType(element, TaskDataLoader.class.getName());
            if (method == null) {
                continue;
            }
            List<? extends TypeMirror> args = method.getParameterTypes();
            if (args.size() != 1 || !Task.class.getName().equals(args.get(0).toString())) {
                error(TaskDataLoader.class.getName() + " should have exactly 1 argument of type "
                        + Task.class.getName(), element);
                continue;
            }
            String type = supportedTaskDataType(method.getReturnType(),
                    "return value of @" + TaskDataLoader.class.getSimpleName() + " method", element);
            String className = ownerClassName(element);
            if (type == null || className == null) {
                continue;
            }
            loaders.add(new TypedMethodInfo(className, element.getSimpleName().toString(), type));
        }
    }

    private void loadSavers(RoundEnvironment roundEnv) {
        for (Element element : roundEnv.getElementsAnnotatedWith(TaskDataSaver.class)) {
            ExecutableType method = methodType(element, TaskDataSaver.class.getName());
            if (method == null) {
                continue;
            }
            List<? extends TypeMirror> args = method.getParameterTypes();
            if (args.size() != 2 || !Task.class.getName().equals(args.get(0).toString())
                    || method.getReturnType().getKind() != TypeKind.VOID) {
                error(TaskDataSaver.class.getName()
                        + " should have exactly 2 arguments: Task and the task data to save, and should return void.",
                        element);
                continue;
            }
            VariableElement data = ((ExecutableElement) element).getParameters().get(1);
            String type = supportedTaskDataType(data.asType(),
                    "parameter " + data.getSimpleName() + " of @" + TaskDataSaver.class.getSimpleName() + " method",
                    data);
            String className = ownerClassName(element);
            if (type == null || className == null) {
                continue;
            }
            savers.add(new TypedMethodInfo(className, element.getSimpleName().toString(), type));
        }
    }

    private void loadExceptionDescribers(RoundEnvironment roundEnv) {
        Types types = processingEnv.getTypeUtils();
        for (Element element : roundEnv.getElementsAnnotatedWith(ExceptionDescriber.class)) {
            ExecutableType method = methodType(element, ExceptionDescriber.class.getName());
            if (method == null) {
                continue;
            }
            List<? extends TypeMirror> args = method.getParameterTypes();
            if (args.size() != 3 || !Task.class.getName().equals(args.get(0).toString())
                    || !"jakarta.json.JsonObjectBuilder".equals(args.get(2).toString())
                    || method.getReturnType().getKind() != TypeKind.VOID) {
                error("@" + ExceptionDescriber.class.getSimpleName() + " method should return void and have exactly "
                        + "3 parameters: Task, the exception type it describes (Throwable or any subclass of it) "
                        + "and JsonObjectBuilder.\n"
                        + "Example:\n"
                        + "@" + ExceptionDescriber.class.getName() + "\n"
                        + "public void describeSqlException(Task task, SQLException e, JsonObjectBuilder builder) {\n"
                        + "}", element);
                continue;
            }
            String className = ownerClassName(element);
            String methodName = element.getSimpleName().toString();
            TypeMirror exType = args.get(1);
            if (exType.getKind() == TypeKind.ERROR) {
                error("Type " + exType + " of the second parameter of @" + ExceptionDescriber.class.getSimpleName()
                        + " annotated method " + methodName + " can not be resolved.", element);
                continue;
            }
            TypeMirror throwable = processingEnv.getElementUtils().getTypeElement(Throwable.class.getName()).asType();
            if (exType.getKind() != TypeKind.DECLARED || !types.isAssignable(exType, throwable)) {
                error("Second parameter of @" + ExceptionDescriber.class.getSimpleName()
                        + " annotated method " + methodName + " should be of Throwable type", element);
                continue;
            }
            TypeElement exElement = (TypeElement) types.asElement(exType);
            String inaccessibility = inaccessibility(exElement);
            if (inaccessibility != null) {
                error("Exception class " + exElement.getQualifiedName() + " of the second parameter of @"
                        + ExceptionDescriber.class.getSimpleName() + " annotated method " + methodName + " "
                        + inaccessibility + ", so the generated service in package com.doer.generated can not "
                        + "refer to it.", element);
                continue;
            }
            if (className == null) {
                continue;
            }
            describers.add(new ExceptionDescriberInfo(className, methodName, exElement.getQualifiedName().toString(),
                    extractParentClasses(types, exElement.getSuperclass())));
        }

        Set<String> describerTypes = describers.stream().map(ExceptionDescriberInfo::type).collect(Collectors.toSet());
        describers.forEach(d -> d.typeParents().removeIf(t -> !describerTypes.contains(t)));
        // Base classes comes first, then alphabetically ordered by class name
        describers.sort((a, b) -> {
            if (a.typeParents().contains(b.type())) {
                return 1;
            } else if (b.typeParents().contains(a.type())) {
                return -1;
            } else {
                return a.type().compareTo(b.type());
            }
        });
    }

    private List<String> extractParentClasses(Types types, TypeMirror typeMirror) {
        List<String> result = new ArrayList<>();
        result.add(typeMirror.toString());
        if (types.asElement(typeMirror) instanceof TypeElement element && element.getKind() == ElementKind.CLASS) {
            result.addAll(extractParentClasses(types, element.getSuperclass()));
        }
        return result;
    }

    private void generateDoerService() throws IOException {
        Map<String, String> shortcuts = createTypeShortcuts();
        Map<String, String> fieldNames = createFieldNames();
        List<String> beans = beanClassNames().distinct().sorted().toList();

        JavaFileObject file = processingEnv.getFiler().createSourceFile("com.doer.generated._GeneratedDoerService");
        try (PrintWriter out = new PrintWriter(file.openWriter())) {
            out.println("package com.doer.generated;");
            new TreeMap<>(shortcuts).forEach((fullName, shortName) -> {
                if (!fullName.equals(shortName)) {
                    out.println("import " + fullName + ";");
                }
            });
            out.println();
            out.println("@ApplicationScoped");
            out.println("@Generated(value = \"" + getClass().getName() + "\", date = \"" + LocalDate.now() + "\")");
            out.println("public class _GeneratedDoerService extends DoerService {");
            out.println();
            for (String bean : beans) {
                out.println("    " + shortcuts.get(bean) + " " + fieldNames.get(bean) + ";");
            }
            out.print("""

                        public _GeneratedDoerService() {
                            super();
                            initializeDomains();
                        }

                        @Override
                        @Inject
                        public void setSelfReference(DoerService self) {
                            super.setSelfReference(self);
                        }

                        @Override
                        @Inject
                        public void setDataSource(DataSource dataSource) {
                            super.setDataSource(dataSource);
                        }

                        @Override
                        @Inject
                        public void setExecutor(Executor executor) {
                            super.setExecutor(executor);
                        }

                    """);
            for (String bean : beans) {
                out.println("    @Inject");
                out.println("    public void _inject_" + fieldNames.get(bean) + "(" + shortcuts.get(bean) + " value) {");
                out.println("        this." + fieldNames.get(bean) + " = value;");
                out.println("    }");
                out.println();
            }
            out.print("""
                        @Override
                        @Transactional(value = Transactional.TxType.REQUIRES_NEW, rollbackOn = Exception.class)
                        public void runInTransaction(Callable<Object> code) throws Exception {
                            code.call();
                        }

                        @Override
                        @Transactional(Transactional.TxType.REQUIRED)
                        public int resetStalledInProgressTasks(Duration timeout) throws SQLException {
                            return super.resetStalledInProgressTasks(timeout);
                        }

                        @Override
                        @Transactional(Transactional.TxType.NOT_SUPPORTED)
                        public void reloadTasksFromDb() {
                            super.reloadTasksFromDb();
                        }

                        @Override
                        @Transactional(Transactional.TxType.REQUIRED)
                        public List<Task> loadTasksFromDatabase(List<Integer> limits) throws SQLException, IOException {
                            return super.loadTasksFromDatabase(limits);
                        }

                    """);
            generateExtraJson(out, shortcuts, fieldNames);
            out.print("""

                        @Override
                        @Transactional(Transactional.TxType.NEVER)
                        public Task facilitateCoordinatedUpdate(long taskId, Duration waitDuration, boolean allowHijacking,
                                TaskUpdater updater) throws Exception {
                            return super.facilitateCoordinatedUpdate(taskId, waitDuration, allowHijacking, updater);
                        }

                        @Override
                        @Transactional(Transactional.TxType.NEVER)
                        public <T> Task facilitateCoordinatedUpdate(long taskId, Duration waitDuration, boolean allowHijacking,
                                Class<T> dataType, TaskAndDataUpdater<T> updater) throws Exception {
                            return super.facilitateCoordinatedUpdate(taskId, waitDuration, allowHijacking, dataType, updater);
                        }

                    """);
            generateLoad(out, shortcuts, fieldNames);
            out.println();
            generateSave(out, shortcuts, fieldNames);
            out.println();
            generateRunTask(out, shortcuts, fieldNames);
            out.println();
            generateInitializeDomains(out);
            out.println();
            out.println("}");
        }
    }

    private void generateExtraJson(PrintWriter out, Map<String, String> shortcuts, Map<String, String> fieldNames) {
        out.print("""
                    @Override
                    public String createExtraJson(Task task, Exception exception) {
                        JsonObjectBuilder builder = Json.createObjectBuilder();
                        try {
                            fillExtraJson(task, exception, builder);
                        } catch (Exception e) {
                            LOG.log(java.util.logging.Level.WARNING, "ExtraJson creation error", e);
                        }
                        JsonObject jsonObject = builder.build();
                        if (jsonObject.isEmpty()) {
                            return null;
                        }
                        HashMap<String, Object> config = new HashMap<>();
                        if (jsonObject.size() > 1) {
                            config.put(JsonGenerator.PRETTY_PRINTING, true);
                        }
                        JsonWriterFactory factory = Json.createWriterFactory(config);
                        StringWriter sw = new StringWriter();
                        try (JsonWriter writer = factory.createWriter(sw)) {
                            writer.writeObject(jsonObject);
                        }
                        return sw.toString();
                    }

                    private void fillExtraJson(Task task, Throwable exception, JsonObjectBuilder builder) throws Exception {
                        if (exception == null) {
                            return;
                        }

                        if (exception.getMessage() != null && !"".equals(exception.getMessage().trim())) {
                            builder.add("message", limitTo1024(exception.getMessage().trim()));
                        }
                """);
        for (ExceptionDescriberInfo describer : describers) {
            String exceptionType = shortcuts.get(describer.type());
            out.println("        if (exception instanceof " + exceptionType + ") {");
            out.println("            " + fieldNames.get(describer.className()) + "." + describer.methodName()
                    + "(task, (" + exceptionType + ") exception, builder);");
            out.println("        }");
        }
        out.print("""

                        JsonObjectBuilder causeBuilder = Json.createObjectBuilder();
                        fillExtraJson(task, exception.getCause(), causeBuilder);
                        JsonObject causeExtraJson = causeBuilder.build();
                        if (!causeExtraJson.isEmpty()) {
                            builder.add("cause", causeExtraJson);
                        }
                        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
                        for (Throwable throwable : exception.getSuppressed()) {
                            JsonObjectBuilder supperssedBuilder = Json.createObjectBuilder();
                            fillExtraJson(task, throwable, supperssedBuilder);
                            JsonObject jsonObject = supperssedBuilder.build();
                            if (!jsonObject.isEmpty()) {
                                arrayBuilder.add(jsonObject);
                            }
                        }
                        JsonArray array = arrayBuilder.build();
                        if (!array.isEmpty()) {
                            builder.add("suppressed", array);
                        }
                    }
                """);
    }

    private void generateLoad(PrintWriter out, Map<String, String> shortcuts, Map<String, String> fieldNames) {
        out.println("    @Override");
        out.println("    protected Object _load(Task task, Class<?> type) throws Exception {");
        for (TypedMethodInfo loader : firstByType(loaders).values()) {
            out.println("        if (" + shortcuts.get(loader.type()) + ".class.equals(type)) {");
            out.println("            return " + fieldNames.get(loader.className()) + "." + loader.methodName()
                    + "(task);");
            out.println("        }");
        }
        out.println("        throw new IllegalArgumentException(\"No @TaskDataLoader for \" + type.getName());");
        out.println("    }");
    }

    private void generateSave(PrintWriter out, Map<String, String> shortcuts, Map<String, String> fieldNames) {
        out.println("    @Override");
        out.println("    protected void _save(Task task, Class<?> type, Object data) throws Exception {");
        for (TypedMethodInfo saver : firstByType(savers).values()) {
            String typeName = shortcuts.get(saver.type());
            out.println("        if (" + typeName + ".class.equals(type)) {");
            out.println("            " + fieldNames.get(saver.className()) + "." + saver.methodName()
                    + "(task, (" + typeName + ") data);");
            out.println("            return;");
            out.println("        }");
        }
        out.println("    }");
    }

    private void generateRunTask(PrintWriter out, Map<String, String> shortcuts, Map<String, String> fieldNames) {
        Map<String, TypedMethodInfo> loadersByType = firstByType(loaders);
        Map<String, TypedMethodInfo> saversByType = firstByType(savers);
        Set<String> missingLoadersReported = new HashSet<>();
        int maxNumberOfParams = doerMethods.stream().mapToInt(m -> m.parameterTypes.size()).max().orElse(0);

        out.println("    @Override");
        out.println("    @Transactional(Transactional.TxType.NOT_SUPPORTED)");
        out.println("    public void runTask(Task task) throws Exception {");
        out.println("        Object[] args = new Object[" + maxNumberOfParams + "];");
        out.println("        String status = task.getStatus();");
        List<DoerMethodInfo> sortedDoerMethods = doerMethods.stream()
                .sorted(Comparator.comparing((DoerMethodInfo m) -> m.domainName)
                        .thenComparing(m -> m.methodName)
                        .thenComparing(m -> m.parameterTypes.toString()))
                .toList();
        for (int i = 0; i < sortedDoerMethods.size(); i++) {
            DoerMethodInfo info = sortedDoerMethods.get(i);
            List<String> params = info.parameterTypes;
            String condition = info.acceptList.stream()
                    .map(a -> jstr(a.status()) + ".equals(status)")
                    .collect(Collectors.joining(" ||\n                "));
            out.println((i == 0 ? "       " : " else") + " if (" + condition + ") {");

            out.println("            callDoerMethod(task, () -> {");
            for (int p = 0; p < params.size(); p++) {
                String paramClass = params.get(p);
                if (paramClass.equals(Task.class.getName())) {
                    continue;
                }
                TypedMethodInfo loader = loadersByType.get(paramClass);
                if (loader != null) {
                    out.println("                    args[" + p + "] = " + fieldNames.get(loader.className()) + "."
                            + loader.methodName() + "(task);");
                    continue;
                }
                if (missingLoadersReported.add(paramClass)) {
                    error("No @" + TaskDataLoader.class.getSimpleName() + " found for argument " + p + "\n"
                            + "Please declare loader method:\n"
                            + "@" + TaskDataLoader.class.getName() + "\n"
                            + "public " + paramClass + " method(" + Task.class.getName() + " task) {}\n",
                            info.element);
                }
                out.println("                    args[" + p + "] = null;");
            }
            out.println("                    return null;");
            out.println("                }, () -> {");
            List<String> argumentCodes = new ArrayList<>();
            for (int p = 0; p < params.size(); p++) {
                argumentCodes.add(Task.class.getName().equals(params.get(p)) ? "task"
                        : "(" + shortcuts.get(params.get(p)) + ")args[" + p + "]");
            }
            out.println("                    " + fieldNames.get(info.className) + "." + info.methodName + "("
                    + String.join(", ", argumentCodes) + ");");
            out.println("                    return null;");
            out.println("                }, () -> {");
            for (int p = params.size() - 1; p >= 0; p--) {
                TypedMethodInfo saver = saversByType.get(params.get(p));
                if (saver != null && !params.get(p).equals(Task.class.getName())) {
                    out.println("                    " + fieldNames.get(saver.className()) + "." + saver.methodName()
                            + "(task, (" + shortcuts.get(params.get(p)) + ")args[" + p + "]);");
                }
            }
            out.println("                    return null;");
            out.println("                }, " + jstr(simpleName(info.className)) + ", " + jstr(info.methodName) + ",");
            out.println("                    " + createDurationLiteral(info.retryDuration) + ", "
                    + jstr(info.fallbackStatus) + ");");
            out.print("        }");
        }
        out.println();
        out.println("    }");
    }

    private void generateInitializeDomains(PrintWriter out) {
        out.println("    protected void initializeDomains() {");
        groupMethodsByDomain(doerMethods).forEach((domainName, methods) -> {
            Map<Duration, List<String>> delays = groupStatuses(methods,
                    (m, a) -> a.delay() != null ? a.delay() : Duration.ZERO);
            Map<Duration, List<String>> retryDelays = groupStatuses(methods, (m, a) -> m.retryInterval);
            retryDelays.remove(DEFAULT_RETRY_INTERVAL);

            out.println("        {");
            out.println("            HashMap<String, Duration> delays = new HashMap<>();");
            out.println("            HashMap<String, Duration> retryDelays = new HashMap<>();");
            delays.forEach((delay, statuses) -> statuses.forEach(status -> out.println(
                    "            delays.put(" + jstr(status) + ", " + createDurationLiteral(delay) + ");")));
            retryDelays.forEach((delay, statuses) -> statuses.forEach(status -> out.println(
                    "            retryDelays.put(" + jstr(status) + ", " + createDurationLiteral(delay) + ");")));
            out.println("            setupConcurrencyDomain(" + jstr(domainName) + ", "
                    + limits.getOrDefault(domainName, DEFAULT_LIMIT) + ", delays, retryDelays);");
            out.println("        }");
        });
        out.println("    }");
    }

    private void generateCreateSchemaSql() throws IOException {
        try (PrintWriter out = openResource("CreateSchema.sql")) {
            out.print("""

                    CREATE SEQUENCE id_generator START WITH 1000 INCREMENT BY 1;

                    CREATE TABLE tasks (
                        id BIGINT DEFAULT nextval('id_generator'::regclass) PRIMARY KEY,
                        created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                        modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
                        status VARCHAR(%d),
                        in_progress BOOLEAN NOT NULL DEFAULT FALSE,
                        failing_since TIMESTAMP WITH TIME ZONE,
                        version INTEGER NOT NULL default 0
                    );

                    CREATE TABLE task_logs (
                        id BIGINT DEFAULT nextval('id_generator'::regclass) PRIMARY KEY,
                        task_id BIGINT NOT NULL,
                        created TIMESTAMP WITH TIME ZONE DEFAULT now(),
                        initial_status VARCHAR,
                        final_status VARCHAR,
                        class_name VARCHAR,
                        method_name VARCHAR,
                        duration_ms BIGINT,
                        exception_type VARCHAR,
                        extra_json JSON
                    );

                    """.formatted(MAX_STATUS_LENGTH));
        }
    }

    /**
     * Per domain: ready statuses (by created), then delayed statuses (by modified, one select per delay), then
     * failing statuses (one select per retry interval); finally all in-progress tasks. Each select except the
     * last has a LIMIT parameter, in this order.
     */
    private void generateSelectTaskSql() throws IOException {
        List<String> chunks = new ArrayList<>();
        for (List<DoerMethodInfo> methods : groupMethodsByDomain(doerMethods).values()) {
            List<String> selects = new ArrayList<>();
            List<String> asapStatuses = methods.stream()
                    .flatMap(m -> m.acceptList.stream())
                    .filter(a -> a.delay() == null)
                    .map(Accept::status)
                    .sorted()
                    .toList();
            if (!asapStatuses.isEmpty()) {
                selects.add(selectTasks("IS NULL", asapStatuses, "created"));
            }
            groupStatuses(methods, (m, a) -> a.delay()).values()
                    .forEach(statuses -> selects.add(selectTasks("IS NULL", statuses, "modified")));
            groupStatuses(methods, (m, a) -> m.retryInterval).values()
                    .forEach(statuses -> selects.add(selectTasks("IS NOT NULL", statuses, "modified")));
            chunks.add(String.join("\nUNION ALL\n", selects) + "\n\n");
        }
        chunks.add("(SELECT * FROM tasks WHERE in_progress)\n\n");

        try (PrintWriter out = openResource("SelectTasks.sql")) {
            out.print(String.join("UNION ALL\n", chunks));
        }
    }

    private String selectTasks(String failingSince, List<String> statuses, String orderBy) {
        return "(SELECT * FROM tasks WHERE NOT in_progress AND failing_since " + failingSince + " AND status IN (\n"
                + createSqlValues(statuses) + "\n"
                + ") ORDER BY " + orderBy + " LIMIT ?)";
    }

    private void generateCreateIndexSql() throws IOException {
        List<String> delayedStatuses = doerMethods.stream()
                .flatMap(m -> m.acceptList.stream())
                .filter(a -> a.delay() != null)
                .map(Accept::status)
                .sorted()
                .toList();

        try (PrintWriter out = openResource("CreateIndexes.sql")) {
            out.print("""
                    CREATE INDEX IF NOT EXISTS tasks_status_idx ON tasks (status, created);
                    CREATE INDEX IF NOT EXISTS tasks_failing_idx ON tasks (status, modified) WHERE failing_since IS NOT NULL;
                    CREATE INDEX IF NOT EXISTS tasks_in_progress_idx ON tasks (status) WHERE in_progress;
                    """);
            if (!delayedStatuses.isEmpty()) {
                out.println("CREATE INDEX IF NOT EXISTS tasks_delayed_idx ON tasks (status, modified) WHERE status IN (");
                out.println(createSqlValues(delayedStatuses));
                out.println(");");
            }
            out.println();
        }
    }

    private static String createSqlValues(List<String> values) {
        return values.stream()
                .map(v -> "  '" + v.replace("'", "''") + "'")
                .collect(Collectors.joining(",\n"));
    }

    private void generateDoerJson() throws IOException {
        try (PrintWriter out = openResource("doer.json")) {
            out.println("{");
            out.println("    \"generator\": \"" + getClass().getName() + "\",");
            out.println("    \"generated\": \"" + Instant.now() + "\",");
            out.println("    \"domains\": [");
            // Same domains as the setupConcurrencyDomain calls in the generated service
            printJsonEntries(out, doerMethods.stream()
                    .filter(m -> !m.acceptList.isEmpty())
                    .map(m -> m.domainName)
                    .distinct()
                    .sorted()
                    .map(name -> String.format("        {%s: %s, %s: %s, %s: %s}",
                            jstr("name"), jstr(name),
                            jstr("limit"), limits.getOrDefault(name, DEFAULT_LIMIT),
                            jstr("implicit"), !namedDomains.contains(name)))
                    .toList());
            out.println("    ],");
            out.println("    \"doer_methods\": [");
            List<DoerMethodInfo> sortedMethods = doerMethods.stream().sorted(DoerMethodInfo.BY_SIGNATURE).toList();
            for (int i = 0; i < sortedMethods.size(); i++) {
                printDoerMethodJson(out, sortedMethods.get(i));
                out.println("        }" + (i < sortedMethods.size() - 1 ? "," : ""));
            }
            out.println("    ],");
            out.println("    \"loaders\": [");
            printTypedMethods(out, new TreeMap<>(firstByType(loaders)).values());
            out.println("    ],");
            out.println("    \"savers\": [");
            printTypedMethods(out, new TreeMap<>(firstByType(savers)).values());
            out.println("    ],");
            out.println("    \"exception_describers\": [");
            printTypedMethods(out, describers.stream()
                    .map(d -> new TypedMethodInfo(d.className(), d.methodName(), d.type()))
                    .toList());
            out.println("    ]");
            out.println("}");
        }
    }

    /** Prints a doer method object without its closing brace. */
    private void printDoerMethodJson(PrintWriter out, DoerMethodInfo method) {
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
        out.println("            \"args\": [" + method.parameterTypes.stream().map(DoerProcessor::jstr)
                .collect(Collectors.joining(", ")) + "],");
        out.println("            \"accepts\": [");
        printJsonEntries(out, method.acceptList.stream()
                .sorted(Comparator.comparing(Accept::status))
                .map(a -> "                {" + jstr("status") + ": " + jstr(a.status())
                        + (a.delay() != null ? ", " + jstr("delay") + ": " + jstr(a.delayText()) : "") + "}")
                .toList());
        out.println("            ],");
        out.println("            \"emits\": [");
        printJsonEntries(out, method.emitList.stream()
                .filter(Objects::nonNull)
                .distinct()
                .sorted()
                .map(status -> "                " + jstr(status))
                .toList());
        if (method.emitList.contains(null)) {
            out.println("            ],");
            out.println("            \"emits_null\": true");
        } else {
            out.println("            ]");
        }
    }

    private void printTypedMethods(PrintWriter out, Collection<TypedMethodInfo> methods) {
        printJsonEntries(out, methods.stream()
                .map(m -> String.format("        {%s: %s, %s: %s, %s: %s}",
                        jstr("type"), jstr(m.type()),
                        jstr("class"), jstr(m.className()),
                        jstr("method"), jstr(m.methodName())))
                .toList());
    }

    /** Prints the entries of a JSON array (or object), one per line, separated by commas. */
    private void printJsonEntries(PrintWriter out, List<String> entries) {
        if (!entries.isEmpty()) {
            out.println(String.join(",\n", entries));
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
        try (PrintWriter out = openResource("doer.dot")) {
            out.print("""
                    digraph alg {
                        rankdir=TD;
                        graph [overlap=true];
                        node [
                            fontname=Helvetica,
                            fontsize=10,
                            shape=box,
                            style=filled,
                            margin="0.1,0.1",
                            height=0.3
                        ];
                    """);

            List<DoerMethodInfo> sortedDoerMethods = doerMethods.stream().sorted(DoerMethodInfo.BY_SIGNATURE).toList();

            Map<DoerMethodInfo, String> methodNodeNames = new HashMap<>();
            int domainIndex = 0;
            for (List<DoerMethodInfo> domainMethods : groupMethodsByDomain(sortedDoerMethods).values()) {
                String[] colorScheme = colors[domainIndex++ % colors.length];
                out.println();
                out.printf("node [color=\"%s\", fillcolor=\"%s\", fontcolor=\"%s\"];%n",
                        colorScheme[0], colorScheme[1], colorScheme[2]);
                for (DoerMethodInfo method : domainMethods) {
                    String nodeName = "m" + (101 + methodNodeNames.size());
                    methodNodeNames.put(method, nodeName);
                    out.printf("%s [label=\"%s\", tooltip=\"%s\"];%n",
                            nodeName, method.methodName, method.className + "." + method.methodName);
                }
            }

            out.println();
            out.println("node [shape=circle,fixedsize=true,color=\"black\",fillcolor=\"white\"];");

            Set<String> accepted = new HashSet<>();
            Set<String> emitted = new HashSet<>();
            Set<String> onError = new HashSet<>();
            for (DoerMethodInfo method : sortedDoerMethods) {
                emitted.addAll(method.emitList);
                method.acceptList.forEach(a -> accepted.add(a.status()));
                if (hasFallbackEdge(method) && method.fallbackStatus != null) {
                    onError.add(method.fallbackStatus);
                }
            }
            emitted.remove(null);
            Set<String> allStatuses = new HashSet<>(accepted);
            allStatuses.addAll(emitted);
            allStatuses.addAll(onError);
            // Accepted statuses first; within each group: only accepted/emitted, emitted, emitted and set on
            // error, only set on error
            Comparator<String> byRank = Comparator.comparingInt(s -> (accepted.contains(s) ? 0 : 4)
                    + (onError.contains(s) ? (emitted.contains(s) ? 2 : 3) : (emitted.contains(s) ? 1 : 0)));
            List<String> sortedStatuses = allStatuses.stream()
                    .sorted(byRank.thenComparing(Comparator.naturalOrder()))
                    .toList();
            int statusNodeIndex = 500;
            Map<String, String> statusNodeNames = new HashMap<>();
            for (String status : sortedStatuses) {
                String nodeName = "s" + (++statusNodeIndex);
                statusNodeNames.put(status, nodeName);
                out.printf("%s [label=\" \", tooltip=\"%s\"];%n", nodeName, escape(status));
            }

            Map<DoerMethodInfo, String> terminationNodeNames = new HashMap<>();
            for (DoerMethodInfo method : sortedDoerMethods) {
                if (method.emitList.contains(null) || (hasFallbackEdge(method) && method.fallbackStatus == null)) {
                    String nodeName = "n" + (++statusNodeIndex);
                    terminationNodeNames.put(method, nodeName);
                    out.printf("%s [label=\"❌\", shape=none, fillcolor=\"none\", fontcolor=\"red\", fontsize=20, tooltip=\"null\"];%n", nodeName);
                }
            }

            Set<String> errorOnlyStatuses = new HashSet<>(onError);
            errorOnlyStatuses.removeAll(emitted);
            out.println();
            out.println("edge [arrowhead=\"vee\",fontname=\"Helvetica\",fontsize=\"8\",penwidth=0.8];");
            for (DoerMethodInfo method : sortedDoerMethods) {
                String methodNodeName = methodNodeNames.get(method);
                List<Accept> acceptList = method.acceptList.stream()
                        .sorted(Comparator.comparing(Accept::status))
                        .toList();
                for (Accept accept : acceptList) {
                    String statusNodeName = statusNodeNames.get(accept.status());
                    if (accept.delay() != null) {
                        out.printf("%s -> %s[arrowtail=dot,dir=both,label=\"%s\", tooltip=\"%s\"];%n",
                                statusNodeName, methodNodeName, escape("delay " + accept.delayText()),
                                escape(accept.delayText()));
                    } else if (errorOnlyStatuses.contains(accept.status())) {
                        out.printf("%s -> %s[color=\"red\"];%n", statusNodeName, methodNodeName);
                    } else {
                        out.printf("%s -> %s;%n", statusNodeName, methodNodeName);
                    }
                }
                List<String> emitList = method.emitList.stream()
                        .distinct()
                        .sorted(Comparator.nullsLast(Comparator.naturalOrder()))
                        .toList();
                for (String status : emitList) {
                    String statusNodeName = status != null ? statusNodeNames.get(status)
                            : terminationNodeNames.get(method);
                    out.printf("%s -> %s;%n", methodNodeName, statusNodeName);
                }
                if (hasFallbackEdge(method)) {
                    String statusNodeName = method.fallbackStatus != null ? statusNodeNames.get(method.fallbackStatus)
                            : terminationNodeNames.get(method);
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

    private void error(String message, Element element) {
        processingEnv.getMessager().printMessage(Kind.ERROR, message, element);
    }

    private void warning(String message, Element element) {
        processingEnv.getMessager().printMessage(Kind.WARNING, message, element);
    }

    private PrintWriter openResource(String name) throws IOException {
        return new PrintWriter(processingEnv.getFiler()
                .createResource(StandardLocation.CLASS_OUTPUT, "com.doer.generated", name)
                .openWriter());
    }

    /** Groups doer methods by domain name, sorted by it. */
    private Map<String, List<DoerMethodInfo>> groupMethodsByDomain(List<DoerMethodInfo> methods) {
        Map<String, List<DoerMethodInfo>> domains = new TreeMap<>();
        for (DoerMethodInfo method : methods) {
            // Methods added by SetStatusFinder are not doer methods and have no domain; group them by class
            String domainName = (method.domainName != null ? method.domainName : method.className);
            domains.computeIfAbsent(domainName, k -> new ArrayList<>()).add(method);
        }
        return domains;
    }

    /**
     * Groups the accepted statuses of the methods by the duration {@code key} returns for them; statuses with a
     * null key are left out. Both the durations and the statuses of each duration are sorted.
     */
    private Map<Duration, List<String>> groupStatuses(List<DoerMethodInfo> methods,
            BiFunction<DoerMethodInfo, Accept, Duration> key) {
        Map<Duration, List<String>> result = new TreeMap<>();
        for (DoerMethodInfo method : methods) {
            for (Accept accept : method.acceptList) {
                Duration duration = key.apply(method, accept);
                if (duration != null) {
                    result.computeIfAbsent(duration, k -> new ArrayList<>()).add(accept.status());
                }
            }
        }
        result.values().forEach(Collections::sort);
        return result;
    }

    /** The first method for each type, in the order of the methods. */
    private static Map<String, TypedMethodInfo> firstByType(List<TypedMethodInfo> methods) {
        Map<String, TypedMethodInfo> result = new LinkedHashMap<>();
        methods.forEach(m -> result.putIfAbsent(m.type(), m));
        return result;
    }

    /** Classes injected into the generated service, with duplicates. */
    private Stream<String> beanClassNames() {
        return Stream.of(doerMethods.stream().map(m -> m.className),
                        loaders.stream().map(TypedMethodInfo::className),
                        savers.stream().map(TypedMethodInfo::className),
                        describers.stream().map(ExceptionDescriberInfo::className))
                .flatMap(s -> s);
    }

    private static String createDurationLiteral(Duration duration) {
        if (duration == null) {
            return "null";
        } else if (duration.isZero()) {
            return "Duration.ZERO";
        }
        long seconds = duration.getSeconds();
        if (seconds % Duration.ofDays(1).getSeconds() == 0) {
            return "Duration.ofDays(" + duration.toDays() + ")";
        } else if (seconds % Duration.ofHours(1).getSeconds() == 0) {
            return "Duration.ofHours(" + duration.toHours() + ")";
        } else if (seconds % Duration.ofMinutes(1).getSeconds() == 0) {
            return "Duration.ofMinutes(" + duration.toMinutes() + ")";
        }
        return "Duration.ofSeconds(" + seconds + ")";
    }

    private static Duration parseDuration(String duration) {
        Matcher matcher = DURATION_PATTERN.matcher(duration);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Failed to parse duration");
        }
        String unit = matcher.group(2);
        ChronoUnit chronoUnit = switch (unit) {
            case "s", "sec", "second", "seconds" -> ChronoUnit.SECONDS;
            case "m", "min", "minute", "minutes" -> ChronoUnit.MINUTES;
            case "h", "hour", "hours" -> ChronoUnit.HOURS;
            case "d", "day", "days" -> ChronoUnit.DAYS;
            default -> throw new IllegalArgumentException("Unknown duration unit type: " + unit);
        };
        return Duration.of(Integer.parseInt(matcher.group(1)), chronoUnit);
    }

    /**
     * Names the generated service uses for types: the short name for the first type claiming it, the full name for
     * the later types with the same short name. The types used by the generated code itself claim their short names
     * first, then the beans, the doer method parameters, the exception describers, the loaders and the savers.
     */
    private Map<String, String> createTypeShortcuts() {
        Map<String, String> shortcuts = new HashMap<>();
        Set<String> takenNames = new HashSet<>();
        TYPES_USED_IN_GENERATED_CODE.forEach(type -> claimShortName(type, shortcuts, takenNames));
        beanClassNames().forEach(type -> claimShortName(type, shortcuts, takenNames));
        doerMethods.forEach(m -> m.parameterTypes.forEach(type -> claimShortName(type, shortcuts, takenNames)));
        describers.forEach(d -> claimShortName(d.type(), shortcuts, takenNames));
        loaders.forEach(l -> claimShortName(l.type(), shortcuts, takenNames));
        savers.forEach(s -> claimShortName(s.type(), shortcuts, takenNames));
        return shortcuts;
    }

    /** Gives the type its short name when no other type has taken it yet, otherwise its full name. */
    private static void claimShortName(String type, Map<String, String> shortcuts, Set<String> takenNames) {
        if (!shortcuts.containsKey(type)) {
            String name = simpleName(type);
            shortcuts.put(type, takenNames.add(name) ? name : type);
        }
    }

    private Map<String, String> createFieldNames() {
        Elements elementUtils = processingEnv.getElementUtils();
        Map<String, String> fieldNames = new HashMap<>();
        beanClassNames().forEach(className -> {
            if (!fieldNames.containsKey(className)) {
                String simpleName = elementUtils.getTypeElement(className).getSimpleName().toString();
                fieldNames.put(className, createFieldName(simpleName, fieldNames));
            }
        });
        return fieldNames;
    }

    private String createFieldName(String simpleName, Map<String, String> fieldNames) {
        String name = simpleName.substring(0, 1).toLowerCase() + simpleName.substring(1);
        if (!fieldNames.containsValue(name) && !RESERVED_FIELD_NAMES.contains(name) && !SourceVersion.isKeyword(name)) {
            return name;
        }
        for (int i = 0; ; i++) {
            String varName = "var" + i;
            if (!fieldNames.containsValue(varName)) {
                return varName;
            }
        }
    }

    private static String simpleName(String className) {
        return className.substring(className.lastIndexOf('.') + 1);
    }

    /** Escapes a string for a Java, JSON or DOT string literal. */
    private static String escape(String s) {
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
    }

    private static String jstr(String s) {
        return s == null ? "null" : "\"" + escape(s) + "\"";
    }
}
