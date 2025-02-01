package com.doer.processor;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
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
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.Diagnostic.Kind;

import javax.tools.FileObject;

import com.doer.*;
import com.google.auto.service.AutoService;

@SupportedAnnotationTypes({ "com.doer.*" })
@AutoService(Processor.class)
public class DoerProcessor extends AbstractProcessor {

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
                List<DoerMethodInfo> doerMethods = loadDoerMethods(roundEnv);
                List<DoerLoaderInfo> loaders = loadLoaders(roundEnv);
                List<DoerUnloaderInfo> unloaders = loadUnloaders(roundEnv);
                List<DoerExtraJsonInfo> appenders = loadExtraJsonAppenders(roundEnv);
                Map<String, Integer> concurrencies = loadDoerConcurrencies(roundEnv);

                generateDoerService(doerMethods, loaders, unloaders, appenders, concurrencies);

                generateCreateSchemaSql();
                generateSelectTaskSql(doerMethods, concurrencies);
                generateCreateIndexSql(doerMethods);

                SetStatusFinder finder = new SetStatusFinder(roundEnv, processingEnv);
                finder.updateDoerMethods(doerMethods);

                generateDoerJson(doerMethods, loaders, unloaders, appenders, concurrencies);
                generateDoerDot(doerMethods);

                return true;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return false;
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

    private List<DoerMethodInfo> loadDoerMethods(RoundEnvironment roundEnv) {
        List<DoerMethodInfo> result = new ArrayList<>();
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
                info.acceptList.add(annotation);
            }
            OnException[] onExceptions = element.getAnnotationsByType(OnException.class);
            info.onException = (onExceptions.length == 0 ? null : onExceptions[0]);
            DoerConcurrency[] concurrencies = element.getAnnotationsByType(DoerConcurrency.class);
            info.concurrency = (concurrencies.length == 0 ? null : concurrencies[0]);
            info.element = element;
            result.add(info);
        }

        return result;
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

    private List<DoerLoaderInfo> loadLoaders(RoundEnvironment roundEnv) {
        Messager messager = processingEnv.getMessager();
        List<DoerLoaderInfo> result = new ArrayList<>();
        Set<Element> elements = new HashSet<>();
        elements.addAll(roundEnv.getElementsAnnotatedWith(DoerLoader.class));
        for (Element element : elements) {
            if (element.getKind() != ElementKind.METHOD) {
                messager.printMessage(Kind.ERROR,
                        "DoerParameterLoader annotation can be used only on public methods.", element);
                continue;
            }
            ExecutableType executableType = (ExecutableType) element.asType();
            List<? extends TypeMirror> args = executableType.getParameterTypes();
            if (args.size() != 1 || !Task.class.getName().equals(args.get(0).toString())) {
                messager.printMessage(Kind.ERROR, "DoerParameterLoader should have exactly 1 argument of type " +
                        Task.class.getName(), element);
                continue;
            }
            DoerLoaderInfo info = new DoerLoaderInfo();
            info.className = element.getEnclosingElement().asType().toString();
            checkUnnamedPackageError(element, info.className);
            info.methodName = element.getSimpleName().toString();
            info.type = ((ExecutableType) element.asType()).getReturnType().toString();
            // ((TypeElement)((DeclaredType)args.get(0)).asElement()).getQualifiedName()
            result.add(info);
        }
        return result;
    }

    private List<DoerUnloaderInfo> loadUnloaders(RoundEnvironment roundEnv) {
        Messager messager = processingEnv.getMessager();
        List<DoerUnloaderInfo> result = new ArrayList<>();
        Set<Element> elements = new HashSet<>();
        elements.addAll(roundEnv.getElementsAnnotatedWith(DoerUnloader.class));
        for (Element element : elements) {
            if (element.getKind() != ElementKind.METHOD) {
                messager.printMessage(Kind.ERROR,
                        DoerUnloader.class.getName() + " annotation can be used only on public methods.", element);
                continue;
            }
            ExecutableType executableType = (ExecutableType) element.asType();
            List<? extends TypeMirror> args = executableType.getParameterTypes();
            if (args.size() != 2 || !Task.class.getName().equals(args.get(0).toString())
                    || ((ExecutableType) element.asType()).getReturnType().getKind() != TypeKind.VOID) {
                messager.printMessage(Kind.ERROR,
                        DoerUnloader.class.getName()
                                + " should have exactly 2 arguments Task and unloading parameter, and should return void.",
                        element);
                continue;
            }
            DoerUnloaderInfo info = new DoerUnloaderInfo();
            info.className = element.getEnclosingElement().asType().toString();
            checkUnnamedPackageError(element, info.className);
            info.methodName = element.getSimpleName().toString();
            info.type = args.get(1).toString();
            result.add(info);
        }
        return result;
    }

    private List<DoerExtraJsonInfo> loadExtraJsonAppenders(RoundEnvironment roundEnv) {
        Messager messager = processingEnv.getMessager();
        Types types = processingEnv.getTypeUtils();
        List<DoerExtraJsonInfo> result = new ArrayList<>();
        Set<Element> elements = new HashSet<>();
        elements.addAll(roundEnv.getElementsAnnotatedWith(DoerExtraJson.class));
        for (Element element : elements) {
            if (element.getKind() != ElementKind.METHOD) {
                messager.printMessage(Kind.ERROR,
                        DoerExtraJson.class.getName() + " annotation can be used only on public methods.", element);
                continue;
            }
            ExecutableType executableType = (ExecutableType) element.asType();
            List<? extends TypeMirror> args = executableType.getParameterTypes();

            String jsonObjectBuilder = getJakartaPrefix() + ".json.JsonObjectBuilder";
            if (args.size() != 3 || !Task.class.getName().equals(args.get(0).toString())
                    || !jsonObjectBuilder.equals(args.get(2).toString())
                    || ((ExecutableType) element.asType()).getReturnType().getKind() != TypeKind.VOID) {
                messager.printMessage(Kind.ERROR,
                        DoerExtraJson.class.getName()
                                + " should mark void method that have exactly 3 arguments: Task, Exception and JsonObjectBuilder\n"
                                + "Example:\n"
                                + "@" + DoerExtraJson.class.getName() + "\n"
                                + "public void myAppender(Task task, Exception e, JsonObjectBuilder builder) {\n"
                                + "}",
                        element);
                continue;
            }
            TypeMirror exType = args.get(1);
            DoerExtraJsonInfo info = new DoerExtraJsonInfo();
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
                messager.printMessage(Kind.ERROR, "Second parameter of @" + DoerExtraJson.class.getSimpleName()
                        + " annotated method " + info.methodName + " should be of Throwable type", element);
            }
            result.add(info);
        }

        HashSet<String> appenderTypes = new HashSet<>();
        for (DoerExtraJsonInfo info : result) {
            appenderTypes.add(info.type);
        }
        for (DoerExtraJsonInfo info : result) {
            Iterator<String> iterator = info.typeParents.iterator();
            while (iterator.hasNext()) {
                if (!appenderTypes.contains(iterator.next())) {
                    iterator.remove();
                }
            }
        }
        // Base classes comes first, then alphabetically ordered by class name
        Collections.sort(result, (a, b) -> {
            if (a.typeParents.contains(b.type)) {
                return 1;
            } else if (b.typeParents.contains(a.type)) {
                return -1;
            } else {
                return a.type.compareTo(b.type);
            }
        });
        return result;
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

    private Map<String, Integer> loadDoerConcurrencies(RoundEnvironment roundEnv) {
        Map<String, Integer> result = new HashMap<>();
        Messager messager = processingEnv.getMessager();
        Set<Element> elements = new HashSet<>();
        elements.addAll(roundEnv.getElementsAnnotatedWith(DoerConcurrency.class));
        for (Element element : elements) {
            String domainName;
            if (element.getKind() == ElementKind.METHOD) {
                domainName = element.getEnclosingElement().asType().toString() + "."
                        + element.getSimpleName().toString();
            } else if (element.getKind() == ElementKind.CLASS) {
                domainName = element.asType().toString();
            } else {
                messager.printMessage(Kind.WARNING,
                        DoerConcurrency.class.getName() + " annotation can be used only on public methods or classes.",
                        element);
                continue;
            }
            DoerConcurrency[] arr = element.getAnnotationsByType(DoerConcurrency.class);
            Integer value = arr[0].value();
            result.put(domainName, value);
        }
        return result;
    }

    void generateDoerService(List<DoerMethodInfo> doerMethods, List<DoerLoaderInfo> loaders,
            List<DoerUnloaderInfo> unloaders, List<DoerExtraJsonInfo> appenders, Map<String, Integer> concurrencies)
            throws IOException {
        HashMap<String, String> shortcuts = createTypeShortcuts(doerMethods, loaders, unloaders, appenders);
        HashMap<String, String> fieldNames = createFieldNames(doerMethods, loaders, unloaders, appenders);

        Stream<String> classes1 = doerMethods.stream().map(s -> s.className);
        Stream<String> classes2 = loaders.stream().map(s -> s.className);
        Stream<String> classes3 = unloaders.stream().map(s -> s.className);
        Stream<String> classes4 = appenders.stream().map(s -> s.className);
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
            out.println("    DataSource dataSource;");
            for (String bean : beans) {
                out.println("    " + shortcuts.get(bean) + " " + fieldNames.get(bean) + ";");
            }
            out.println();
            out.println("    public _GeneratedDoerService() {");
            out.println("        super();");
            out.println("        initializeDomains();");
            out.println("    }");
            out.println();
            out.println("    @Inject");
            out.println("    public void _inject_SelfReference(DoerService self) {");
            out.println("        this.self = self;");
            out.println("    }");
            out.println();
            out.println("    @Inject");
            out.println("    public void _inject_DataSource(DataSource dataSource) {");
            out.println("        this.dataSource = dataSource;");
            out.println("    }");
            out.println();
            out.println("    @Inject");
            out.println("    public void setExecutor(Executor executor) {");
            out.println("        this.executor = executor;");
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
            out.println("    @Transactional(Transactional.TxType.REQUIRES_NEW)");
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
            out.println("    public Connection getConnection() throws SQLException {");
            out.println("        return dataSource.getConnection();");
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
            out.println("            logWarning(\"ExtraJson creation error\", e);");
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
            for (DoerExtraJsonInfo appender : appenders) {
                String exceptionType = shortcuts.get(appender.type);
                String fieldName = fieldNames.get(appender.className);
                out.println("        if (exception instanceof " + exceptionType + ") {");
                out.println("            " + fieldName + "." + appender.methodName + "(task, (" + exceptionType
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
            out.println("    protected Object _callLoader(Class<?> klazz, Task task) throws Exception {");
            for (DoerLoaderInfo loader : loaders) {
                out.println("        if (" + shortcuts.get(loader.type) + ".class.equals(klazz)) {");
                out.println("            return " + fieldNames.get(loader.className) + "." + loader.methodName + "(task);");
                out.println("        }");
            }
            out.println("        return null;");
            out.println("    }");
            out.println();
            out.println("    @Override");
            out.println("    protected void _callUnLoader(Class<?> klazz, Task task, Object data) throws Exception {");
            for (DoerUnloaderInfo unloader : unloaders) {
                out.println("        if (" + shortcuts.get(unloader.type) + ".class.equals(klazz)) {");
                out.println("            " + fieldNames.get(unloader.className) + "." + unloader.methodName + "(task, ("
                        + shortcuts.get(unloader.type) + ")data);");
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

                String onExceptionTimeoutLiteral;
                String onExceptionStatusLiteral;
                if (info.onException != null) {
                    onExceptionTimeoutLiteral = createDurationLiteral(parseRetryDuration(info.onException.retry()));
                    onExceptionStatusLiteral = "\"" + escape(info.onException.setStatus()) + "\"";
                } else {
                    onExceptionTimeoutLiteral = "null";
                    onExceptionStatusLiteral = "null";
                }
                out.println("            callDoerMethod(task, () -> {");
                for (int paramIndex = 0; paramIndex < info.parameterTypes.size(); paramIndex++) {
                    String paramClass = info.parameterTypes.get(paramIndex);
                    if (!paramClass.equals(Task.class.getName())) {
                        DoerLoaderInfo loader = loaders.stream()
                                .filter(l -> l.type.equals(paramClass))
                                .findFirst()
                                .orElse(null);
                        if (loader == null) {
                            if (!missingLoadersReported.contains(paramClass)) {
                                missingLoadersReported.add(paramClass);
                                Messager messager = processingEnv.getMessager();
                                messager.printMessage(Kind.ERROR,
                                        "No " + DoerLoader.class.getSimpleName() + " found for argument " + paramIndex
                                                + "\n" +
                                                "Please declare loader method:\n" +
                                                "@" + DoerLoader.class.getName() + "\n" +
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
                        DoerUnloaderInfo unloader = unloaders.stream()
                                .filter(l -> l.type.equals(paramClass))
                                .findFirst()
                                .orElse(null);
                        if (unloader != null) {
                            String unloaderBean = fieldNames.get(unloader.className);
                            out.println("                    " + unloaderBean + "." + unloader.methodName +
                                    "(task, (" + shortcuts.get(paramClass) + ")args[" + paramIndex + "]);");
                        }
                    }
                }

                out.println("                    return null;");
                out.println("                }, \"" + shortClassName + "\", \"" + info.methodName + "\",");
                out.println("                    " + onExceptionTimeoutLiteral + ", " +
                        onExceptionStatusLiteral + ");");
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
                byRetryDelay.remove(Duration.ofMinutes(5));
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
                        + concurrencies.getOrDefault(domainName, 2)
                        + ", delays, retryDelays);");
                out.println("        }");
            }
            out.println("    }");
            out.println();
            out.println("}");
        }
    }

    void generateSelectTaskSql(List<DoerMethodInfo> doerMethods, Map<String, Integer> concurrencies)
            throws IOException {
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
                    Duration retryDuration;
                    if (method.onException == null) {
                        retryDuration = Duration.ofMinutes(5);
                    } else {
                        retryDuration = parseRetryInterval(method.onException.retry());
                    }
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

    private Map<String, List<DoerMethodInfo>> groupMethodsByDomain(List<DoerMethodInfo> doerMethods) {
        Map<String, List<DoerMethodInfo>> domains = new HashMap<>();
        for (DoerMethodInfo method : doerMethods) {
            if (method.concurrency != null) {
                List<DoerMethodInfo> list = new ArrayList<>();
                list.add(method);
                domains.put(method.className + "." + method.methodName, list);
            } else {
                if (!domains.containsKey(method.className)) {
                    domains.put(method.className, new ArrayList<>());
                }
                domains.get(method.className).add(method);
            }
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
            Duration retryDuration;
            if (method.onException == null) {
                retryDuration = Duration.ofMinutes(5);
            } else {
                retryDuration = parseRetryInterval(method.onException.retry());
            }
            if (!result.containsKey(retryDuration)) {
                result.put(retryDuration, new ArrayList<>());
            }
            for (AcceptStatus annotation : method.acceptList) {
                result.get(retryDuration).add(annotation.value());
            }
        }
        return result;
    }

    private void generateCreateIndexSql(List<DoerMethodInfo> doerMethods) throws IOException {
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
            out.println("    status VARCHAR(50),");
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

    private void generateDoerJson(List<DoerMethodInfo> doerMethods, List<DoerLoaderInfo> loaders,
            List<DoerUnloaderInfo> unloaders, List<DoerExtraJsonInfo> appenders, Map<String, Integer> concurrencies) throws IOException {
        FileObject doerJson = processingEnv.getFiler()
                .createResource(StandardLocation.CLASS_OUTPUT, "com.doer.generated", "doer.json");
        try (PrintWriter out = new PrintWriter(doerJson.openWriter())) {
            out.println("{");
            out.println("    \"generator\": \"" + getClass().getName() + "\",");
            out.println("    \"generated\": \"" + Instant.now() + "\",");
            out.println("    \"domains\": [");
            List<String> domainNames = new ArrayList<>(concurrencies.keySet());
            Collections.sort(domainNames);
            for (String name : domainNames) {
                boolean last = name.equals(domainNames.get(domainNames.size() - 1));
                Integer n = concurrencies.get(name);
                out.printf("        {%s: %s, %s: %s}%s%n", jstr("concurrency"), n, jstr("name"), jstr(name), 
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
                if (method.concurrency != null) {
                    out.printf("            %s: %s,%n", jstr("concurrency"), method.concurrency.value());
                }
                out.printf("            %s: %s, %s: %s,", jstr("class"), jstr(method.className), jstr("method"),
                        jstr(method.methodName));
                if(method.onException != null) {
                    out.printf(" %s: %s, %s: %s,%n",
                            jstr("retry"), jstr(method.onException.retry()),
                            jstr("error_status"), jstr(method.onException.setStatus()));
                } else {
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
            HashMap<String, DoerLoaderInfo> loaderMap = new HashMap<>();
            for (DoerLoaderInfo info : loaders) {
                loaderTypes.add(info.type);
                loaderMap.put(info.type, info);
            }
            Collections.sort(loaderTypes);
            for (int i = 0; i < loaderTypes.size(); i++) {
                DoerLoaderInfo loader = loaderMap.get(loaderTypes.get(i));
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
            out.println("    \"unloaders\": [");

            List<String> unLoaderTypes = new ArrayList<>();
            HashMap<String, DoerUnloaderInfo> unLoaderMap = new HashMap<>();
            for (DoerUnloaderInfo info : unloaders) {
                unLoaderTypes.add(info.type);
                unLoaderMap.put(info.type, info);
            }
            Collections.sort(unLoaderTypes);
            for (int i = 0; i < unLoaderTypes.size(); i++) {
                DoerUnloaderInfo unLoader = unLoaderMap.get(unLoaderTypes.get(i));
                out.printf("        {%s: %s, %s: %s, %s: %s}",
                        jstr("type"), jstr(unLoader.type),
                        jstr("class"), jstr(unLoader.className),
                        jstr("method"), jstr(unLoader.methodName));
                if (i >= unLoaderTypes.size() - 1) {
                    out.println();
                } else {
                    out.println(",");
                }
            }
            out.println("    ],");
            out.println("    \"extra_json_appenders\": [");
            for (int i = 0; i < appenders.size(); i++) {
                DoerExtraJsonInfo appender = appenders.get(i);
                out.printf("        {%s: %s, %s: %s, %s: %s}",
                        jstr("type"), jstr(appender.type),
                        jstr("class"), jstr(appender.className),
                        jstr("method"), jstr(appender.methodName));
                if (i >= appenders.size() - 1) {
                    out.println();
                } else {
                    out.println(",");
                }
            }
            out.println("    ]");
            out.println("}");
        }
    }

    private void generateDoerDot(List<DoerMethodInfo> doerMethods) throws IOException {
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
                if (method.onException != null) {
                    onerror.add(method.onException.setStatus());
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
                if (method.emitList.contains(null)) {
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
                Collections.sort(emitList);
                for (String status : emitList) {
                    String statusNodeName = (status != null ? statusNodeNames.get(status) :
                            terminationStatusNodeNames.get(method));
                    out.printf("%s -> %s;%n", methodNodeName, statusNodeName);
                }
                if (method.onException != null) {
                    String status = method.onException.setStatus();
                    String statusNodeName = statusNodeNames.get(status);
                    String toolTip = "[after retry] " + method.onException.retry();
                    out.printf("%s -> %s[color=\"red\", tooltip=\"%s\"];%n",
                            methodNodeName, statusNodeName, escape(toolTip));
                }
            }
            out.println("}");
        }
    }

    protected Duration parseRetryInterval(String string) {
        Pattern RETRY_PATTERN = Pattern.compile("^\\s*every\\s+(\\d+)\\s*(\\w+)(\\s+during\\s+(\\d+)\\s*(\\w+))?\\s*$");
        Matcher matcher = RETRY_PATTERN.matcher(string);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Failed to parse retry expression");
        }
        return parseDuration(matcher.group(1) + " " + matcher.group(2));
    }

    protected Duration parseRetryDuration(String string) {
        Pattern RETRY_PATTERN = Pattern.compile("^\\s*every\\s+(\\d+)\\s*(\\w+)(\\s+during\\s+(\\d+)\\s*(\\w+))?\\s*$");
        Matcher matcher = RETRY_PATTERN.matcher(string);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Failed to parse retry expression");
        }
        if (matcher.group(4) == null) {
            return null;
        }
        return parseDuration(matcher.group(4) + " " + matcher.group(5));
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
        Matcher matcher = Pattern.compile("^(\\d+)\\s*(\\w+)$")
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

    private HashMap<String, String> createTypeShortcuts(List<DoerMethodInfo> doerMethods, List<DoerLoaderInfo> loaders,
            List<DoerUnloaderInfo> unloaders, List<DoerExtraJsonInfo> appenders) {
        Elements elementUtils = processingEnv.getElementUtils();
        HashMap<String, String> shortnames = new HashMap<>();
        shortnames.put("com.doer.Task", "Task");
        shortnames.put("com.doer.DoerService", "DoerService");
        shortnames.put("java.lang.Override", "Override");
        shortnames.put("java.lang.Exception", "Exception");
        shortnames.put("java.lang.Throwable", "Throwable");
        String javaEePackage = getJakartaPrefix();
        shortnames.put(javaEePackage + ".transaction.Transactional", "Transactional");
        shortnames.put(javaEePackage + ".inject.Inject", "Inject");
        shortnames.put(javaEePackage + ".enterprise.context.ApplicationScoped", "ApplicationScoped");
        shortnames.put(javaEePackage + ".annotation.Generated", "Generated");
        shortnames.put(javaEePackage + ".json.JsonObjectBuilder", "JsonObjectBuilder");
        shortnames.put(javaEePackage + ".json.JsonObject", "JsonObject");
        shortnames.put(javaEePackage + ".json.JsonArrayBuilder", "JsonArrayBuilder");
        shortnames.put(javaEePackage + ".json.JsonArray", "JsonArray");
        shortnames.put(javaEePackage + ".json.Json", "Json");
        shortnames.put(javaEePackage + ".json.JsonWriterFactory", "JsonWriterFactory");
        shortnames.put(javaEePackage + ".json.JsonWriter", "JsonWriter");
        shortnames.put(javaEePackage + ".json.stream.JsonGenerator", "JsonGenerator");

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

        Stream<String> classes1 = doerMethods.stream().map(s -> s.className);
        Stream<String> classes2 = loaders.stream().map(s -> s.className);
        Stream<String> classes3 = unloaders.stream().map(s -> s.className);
        Stream<String> classes4 = doerMethods.stream().flatMap(s -> s.parameterTypes.stream());
        Stream<String> classes5 = appenders.stream().flatMap(s -> Stream.of(s.type, s.className));
        Stream.of(classes1, classes2, classes3, classes4, classes5).flatMap(i -> i).forEach(cn -> {
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

    private String getJakartaPrefix() {
        try {
            Class.forName("jakarta.inject.Inject");
            return "jakarta";
        } catch (ClassNotFoundException e) {
            try {
                Class.forName("javax.inject.Inject");
                return "javax";
            } catch (ClassNotFoundException e2) {
                return "jakarta";
            }
        }
    }

    private HashMap<String, String> createFieldNames(List<DoerMethodInfo> doerMethods, List<DoerLoaderInfo> loaders,
            List<DoerUnloaderInfo> unloaders, List<DoerExtraJsonInfo> appenders) {
        Elements elementUtils = processingEnv.getElementUtils();
        HashMap<String, String> fieldNames = new HashMap<>();
        Stream<String> classes1 = doerMethods.stream().map(s -> s.className);
        Stream<String> classes2 = loaders.stream().map(s -> s.className);
        Stream<String> classes3 = unloaders.stream().map(s -> s.className);
        Stream<String> classes4 = appenders.stream().map(s -> s.className);
        Stream.of(classes1, classes2, classes3, classes4).flatMap(i -> i).forEach(cn -> {
            if (!fieldNames.containsKey(cn)) {
                TypeElement element = elementUtils.getTypeElement(cn);
                fieldNames.put(cn, createFieldName(element.getSimpleName().toString(), fieldNames));
            }
        });
        return fieldNames;
    }

    private String createFieldName(String shortName, HashMap<String, String> fieldNames) {
        List<String> notAllowedNames = Arrays.asList("task", "status", "args", "dataSource");
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
