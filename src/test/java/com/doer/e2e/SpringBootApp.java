package com.doer.e2e;

import static com.doer.testkit.Toolchain.doerLibVersion;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Transit Sims in Spring Boot: an executable jar with Jersey, run with {@code java -jar} in a JRE image. What Spring does
 * differently from CDI is in {@code SpringBootMain.java}, written into the work folder only (see src/test/README.md).
 */
class SpringBootApp implements E2eApp {
    static final String SPRING_BOOT_VERSION = "4.1.1";
    static final String CDI_API_VERSION = "4.1.0";

    @Override
    public void configure(Path workdir) throws Exception {
        copySources("e2e/transitsims", workdir.resolve("src/main/java/transitsims"));
        copySources("e2e/db/migration", workdir.resolve("src/main/resources/db/migration"));
        configureSpringBootApp(workdir);
    }

    @Override
    public DockerRun dockerRun(Path workdir) {
        return springBootDockerRun(workdir);
    }

    void configureSpringBootApp(Path workdir) throws IOException {
        writeFile(workdir, "pom.xml", """
                <?xml version="1.0" encoding="UTF-8"?>
                <project xmlns="http://maven.apache.org/POM/4.0.0"
                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
                    <modelVersion>4.0.0</modelVersion>
                    <parent>
                        <groupId>org.springframework.boot</groupId>
                        <artifactId>spring-boot-starter-parent</artifactId>
                        <version>%1$s</version>
                        <relativePath/>
                    </parent>
                    <groupId>transitsims</groupId>
                    <artifactId>transitsims-spring-boot</artifactId>
                    <version>1.0.0-SNAPSHOT</version>

                    <properties>
                        <java.version>25</java.version>
                        <doer.version>%2$s</doer.version>
                    </properties>

                    <dependencies>
                        <dependency>
                            <groupId>org.springframework.boot</groupId>
                            <artifactId>spring-boot-starter-jersey</artifactId>
                            <exclusions>
                                <!-- Transit Sims uses JSON-B annotations -->
                                <exclusion>
                                    <groupId>org.glassfish.jersey.media</groupId>
                                    <artifactId>jersey-media-json-jackson</artifactId>
                                </exclusion>
                            </exclusions>
                        </dependency>
                        <dependency>
                            <groupId>org.glassfish.jersey.media</groupId>
                            <artifactId>jersey-media-json-binding</artifactId>
                        </dependency>
                        <dependency>
                            <groupId>org.glassfish.jersey.media</groupId>
                            <artifactId>jersey-media-json-processing</artifactId>
                        </dependency>
                        <dependency>
                            <groupId>org.springframework.boot</groupId>
                            <artifactId>spring-boot-starter-jdbc</artifactId>
                        </dependency>
                        <dependency>
                            <groupId>org.springframework.boot</groupId>
                            <artifactId>spring-boot-starter-flyway</artifactId>
                        </dependency>
                        <dependency>
                            <groupId>org.flywaydb</groupId>
                            <artifactId>flyway-database-postgresql</artifactId>
                        </dependency>
                        <dependency>
                            <groupId>org.postgresql</groupId>
                            <artifactId>postgresql</artifactId>
                            <scope>runtime</scope>
                        </dependency>
                        <dependency>
                            <!-- The APIs Transit Sims compiles against and Spring does not bring -->
                            <groupId>jakarta.enterprise</groupId>
                            <artifactId>jakarta.enterprise.cdi-api</artifactId>
                            <version>%3$s</version>
                        </dependency>
                        <dependency>
                            <groupId>jakarta.transaction</groupId>
                            <artifactId>jakarta.transaction-api</artifactId>
                        </dependency>
                        <dependency>
                            <groupId>com.java-doer</groupId>
                            <artifactId>doer</artifactId>
                            <version>${doer.version}</version>
                        </dependency>
                    </dependencies>

                    <build>
                        <finalName>transitsims</finalName>
                        <plugins>
                            <plugin>
                                <groupId>org.springframework.boot</groupId>
                                <artifactId>spring-boot-maven-plugin</artifactId>
                            </plugin>
                            <plugin>
                                <artifactId>maven-compiler-plugin</artifactId>
                                <configuration>
                                    <parameters>true</parameters>
                                    <annotationProcessorPaths>
                                        <path>
                                            <groupId>com.java-doer</groupId>
                                            <artifactId>doer</artifactId>
                                            <version>${doer.version}</version>
                                        </path>
                                    </annotationProcessorPaths>
                                </configuration>
                            </plugin>
                        </plugins>
                    </build>
                </project>
                """.formatted(SPRING_BOOT_VERSION, doerLibVersion, CDI_API_VERSION));
        writeFile(workdir, "src/main/resources/application.properties", """
                # The defaults are for a run from the IDE (doer.e2e.runtime=external)
                spring.datasource.url=${E2E_DB_URL:jdbc:postgresql://localhost:5432/doer}
                spring.datasource.username=${E2E_DB_USER:doer}
                spring.datasource.password=${E2E_DB_PASSWORD:doer}
                spring.datasource.hikari.maximum-pool-size=4
                # @ApplicationPath of TransitSimsApplication: Spring Boot reads the path from the ResourceConfig bean
                spring.jersey.application-path=/api
                server.port=8080
                logging.pattern.console=%d{yyyy-MM-dd HH:mm:ss.SSS} %p {%t} %logger{1} - %m%n
                """);
        writeFile(workdir, "src/main/java/transitsims/SpringBootMain.java", SPRING_BOOT_MAIN);
    }

    DockerRun springBootDockerRun(Path workdir) {
        return new DockerRun(List.of("-v", workdir.resolve("target/transitsims.jar") + ":/app/transitsims.jar:ro"),
                QuarkusApp.JRE_IMAGE, List.of("java", "-jar", "/app/transitsims.jar"));
    }

    /** Everything Spring does differently from CDI, so that Transit Sims stays free of Spring. */
    static final String SPRING_BOOT_MAIN = """
            package transitsims;

            import com.doer.DoerService;
            import com.doer.generated._GeneratedDoerService;
            import com.zaxxer.hikari.HikariDataSource;
            import jakarta.annotation.Priority;
            import jakarta.enterprise.context.ApplicationScoped;
            import jakarta.enterprise.context.Dependent;
            import jakarta.inject.Singleton;
            import jakarta.interceptor.AroundInvoke;
            import jakarta.interceptor.Interceptor;
            import jakarta.interceptor.InterceptorBinding;
            import jakarta.interceptor.InvocationContext;
            import jakarta.ws.rs.Path;
            import jakarta.ws.rs.ext.Provider;
            import java.lang.annotation.Annotation;
            import java.lang.reflect.Constructor;
            import java.lang.reflect.InvocationTargetException;
            import java.lang.reflect.Method;
            import java.lang.reflect.UndeclaredThrowableException;
            import java.util.ArrayList;
            import java.util.Comparator;
            import java.util.HashMap;
            import java.util.LinkedHashSet;
            import java.util.List;
            import java.util.Map;
            import java.util.Set;
            import java.util.concurrent.ExecutorService;
            import java.util.concurrent.Executors;
            import javax.sql.DataSource;
            import org.aopalliance.intercept.MethodInterceptor;
            import org.aopalliance.intercept.MethodInvocation;
            import org.glassfish.jersey.inject.hk2.AbstractBinder;
            import org.glassfish.jersey.server.ResourceConfig;
            import org.glassfish.jersey.server.ServerProperties;
            import org.springframework.aop.Advisor;
            import org.springframework.aop.support.AopUtils;
            import org.springframework.aop.support.StaticMethodMatcherPointcutAdvisor;
            import org.springframework.beans.factory.BeanFactory;
            import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
            import org.springframework.beans.factory.annotation.Qualifier;
            import org.springframework.beans.factory.config.BeanDefinition;
            import org.springframework.boot.SpringApplication;
            import org.springframework.boot.SpringBootConfiguration;
            import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
            import org.springframework.boot.context.properties.ConfigurationProperties;
            import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
            import org.springframework.context.ApplicationContext;
            import org.springframework.context.SmartLifecycle;
            import org.springframework.context.annotation.Bean;
            import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
            import org.springframework.context.annotation.ComponentScan;
            import org.springframework.context.annotation.FilterType;
            import org.springframework.context.annotation.Import;
            import org.springframework.context.annotation.Role;
            import org.springframework.context.annotation.Scope;
            import org.springframework.context.annotation.ScopeMetadata;
            import org.springframework.context.annotation.ScopeMetadataResolver;
            import org.springframework.core.type.filter.AnnotationTypeFilter;
            import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
            import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
            import org.springframework.util.ClassUtils;
            import org.springframework.util.ReflectionUtils;
            import transitsims.validation.BeanKindProduced;
            import transitsims.validation.BeanKindProducers;

            /** Transit Sims in Spring Boot: what Spring does differently from CDI. Written by the e2e suite (SpringBootApp). */
            @SpringBootConfiguration
            @EnableAutoConfiguration
            @Import(_GeneratedDoerService.class) // Spring does not know @ApplicationScoped
            @ComponentScan(basePackages = "transitsims", scopeResolver = SpringBootMain.CdiScopes.class,
                    includeFilters = @ComponentScan.Filter(type = FilterType.ANNOTATION,
                            classes = {ApplicationScoped.class, Dependent.class, Singleton.class, Interceptor.class}))
            public class SpringBootMain {

                public static void main(String[] args) {
                    SpringApplication.run(SpringBootMain.class, args);
                }

                /** @Dependent → prototype (one instance per injection point); @ApplicationScoped, @Singleton → singleton. */
                public static class CdiScopes implements ScopeMetadataResolver {
                    @Override
                    public ScopeMetadata resolveScopeMetadata(BeanDefinition definition) {
                        ScopeMetadata metadata = new ScopeMetadata();
                        if (definition instanceof AnnotatedBeanDefinition annotated
                                && annotated.getMetadata().hasAnnotation(Dependent.class.getName())) {
                            metadata.setScopeName(BeanDefinition.SCOPE_PROTOTYPE);
                        }
                        return metadata;
                    }
                }

                /** The pool of Spring Boot; injected by the qualifier only, so @Inject DataSource gets the proxy below. */
                @Bean(defaultCandidate = false)
                @ConfigurationProperties("spring.datasource.hikari")
                HikariDataSource hikariDataSource(DataSourceProperties properties) {
                    return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
                }

                /** On a plain DataSource, getConnection() does not join the Spring transaction. */
                @Bean
                DataSource dataSource(@Qualifier("hikariDataSource") HikariDataSource hikari) {
                    return new TransactionAwareDataSourceProxy(hikari);
                }

                /** The only Executor (the applicationTaskExecutor of Spring Boot backs off); shut down without waiting. */
                @Bean(destroyMethod = "shutdown")
                ExecutorService executor() {
                    return Executors.newCachedThreadPool(new CustomizableThreadFactory("doer-"));
                }

                /** Spring does not know @jakarta.enterprise.inject.Produces; no scope on the producer: @Dependent. */
                @Bean
                @Scope(BeanDefinition.SCOPE_PROTOTYPE)
                BeanKindProduced beanKindProduced(BeanKindProducers producers) {
                    return producers.produce();
                }

                /**
                 * Doer starts before Tomcat accepts requests and stops after Tomcat has stopped accepting them, before the
                 * DataSource and the Executor are destroyed: the phase is below the ones of the web server. Spring does not
                 * fire the CDI events Startup and Shutdown, so DoerLifecycle is never called.
                 */
                @Bean
                SmartLifecycle doerStartStop(DoerService doerService) {
                    return new SmartLifecycle() {
                        volatile boolean running;

                        @Override
                        public void start() {
                            doerService.start(true);
                            running = true;
                        }

                        @Override
                        public void stop() {
                            doerService.stop();
                            running = false;
                        }

                        @Override
                        public boolean isRunning() {
                            return running;
                        }

                        @Override
                        public int getPhase() {
                            return 0;
                        }
                    };
                }

                /**
                 * The JAX-RS resources and providers of Transit Sims (Jersey does not scan a Spring Boot jar). Jersey asks
                 * Spring only for classes with @Component, so a resource that is a Spring bean is bound to the bean.
                 */
                @Bean
                ResourceConfig resourceConfig(ApplicationContext context) {
                    ResourceConfig config = new ResourceConfig();
                    config.property(ServerProperties.WADL_FEATURE_DISABLE, true);
                    for (Class<?> type : scan(Path.class, Provider.class)) {
                        config.register(type);
                        if (context.getBeanNamesForType(type).length > 0) {
                            config.register(springBean(type, context));
                        }
                    }
                    return config;
                }

                static <T> AbstractBinder springBean(Class<T> type, ApplicationContext context) {
                    return new AbstractBinder() {
                        @Override
                        protected void configure() {
                            bindFactory(() -> context.getBean(type)).to(type);
                        }
                    };
                }

                static Set<Class<?>> scan(Class<? extends Annotation>... annotations) {
                    var scanner = new ClassPathScanningCandidateComponentProvider(false);
                    for (Class<? extends Annotation> annotation : annotations) {
                        scanner.addIncludeFilter(new AnnotationTypeFilter(annotation));
                    }
                    Set<Class<?>> types = new LinkedHashSet<>();
                    for (BeanDefinition candidate : scanner.findCandidateComponents("transitsims")) {
                        types.add(ClassUtils.resolveClassName(candidate.getBeanClassName(), SpringBootMain.class.getClassLoader()));
                    }
                    return types;
                }

                /**
                 * CDI interceptors: the methods with an interceptor binding of an @Interceptor class (on the method or its
                 * class) are called through its @AroundInvoke method, in the order of @Priority.
                 */
                @Bean
                @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
                static Advisor cdiInterceptors(BeanFactory beanFactory) {
                    return new CdiInterceptors(beanFactory);
                }

                static class CdiInterceptors extends StaticMethodMatcherPointcutAdvisor implements MethodInterceptor {
                    final BeanFactory beanFactory;
                    final Map<Class<? extends Annotation>, List<Class<?>>> interceptors = new HashMap<>();

                    CdiInterceptors(BeanFactory beanFactory) {
                        this.beanFactory = beanFactory;
                        for (Class<?> type : scan(Interceptor.class)) {
                            for (Annotation annotation : type.getAnnotations()) {
                                if (annotation.annotationType().isAnnotationPresent(InterceptorBinding.class)) {
                                    interceptors.computeIfAbsent(annotation.annotationType(), k -> new ArrayList<>()).add(type);
                                }
                            }
                        }
                        setAdvice(this);
                    }

                    @Override
                    public boolean matches(Method method, Class<?> targetClass) {
                        return !interceptorsOf(method, targetClass).isEmpty();
                    }

                    @Override
                    public Object invoke(MethodInvocation invocation) throws Throwable {
                        List<Object> chain = new ArrayList<>();
                        for (Class<?> type : interceptorsOf(invocation.getMethod(), invocation.getThis().getClass())) {
                            chain.add(beanFactory.getBean(type));
                        }
                        return new CdiInvocationContext(invocation, chain, 0).proceed();
                    }

                    List<Class<?>> interceptorsOf(Method method, Class<?> targetClass) {
                        if (targetClass.isAnnotationPresent(Interceptor.class)) {
                            return List.of();
                        }
                        Set<Class<?>> found = new LinkedHashSet<>();
                        Method specific = AopUtils.getMostSpecificMethod(method, targetClass);
                        for (Annotation[] annotations : List.of(targetClass.getAnnotations(), specific.getAnnotations())) {
                            for (Annotation annotation : annotations) {
                                found.addAll(interceptors.getOrDefault(annotation.annotationType(), List.of()));
                            }
                        }
                        return found.stream().sorted(Comparator.comparingInt(CdiInterceptors::priority)).toList();
                    }

                    static int priority(Class<?> type) {
                        Priority priority = type.getAnnotation(Priority.class);
                        return priority == null ? 0 : priority.value();
                    }
                }

                /** InvocationContext of the interceptor at index; proceed() calls the next one, or the method after the last. */
                record CdiInvocationContext(MethodInvocation invocation, List<Object> chain, int index,
                        Map<String, Object> contextData) implements InvocationContext {

                    CdiInvocationContext(MethodInvocation invocation, List<Object> chain, int index) {
                        this(invocation, chain, index, new HashMap<>());
                    }

                    @Override
                    public Object proceed() throws Exception {
                        try {
                            if (index == chain.size()) {
                                return invocation.proceed();
                            }
                            Object interceptor = chain.get(index);
                            Method aroundInvoke = aroundInvoke(interceptor.getClass());
                            return aroundInvoke.invoke(interceptor,
                                    new CdiInvocationContext(invocation, chain, index + 1, contextData));
                        } catch (InvocationTargetException e) {
                            throw rethrow(e.getCause());
                        } catch (Throwable e) {
                            throw rethrow(e);
                        }
                    }

                    static Exception rethrow(Throwable e) {
                        if (e instanceof Exception exception) {
                            return exception;
                        }
                        if (e instanceof Error error) {
                            throw error;
                        }
                        return new UndeclaredThrowableException(e);
                    }

                    static Method aroundInvoke(Class<?> type) {
                        List<Method> methods = new ArrayList<>();
                        ReflectionUtils.doWithMethods(type, methods::add, m -> m.isAnnotationPresent(AroundInvoke.class));
                        Method method = methods.getFirst();
                        ReflectionUtils.makeAccessible(method);
                        return method;
                    }

                    @Override
                    public Object getTarget() {
                        return invocation.getThis();
                    }

                    @Override
                    public Object getTimer() {
                        return null;
                    }

                    @Override
                    public Method getMethod() {
                        return invocation.getMethod();
                    }

                    @Override
                    public Constructor<?> getConstructor() {
                        return null;
                    }

                    @Override
                    public Object[] getParameters() {
                        return invocation.getArguments();
                    }

                    @Override
                    public void setParameters(Object[] params) {
                        System.arraycopy(params, 0, invocation.getArguments(), 0, params.length);
                    }

                    @Override
                    public Map<String, Object> getContextData() {
                        return contextData;
                    }
                }
            }
            """;
}
