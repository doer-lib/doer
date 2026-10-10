package com.doer.generatedcode;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.restassured.path.json.JsonPath;
import org.junit.jupiter.api.Test;

/**
 * What {@code runTask} of the generated service does with a task: calls the doer method with its parameters, the
 * loaders before it and the savers after it, and sets the status and failingSince after a failure. Each check sends
 * a request to {@code transitsims.validation.TaskRunner} and compares its response. The doer methods are the
 * GeneratedCode* classes of {@code transitsims.validation}. See e2e-test-app.md.
 * <p>
 * The same checks run on Transit Sims built and run in different ways; the implementing class says how:
 * <ul>
 * <li>{@link JavacGeneratedCodeITCase}, {@link MavenGeneratedCodeITCase} — built by javac or Maven and run in its own
 * JVM ({@link TransitSimsProcess}), with every JDK of the matrix;</li>
 * <li>{@link GeneratedCodeE2E} — deployed in a runtime, called over REST, with one JDK per runtime.</li>
 * </ul>
 */
public interface GeneratedCodeTest {

    /** Sends the JSON request to TaskRunner and returns its JSON response as it is. */
    String callTaskRunner(String request) throws Exception;

    /** Runs the task through TaskRunner and returns its response pretty-printed, to compare with a text block. */
    default String runTask(String request) throws Exception {
        return JsonPath.from(callTaskRunner(request)).prettify();
    }

    @Test
    default void method_with_1_param_should_be_called() throws Exception {
        String result = runTask("""
                {"status": "Generated code 1 param"}
                """);

        assertEquals("""
                {
                    "status": "Generated code 1 param done",
                    "failing": false,
                    "trace": [
                        "GeneratedCodeMethods.oneParam"
                    ]
                }""", result);
    }

    @Test
    default void method_with_2_params_should_get_data_from_loader_and_give_it_to_saver() throws Exception {
        String result = runTask("""
                {"status": "Generated code 2 params"}
                """);

        assertEquals("""
                {
                    "status": "Generated code 2 params done",
                    "failing": false,
                    "trace": [
                        "GeneratedCodeTaskData.loadFirst",
                        "GeneratedCodeMethods.twoParams(First[loadFirst])",
                        "GeneratedCodeTaskData.saveFirst(First[loadFirst, twoParams])"
                    ]
                }""", result);
    }

    @Test
    default void method_with_3_params_should_get_data_from_2_loaders_in_different_beans() throws Exception {
        String result = runTask("""
                {"status": "Generated code 3 params"}
                """);

        assertEquals("""
                {
                    "status": "Generated code 3 params done",
                    "failing": false,
                    "trace": [
                        "GeneratedCodeTaskData.loadFirst",
                        "GeneratedCodeMethods.loadSecond",
                        "GeneratedCodeMethods.threeParams(First[loadFirst], Second[loadSecond])",
                        "GeneratedCodeMethods.saveSecond(Second[loadSecond, threeParams])",
                        "GeneratedCodeTaskData.saveFirst(First[loadFirst, threeParams])"
                    ]
                }""", result);
    }

    @Test
    default void checked_exception_should_set_failing_and_restore_status_without_saving() throws Exception {
        String result = runTask("""
                {"status": "Generated code checked exception"}
                """);

        assertEquals("""
                {
                    "status": "Generated code checked exception",
                    "failing": true,
                    "trace": [
                        "GeneratedCodeTaskData.loadFirst",
                        "GeneratedCodeFailures.checkedException(First[loadFirst])"
                    ]
                }""", result);
    }

    @Test
    default void runtime_exception_should_set_failing_and_restore_status() throws Exception {
        String result = runTask("""
                {"status": "Generated code runtime exception"}
                """);

        assertEquals("""
                {
                    "status": "Generated code runtime exception",
                    "failing": true,
                    "trace": [
                        "GeneratedCodeFailures.runtimeException"
                    ]
                }""", result);
    }

    @Test
    default void method_failing_less_than_1_day_should_keep_status() throws Exception {
        String result = runTask("""
                {"status": "Generated code runtime exception", "failingFor": "PT23H"}
                """);

        assertEquals("""
                {
                    "status": "Generated code runtime exception",
                    "failing": true,
                    "trace": [
                        "GeneratedCodeFailures.runtimeException"
                    ]
                }""", result);
    }

    @Test
    default void method_failing_more_than_1_day_should_set_status_to_null() throws Exception {
        String result = runTask("""
                {"status": "Generated code runtime exception", "failingFor": "PT25H"}
                """);

        assertEquals("""
                {
                    "status": null,
                    "failing": false,
                    "trace": [
                        "GeneratedCodeFailures.runtimeException"
                    ]
                }""", result);
    }

    @Test
    default void method_failing_less_than_retry_duration_should_keep_status() throws Exception {
        String result = runTask("""
                {"status": "Generated code retry policy", "failingFor": "PT50M"}
                """);

        assertEquals("""
                {
                    "status": "Generated code retry policy",
                    "failing": true,
                    "trace": [
                        "GeneratedCodeFailures.retryPolicy"
                    ]
                }""", result);
    }

    @Test
    default void method_failing_more_than_retry_duration_should_set_fallback_status() throws Exception {
        String result = runTask("""
                {"status": "Generated code retry policy", "failingFor": "PT70M"}
                """);

        assertEquals("""
                {
                    "status": "Generated code retry policy fallback",
                    "failing": false,
                    "trace": [
                        "GeneratedCodeFailures.retryPolicy"
                    ]
                }""", result);
    }
}
