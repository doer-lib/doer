package com.doer.processor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.io.CleanupMode.NEVER;

import com.doer.testkit.InWorkspace;
import com.doer.testkit.Workspaces;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Errors and warnings reported by DoerProcessor: the javac messages. */
public class GeneratorErrorsITCase implements InWorkspace {

    @TempDir(factory = Workspaces.class, cleanup = NEVER)
    Path workspace;

    @Override
    public Path getWorkspace() {
        return workspace;
    }

    @Test
    void AcceptStatus__should_fail_on_invalid_delay() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @AcceptStatus(value = "A", delay = "soon")
                    public void accept(Task task) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertEquals("""
                Orders.java:5: error: @AcceptStatus delay "soon" is not a duration. Expected a number and a unit, e.g. "5s", "10 min", "2h", "1 day".
                    public void accept(Task task) {}
                                ^
                1 error
                """, result.stdErr());
    }

    @Test
    void AcceptStatus__should_fail_on_zero_delay() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @AcceptStatus("A")
                    @AcceptStatus(value = "B", delay = "0s")
                    public void accept(Task task) {}

                    @AcceptStatus(value = "C", delay = "0 min")
                    public void pay(Task task) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        // the order of the methods is up to javac
        assertThat(result.stdErr(), allOf(
                containsString("""
                        Orders.java:6: error: @AcceptStatus delay "0s" must be greater than zero. Remove delay to process the task as soon as possible.
                            public void accept(Task task) {}
                        """),
                containsString("""
                        Orders.java:9: error: @AcceptStatus delay "0 min" must be greater than zero. Remove delay to process the task as soon as possible.
                            public void pay(Task task) {}
                        """),
                endsWith("2 errors\n")));
    }

    @Test
    void AcceptStatus__should_fail_on_invalid_values() throws Exception {
        String longStatus = "S".repeat(51);
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @AcceptStatus("")
                    @RetryPolicy(interval = "5m")
                    public void empty(Task task) {}

                    @AcceptStatus(" A")
                    @RetryPolicy(interval = "5m")
                    public void whitespace(Task task) {}

                    @AcceptStatus("%s")
                    @RetryPolicy(interval = "5m")
                    public void tooLong(Task task) {}

                    @AcceptStatus("C")
                    @RetryPolicy(interval = "5m", duration = "1h", fallbackStatus = "B ")
                    public void fallback(Task task) {}
                }
                """.formatted(longStatus));

        var result = javac("Orders.java").assertStatus(1);

        assertThat(result.stdErr(), allOf(
                containsString("@AcceptStatus value must not be empty."),
                containsString("@AcceptStatus value \" A\" must not start or end with whitespace."),
                containsString("@AcceptStatus value \"" + longStatus + "\" is 51 characters long; the maximum is 50."),
                containsString("@RetryPolicy fallbackStatus \"B \" must not start or end with whitespace.")));
    }

    @Test
    void AcceptStatus__should_fail_when_status_is_accepted_by_several_methods() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @AcceptStatus("TEST_STATUS_A")
                    @RetryPolicy(interval = "5m")
                    public void accept(Task task) {}

                    @AcceptStatus("B")
                    @AcceptStatus("TEST_STATUS_C")
                    @RetryPolicy(interval = "5m")
                    public void pay(Task task) {}
                }
                """);
        writeSource("Payments.java", """
                package demo.test;
                import com.doer.*;
                public class Payments {
                    @AcceptStatus("B")
                    @AcceptStatus("TEST_STATUS_D")
                    @RetryPolicy(interval = "5m")
                    public void pay(Task task) {}
                }
                """);

        var result = javac("Orders.java", "Payments.java").assertStatus(1);

        assertThat(result.stdErr(), allOf(
                containsString("Status \"B\" is accepted by more than one doer method"),
                containsString("demo.test.Orders.pay(com.doer.Task)"),
                containsString("demo.test.Payments.pay(com.doer.Task)"),
                not(containsString("TEST_STATUS_A")),
                not(containsString("TEST_STATUS_C")),
                not(containsString("TEST_STATUS_D"))));
    }

    @Test
    void RetryPolicy__should_fail_on_invalid_interval_and_fallback_without_duration() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @AcceptStatus("A")
                    @RetryPolicy(interval = "5s", fallbackStatus = "Failed")
                    public void accept(Task task) {}

                    @AcceptStatus("B")
                    @RetryPolicy(interval = "every 5s")
                    public void pay(Task task) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertThat(result.stdErr(), allOf(
                containsString("@RetryPolicy fallbackStatus requires duration"),
                containsString("@RetryPolicy interval \"every 5s\" is not a duration")));
    }

    @Test
    void RetryPolicy__should_fail_on_invalid_duration() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @AcceptStatus("A")
                    @RetryPolicy(interval = "5s", duration = "long")
                    public void accept(Task task) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertEquals("""
                Orders.java:6: error: @RetryPolicy duration "long" is not a duration. Expected a number and a unit, e.g. "5s", "10 min", "2h", "1 day".
                    public void accept(Task task) {}
                                ^
                1 error
                """, result.stdErr());
    }

    @Test
    void ConcurrencyGroup__should_fail_on_method_without_AcceptStatus() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @AcceptStatus("A")
                    public void accept(Task task) {}

                    @ConcurrencyGroup(Orders.class)
                    public void cancel(Task task) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertEquals("""
                Orders.java:8: error: @ConcurrencyGroup can be used only on doer methods (@AcceptStatus methods) and on classes.
                    public void cancel(Task task) {}
                                ^
                1 error
                """, result.stdErr());
    }

    @Test
    void ConcurrencyGroup__should_fail_on_method_with_ConcurrencyLimit() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @ConcurrencyGroup(Orders.class)
                    @ConcurrencyLimit(3)
                    @AcceptStatus("A")
                    public void accept(Task task) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertEquals("""
                Orders.java:7: error: @ConcurrencyGroup and @ConcurrencyLimit can not be used together on a method: @ConcurrencyLimit creates a concurrency group used only by this method. Remove @ConcurrencyGroup to give the method a group of its own, or remove @ConcurrencyLimit to run it in the group of demo.test.Orders.
                    public void accept(Task task) {}
                                ^
                1 error
                """, result.stdErr());
    }

    @Test
    void ConcurrencyGroup__should_fail_on_class_with_limit_referencing_another_class() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @AcceptStatus("A")
                    public void accept(Task task) {}
                }
                """);
        writeSource("Payments.java", """
                package demo.test;
                import com.doer.*;
                @ConcurrencyGroup(Orders.class)
                @ConcurrencyLimit(3)
                public class Payments {
                    @AcceptStatus("B")
                    public void pay(Task task) {}
                }
                """);

        var result = javac("Orders.java", "Payments.java").assertStatus(1);

        assertEquals("""
                Payments.java:5: error: @ConcurrencyGroup of a class with @ConcurrencyLimit must reference the class itself: @ConcurrencyLimit declares the concurrency group of demo.test.Payments. Remove @ConcurrencyLimit to run the class in the group of demo.test.Orders, or use @ConcurrencyGroup(Payments.class).
                public class Payments {
                       ^
                1 error
                """, result.stdErr());
    }

    @Test
    void ConcurrencyGroup__should_fail_on_primitive_class() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @ConcurrencyGroup(int.class)
                    @AcceptStatus("A")
                    public void accept(Task task) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertEquals("""
                Orders.java:6: error: @ConcurrencyGroup value must be a class, not int.
                    public void accept(Task task) {}
                                ^
                1 error
                """, result.stdErr());
    }

    @Test
    void ConcurrencyGroup__should_fail_on_cycle() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                @ConcurrencyGroup(Payments.class)
                public class Orders {
                    @AcceptStatus("A")
                    public void accept(Task task) {}
                }
                """);
        writeSource("Payments.java", """
                package demo.test;
                import com.doer.*;
                @ConcurrencyGroup(Orders.class)
                public class Payments {
                    @AcceptStatus("B")
                    public void pay(Task task) {}
                }
                """);
        writeSource("Shipping.java", """
                package demo.test;
                import com.doer.*;
                public class Shipping {
                    @ConcurrencyGroup(Orders.class)
                    @AcceptStatus("C")
                    public void ship(Task task) {}
                }
                """);

        var result = javac("Orders.java", "Payments.java", "Shipping.java").assertStatus(1);

        assertEquals("""
                Orders.java:4: error: @ConcurrencyGroup references form a cycle, so the concurrency group can not be resolved:
                public class Orders {
                       ^
                      demo.test.Orders -> demo.test.Payments -> demo.test.Orders
                Payments.java:4: error: @ConcurrencyGroup references form a cycle, so the concurrency group can not be resolved:
                public class Payments {
                       ^
                      demo.test.Payments -> demo.test.Orders -> demo.test.Payments
                Shipping.java:6: error: @ConcurrencyGroup references form a cycle, so the concurrency group can not be resolved:
                    public void ship(Task task) {}
                                ^
                      demo.test.Orders -> demo.test.Payments -> demo.test.Orders
                3 errors
                """, result.stdErr());
    }

    @Test
    void ConcurrencyLimit__should_fail_when_method_group_has_the_name_of_another_group() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @ConcurrencyLimit(3)
                    @AcceptStatus("A")
                    public void accept(Task task) {}

                    public static class accept {
                        @AcceptStatus("B")
                        public void run(Task task) {}
                    }
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertEquals("""
                Orders.java:6: error: The concurrency group of this method, created by @ConcurrencyLimit, has the same name "demo.test.Orders.accept" as the group of another doer method or class, so they would share one limit. Rename the method.
                    public void accept(Task task) {}
                                ^
                1 error
                """, result.stdErr());
    }

    @Test
    void ConcurrencyLimit__should_fail_on_value_below_1() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @ConcurrencyLimit(0)
                    @AcceptStatus("A")
                    public void accept(Task task) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertEquals("""
                Orders.java:6: error: @ConcurrencyLimit value must be at least 1.
                    public void accept(Task task) {}
                                ^
                1 error
                """, result.stdErr());
    }

    @Test
    void ConcurrencyLimit__should_fail_on_method_without_AcceptStatus() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @AcceptStatus("A")
                    public void accept(Task task) {}

                    @ConcurrencyLimit(3)
                    public void cancel(Task task) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertEquals("""
                Orders.java:8: error: @ConcurrencyLimit can be used only on doer methods (@AcceptStatus methods) and on classes.
                    public void cancel(Task task) {}
                                ^
                1 error
                """, result.stdErr());
    }

    @Test
    void ConcurrencyLimit__should_warn_when_class_group_has_no_doer_methods() throws Exception {
        writeSource("OrderGroup.java", """
                package demo.test;
                import com.doer.*;
                @ConcurrencyLimit(3)
                public class OrderGroup {
                }
                """);
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @AcceptStatus("A")
                    public void accept(Task task) {}
                }
                """);

        var result = javac("OrderGroup.java", "Orders.java").assertStatus(0);

        assertEquals("""
                OrderGroup.java:4: warning: @ConcurrencyLimit has no effect: no doer method runs in concurrency domain "demo.test.OrderGroup".
                public class OrderGroup {
                       ^
                1 warning
                """, result.stdErr());
    }

    @Test
    void TaskDataLoader__should_fail_when_argument_has_no_loader() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @AcceptStatus("A")
                    @RetryPolicy(interval = "5m")
                    public void accept(Task task, Integer order) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertThat(result.stdErr(), containsString("No @TaskDataLoader found for argument 1"));
    }

    @Test
    void TaskDataLoader__should_fail_on_wrong_parameters() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @TaskDataLoader
                    public String loadName(TaskSnapshot task, String prefix) {
                        return prefix;
                    }
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertEquals("""
                Orders.java:5: error: com.doer.TaskDataLoader should have exactly 1 argument of type com.doer.TaskSnapshot
                    public String loadName(TaskSnapshot task, String prefix) {
                                  ^
                1 error
                """, result.stdErr());
    }

    @Test
    void TaskDataLoader__should_fail_on_Task_TaskSnapshot_and_DoerService_types() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @TaskDataLoader
                    public Task loadTask(TaskSnapshot task) {
                        return null;
                    }

                    @TaskDataLoader
                    public TaskSnapshot loadSnapshot(TaskSnapshot task) {
                        return task;
                    }

                    @TaskDataLoader
                    public DoerService loadDoerService(TaskSnapshot task) {
                        return null;
                    }
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertThat(result.stdErr(), allOf(
                containsString("Type com.doer.Task of return value of @TaskDataLoader method can not be task data"),
                containsString("Type com.doer.TaskSnapshot of return value of @TaskDataLoader method "
                        + "can not be task data"),
                containsString("Type com.doer.DoerService of return value of @TaskDataLoader method "
                        + "can not be task data"),
                not(containsString("_GeneratedDoerService.java"))));
    }

    @Test
    void TaskDataSaver__should_fail_on_wrong_signature() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @TaskDataSaver
                    public void saveName(TaskSnapshot task) {}

                    @TaskDataSaver
                    public boolean saveText(TaskSnapshot task, String text) {
                        return true;
                    }
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertEquals("""
                Orders.java:5: error: com.doer.TaskDataSaver should have exactly 2 arguments: TaskSnapshot and the task data to save, and should return void.
                    public void saveName(TaskSnapshot task) {}
                                ^
                Orders.java:8: error: com.doer.TaskDataSaver should have exactly 2 arguments: TaskSnapshot and the task data to save, and should return void.
                    public boolean saveText(TaskSnapshot task, String text) {
                                   ^
                2 errors
                """, result.stdErr());
    }

    @Test
    void TaskDataSaver__should_fail_on_Task_TaskSnapshot_and_DoerService_types() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @TaskDataSaver
                    public void saveTask(TaskSnapshot task, Task data) {}

                    @TaskDataSaver
                    public void saveSnapshot(TaskSnapshot task, TaskSnapshot data) {}

                    @TaskDataSaver
                    public void saveDoerService(TaskSnapshot task, DoerService service) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertThat(result.stdErr(), allOf(
                containsString("Type com.doer.Task of parameter data of @TaskDataSaver method can not be task data"),
                containsString("Type com.doer.TaskSnapshot of parameter data of @TaskDataSaver method "
                        + "can not be task data"),
                containsString("Type com.doer.DoerService of parameter service of @TaskDataSaver method "
                        + "can not be task data"),
                not(containsString("_GeneratedDoerService.java"))));
    }

    @Test
    void task_data_methods__should_fail_on_Task_instead_of_TaskSnapshot() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                import jakarta.json.JsonObjectBuilder;
                public class Orders {
                    @TaskDataLoader
                    public String loadName(Task task) {
                        return null;
                    }

                    @TaskDataSaver
                    public void saveName(Task task, String name) {}

                    @ExceptionDescriber
                    public void describe(Task task, Exception e, JsonObjectBuilder builder) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertThat(result.stdErr(), allOf(
                containsString("com.doer.TaskDataLoader should have exactly 1 argument of type com.doer.TaskSnapshot"),
                containsString("com.doer.TaskDataSaver should have exactly 2 arguments: TaskSnapshot and the task data "
                        + "to save, and should return void."),
                containsString("@ExceptionDescriber method should return void and have exactly 3 parameters: "
                        + "TaskSnapshot, the exception type"),
                not(containsString("_GeneratedDoerService.java"))));
    }

    @Test
    void task_data__should_fail_on_types_that_are_not_plain_classes() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                import java.util.List;
                public class Orders {
                    @AcceptStatus("A")
                    public void accept(Task task, List<String> names) {}

                    @AcceptStatus("B")
                    public void pay(Task task, int amount) {}

                    @AcceptStatus("C")
                    public void ship(Task task, String[] addresses) {}

                    @TaskDataLoader
                    public List<String> loadNames(TaskSnapshot task) {
                        return null;
                    }

                    @TaskDataSaver
                    public void saveAddresses(TaskSnapshot task, String[] addresses) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        String notSupported = " is not supported by doer methods (@AcceptStatus methods). "
                + "Only classes without type arguments are supported";
        assertThat(result.stdErr(), allOf(
                containsString("Type java.util.List<java.lang.String> of parameter names of @AcceptStatus method"
                        + notSupported),
                containsString("Type int of parameter amount of @AcceptStatus method" + notSupported),
                containsString("Type java.lang.String[] of parameter addresses of @AcceptStatus method"
                        + notSupported),
                containsString("Type java.util.List<java.lang.String> of return value of @TaskDataLoader method"
                        + notSupported),
                containsString("Type java.lang.String[] of parameter addresses of @TaskDataSaver method"
                        + notSupported),
                not(containsString("No @TaskDataLoader found"))));
    }

    @Test
    void task_data__should_fail_on_unresolved_type() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @AcceptStatus("A")
                    public void accept(Task task, Unknown order) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertThat(result.stdErr(), containsString(
                "Type Unknown of parameter order of @AcceptStatus method can not be resolved."));
    }

    @Test
    void ExceptionDescriber__should_fail_on_not_throwable_type() throws Exception {
        writeSource("Describers.java", """
                package demo.test;
                import com.doer.*;
                import jakarta.json.JsonObjectBuilder;
                public class Describers {
                    @ExceptionDescriber
                    public void describeText(TaskSnapshot task, String text, JsonObjectBuilder builder) {}
                }
                """);

        var result = javac("Describers.java").assertStatus(1);

        assertThat(result.stdErr(), allOf(
                containsString("Second parameter of @ExceptionDescriber annotated method describeText "
                        + "should be of Throwable type"),
                not(containsString("_GeneratedDoerService.java"))));
    }

    @Test
    void ExceptionDescriber__should_fail_on_wrong_signature() throws Exception {
        writeSource("Describers.java", """
                package demo.test;
                import com.doer.*;
                public class Describers {
                    @ExceptionDescriber
                    public void describe(TaskSnapshot task, Exception e) {}
                }
                """);

        var result = javac("Describers.java").assertStatus(1);

        assertThat(result.stdErr(), allOf(
                containsString("@ExceptionDescriber method should return void and have exactly 3 parameters: "
                        + "TaskSnapshot, the exception type it describes (Throwable or any subclass of it) "
                        + "and JsonObjectBuilder."),
                not(containsString("_GeneratedDoerService.java"))));
    }

    @Test
    void access__should_fail_on_classes_the_generated_service_can_not_refer_to() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    record CancellationContext(String reason) {}

                    @AcceptStatus("A")
                    public void cancel(Task task, CancellationContext context) {}

                    @AcceptStatus("B")
                    public void ship(Task task, Contexts.ShippingContext context) {}

                    @TaskDataLoader
                    public Contexts.ShippingContext loadShippingContext(TaskSnapshot task) {
                        return null;
                    }

                    @TaskDataLoader
                    public Hidden.PaymentContext loadPaymentContext(TaskSnapshot task) {
                        return null;
                    }

                    @ExceptionDescriber
                    public void describe(TaskSnapshot task, HiddenException e, jakarta.json.JsonObjectBuilder builder) {}
                }

                class HiddenException extends Exception {
                }

                interface Contexts {
                    record ShippingContext(String address) {}
                }

                class Hidden {
                    public record PaymentContext(String card) {}

                    @AcceptStatus("C")
                    public void hide(Task task) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        String notSupported = " is not supported by doer methods (@AcceptStatus methods). ";
        String generated = ", so the generated service in package com.doer.generated can not refer to it.";
        assertThat(result.stdErr(), allOf(
                containsString("Type demo.test.Orders.CancellationContext of parameter context of @AcceptStatus "
                        + "method" + notSupported + "The class is not public" + generated),
                containsString("Type demo.test.Contexts.ShippingContext of parameter context of @AcceptStatus "
                        + "method" + notSupported + "The class is declared in not public class demo.test.Contexts"
                        + generated),
                containsString("Type demo.test.Hidden.PaymentContext of return value of @TaskDataLoader method"
                        + notSupported + "The class is declared in not public class demo.test.Hidden" + generated),
                containsString("Exception class demo.test.HiddenException of the second parameter of "
                        + "@ExceptionDescriber annotated method describe is not public" + generated),
                containsString("Class demo.test.Hidden is not public" + generated),
                not(containsString("_GeneratedDoerService.java"))));
    }

    @Test
    void access__should_fail_on_doer_class_in_unnamed_package() throws Exception {
        writeSource("Orders.java", """
                import com.doer.*;
                public class Orders {
                    @AcceptStatus("A")
                    public void accept(Task task) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertThat(result.stdErr(), allOf(
                containsString("Class in unnamed package"),
                containsString("Please move your class Orders to any package"),
                not(containsString("_GeneratedDoerService.java"))));
    }

    @Test
    void proc_only__should_warn_that_doer_json_has_no_emits() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @AcceptStatus("A")
                    @RetryPolicy(interval = "5m")
                    public void accept(Task task) {
                        task.setStatus("B");
                    }
                }
                """);

        var result = javac("-proc:only", "Orders.java").assertStatus(0);

        assertThat(result.stdErr(),
                containsString("doer.json and doer.dot do not contain the statuses set by Task.setStatus"));
        assertEquals(List.of(), doerJson().getList("doer_methods[0].emits"));
        assertTrue(Files.isRegularFile(generated("doer.dot")));
    }

    @Test
    void proc_only__should_not_warn_that_doer_json_has_no_emits_when_processing_failed() throws Exception {
        writeSource("Orders.java", """
                package demo.test;
                import com.doer.*;
                public class Orders {
                    @AcceptStatus(value = "A", delay = "soon")
                    public void accept(Task task) {}
                }
                """);

        var result = javac("Orders.java").assertStatus(1);

        assertThat(result.stdErr(),
                not(containsString("doer.json and doer.dot do not contain the statuses set by Task.setStatus")));
    }
}
