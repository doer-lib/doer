package com.doer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Failures of the user's code in {@link DoerService#callDoerMethod}: loaders, the doer method and savers (the
 * generated createExtraJson catches the failures of the exception describers itself, see GeneratedCodeTest). The
 * tables are faked in memory, and a transaction that throws is rolled back.
 */
public class CallDoerMethodTest {

    static final Duration ERROR_TIMEOUT = Duration.ofDays(1);

    FakeDbService service = new FakeDbService();
    List<String> calls = new ArrayList<>();
    List<LogRecord> logRecords = new ArrayList<>();
    Handler logHandler = new Handler() {
        @Override
        public void publish(LogRecord record) {
            logRecords.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    };
    Task task;

    @BeforeEach
    void init() {
        DoerService.LOG.addHandler(logHandler);
        task = service.insertRow("A", null);
    }

    @AfterEach
    void removeLogHandler() {
        DoerService.LOG.removeHandler(logHandler);
        Thread.interrupted();
    }

    @Test
    void success__should_save_new_status_and_write_log() throws Exception {
        call(load(), doer("B"), save());

        assertEquals(List.of("load", "doer", "save"), calls);
        assertRow("B", false, 2);
        assertEquals(List.of("A -> B"), service.logs());
        assertTaskIsRow();
    }

    @Test
    void loader_exception__should_be_recorded_as_failure_without_in_progress_mark() throws Exception {
        call(fail("load", new IllegalStateException("No data")), doer("B"), save());

        assertEquals(List.of("load"), calls, "the doer method and the savers are not called");
        // The transaction of the loader is rolled back, the failure is saved in the next one
        assertRow("A", true, 1);
        assertEquals(List.of("A -> A java.lang.IllegalStateException"), service.logs());
        assertLogged(Level.SEVERE, "Task data loader error");
        assertTaskIsRow();
    }

    @ParameterizedTest
    @MethodSource("userCodeFailures")
    void loader_failure__should_be_recorded_for_exceptions_and_errors_of_user_code(Throwable failure) throws Exception {
        call(fail("load", failure), doer("B"), save());

        assertRow("A", true, 1);
        assertEquals(List.of("A -> A " + failure.getClass().getName()), service.logs());
    }

    static Stream<Throwable> userCodeFailures() {
        return Stream.of(new Exception(), new RuntimeException(), new InterruptedException(),
                new OutOfMemoryError(), new StackOverflowError(), new NoClassDefFoundError("demo/Missing"),
                new ExceptionInInitializerError(), new AbstractMethodError(), new IllegalAccessError(),
                new ServiceConfigurationError("no provider"), new AssertionError("assert"));
    }

    @Test
    void loader_internal_error__should_not_be_caught() {
        InternalError error = new InternalError("JVM");

        assertSame(error, assertThrows(InternalError.class, () -> call(fail("load", error), doer("B"), save())));

        assertRow("A", false, 0);
        assertEquals(List.of(), service.logs());
    }

    @Test
    void doer_method_exception__should_be_recorded_and_logged_as_warning() throws Exception {
        call(load(), fail("doer", new IllegalStateException("Payment declined")), save());

        assertEquals(List.of("load", "doer"), calls);
        assertRow("A", true, 2);
        assertEquals(List.of("A -> A java.lang.IllegalStateException"), service.logs());
        assertLogged(Level.WARNING, "Doer method error");
    }

    @Test
    void doer_method_interrupted__should_be_recorded_and_leave_thread_interrupted() throws Exception {
        call(load(), fail("doer", new InterruptedException()), save());

        assertTrue(Thread.interrupted(), "the thread is interrupted again after the failure is recorded");
        assertRow("A", true, 2);
        assertEquals(List.of("A -> A java.lang.InterruptedException"), service.logs());
    }

    @Test
    void saver_exception__should_roll_back_new_status_and_be_recorded() throws Exception {
        call(load(), doer("B"), fail("save", new IllegalStateException("Disk full")));

        assertEquals(List.of("load", "doer", "save"), calls);
        // The transaction of the saver is rolled back with the new status and its log
        assertRow("A", true, 2);
        assertEquals(List.of("A -> A java.lang.IllegalStateException"), service.logs());
        assertLogged(Level.SEVERE, "Task data saver error");
        assertTaskIsRow();
    }

    @Test
    void saver_exception__should_keep_failing_since_of_failing_task() throws Exception {
        Instant failingSince = service.getDbNow().minus(Duration.ofHours(1));
        task = service.insertRow("A", failingSince);

        call(load(), doer("B"), fail("save", new IllegalStateException("Disk full")));

        assertEquals(failingSince, service.row().getFailingSince());
        assertEquals("A", service.row().getStatus());
    }

    @Test
    void saver_exception__should_set_error_status_after_error_timeout() throws Exception {
        task = service.insertRow("A", service.getDbNow().minus(Duration.ofHours(25)));

        call(load(), doer("B"), fail("save", new IllegalStateException("Disk full")));

        assertEquals("FAILED", service.row().getStatus());
        assertNull(service.row().getFailingSince());
        assertEquals(List.of("A -> FAILED java.lang.IllegalStateException"), service.logs());
    }

    @Test
    void createExtraJson__should_give_extra_json_of_failure() throws Exception {
        call(load(), fail("doer", new IllegalStateException("Payment declined")), save());

        assertEquals("{\"message\": \"Payment declined\"}", service.logs.get(0).extraJson());
    }

    @Test
    void version_conflict_at_start__should_be_thrown_without_calling_user_code() {
        service.row().setVersion(5);

        assertThrows(TaskVersionConflictException.class, () -> call(load(), doer("B"), save()));

        assertEquals(List.of(), calls);
        assertEquals(List.of(), service.logs());
    }

    private void call(Callable<Object> loader, Callable<Object> caller, Callable<Object> saver) throws Exception {
        service.callDoerMethod(task, loader, caller, saver, "Orders", "accept", ERROR_TIMEOUT, "FAILED");
    }

    private Callable<Object> load() {
        return () -> {
            calls.add("load");
            return null;
        };
    }

    private Callable<Object> doer(String newStatus) {
        return () -> {
            calls.add("doer");
            task.setStatus(newStatus);
            return null;
        };
    }

    private Callable<Object> save() {
        return () -> {
            calls.add("save");
            return null;
        };
    }

    private Callable<Object> fail(String call, Throwable failure) {
        return () -> {
            calls.add(call);
            if (call.equals("doer")) {
                task.setStatus("B");
            }
            throw sneaky(failure);
        };
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> RuntimeException sneaky(Throwable failure) throws T {
        throw (T) failure;
    }

    private void assertRow(String status, boolean failing, int version) {
        Task row = service.row();
        assertEquals(status, row.getStatus(), "status");
        assertEquals(failing, row.getFailingSince() != null, "failing");
        assertFalse(row.isInProgress(), "in_progress");
        assertEquals(version, row.getVersion(), "version");
    }

    /** The task in memory is as saved: Doer can update it again. */
    private void assertTaskIsRow() {
        Task row = service.row();
        assertEquals(row.getVersion(), task.getVersion());
        assertEquals(row.getStatus(), task.getStatus());
        assertEquals(row.getFailingSince(), task.getFailingSince());
        assertEquals(row.isInProgress(), task.isInProgress());
    }

    private void assertLogged(Level level, String messageStart) {
        assertTrue(logRecords.stream().anyMatch(r -> r.getLevel() == level && r.getMessage().startsWith(messageStart)),
                () -> "no " + level + " \"" + messageStart + "...\" in " + logRecords.stream()
                        .map(r -> r.getLevel() + " " + r.getMessage()).toList());
    }

    record Log(String initialStatus, String finalStatus, String exceptionType, String extraJson) {
    }

    /** DoerService on tables in memory: one task row and the task logs. */
    static class FakeDbService extends DoerService {
        final Map<Long, Task> rows = new HashMap<>();
        final List<Log> logs = new ArrayList<>();
        Function<Throwable, String> extraJson = e -> "{\"message\": \"" + e.getMessage() + "\"}";

        FakeDbService() {
            beanProviders.put(DoerService.class, () -> this);
        }

        Task insertRow(String status, Instant failingSince) {
            rows.clear();
            Task row = new Task();
            row.setId(1L);
            row.setStatus(status);
            row.setCreated(getDbNow());
            row.setModified(getDbNow());
            row.setFailingSince(failingSince);
            rows.put(row.getId(), row);
            return copyOf(row);
        }

        Task row() {
            return rows.get(1L);
        }

        /** "initial -> final exceptionType" of each log. */
        List<String> logs() {
            return logs.stream().map(l -> l.initialStatus() + " -> " + l.finalStatus()
                    + (l.exceptionType() == null ? "" : " " + l.exceptionType())).toList();
        }

        @Override
        public void runInTransaction(Callable<Object> code) throws Exception {
            Map<Long, Task> rowsBefore = new HashMap<>();
            rows.forEach((id, row) -> rowsBefore.put(id, copyOf(row)));
            int logsBefore = logs.size();
            try {
                code.call();
            } catch (Throwable e) {
                rows.clear();
                rows.putAll(rowsBefore);
                logs.subList(logsBefore, logs.size()).clear();
                throw e;
            }
        }

        @Override
        public boolean updateAndBumpVersion(Task task) {
            Task row = rows.get(task.getId());
            if (row == null || row.getVersion() != task.getVersion()) {
                return false;
            }
            Task updated = copyOf(task);
            updated.setVersion(task.getVersion() + 1);
            updated.setModified(getDbNow());
            rows.put(task.getId(), updated);
            task.assignFieldsFrom(updated);
            return true;
        }

        @Override
        public long writeTaskLog(Long taskId, String initialStatus, String finalStatus, String className,
                String methodName, String exceptionType, String extraJson, Integer durationMs) {
            logs.add(new Log(initialStatus, finalStatus, exceptionType, extraJson));
            return logs.size();
        }

        @Override
        public String createExtraJson(TaskSnapshot task, Throwable exception) {
            return extraJson.apply(exception);
        }

        @Override
        public void runTask(Task task) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Object _load(TaskSnapshot task, Class<?> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void _save(TaskSnapshot task, Class<?> type, Object data) {
            throw new UnsupportedOperationException();
        }

        private static Task copyOf(Task task) {
            Task copy = new Task();
            copy.assignFieldsFrom(task);
            return copy;
        }
    }
}
