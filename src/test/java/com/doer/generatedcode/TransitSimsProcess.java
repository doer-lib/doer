package com.doer.generatedcode;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.doer.testkit.Toolchain;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Transit Sims built by a build tool and run in its own JVM, without a container and a database: the system under test
 * of the {@code *GeneratedCodeITCase} classes. They compile {@link #MAIN} together with Transit Sims and start it with
 * {@link #start}; then every request is a line to the stdin of {@code transitsims.Main}, and every response a line from
 * its stdout. The command is kept in main-cmd.txt of the workspace, the stderr of Main in main-err.txt. See
 * e2e-test-app.md.
 */
class TransitSimsProcess implements AutoCloseable {
    private final Path workDir;
    private final Process process;
    private final BufferedWriter in;
    private final BufferedReader out;

    private TransitSimsProcess(Path workDir, Process process) {
        this.workDir = workDir;
        this.process = process;
        this.in = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), UTF_8));
        this.out = new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8));
    }

    /** Starts transitsims.Main in the folder, with the classpath. */
    static TransitSimsProcess start(Path workDir, String classpath) throws Exception {
        List<String> cmd = List.of("java", "-cp", classpath, "transitsims.Main");
        Files.writeString(workDir.resolve("main-cmd.txt"), String.join(" ", cmd) + "\n");
        Process process = new ProcessBuilder(cmd)
                .directory(workDir.toFile())
                .redirectError(workDir.resolve("main-err.txt").toFile())
                .start();
        return new TransitSimsProcess(workDir, process);
    }

    /** Sends the JSON request to TaskRunner as one line and returns its response line. */
    String runTask(String request) throws Exception {
        in.write(request.replace("\n", " ").strip());
        in.newLine();
        in.flush();
        String response = CompletableFuture.supplyAsync(() -> {
            try {
                return out.readLine();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).get(Toolchain.TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (response == null) {
            throw new IllegalStateException("transitsims.Main has exited, see main-err.txt in " + workDir);
        }
        return response;
    }

    /** Closes the stdin of Main, and Main exits. */
    @Override
    public void close() throws Exception {
        in.close();
        if (!process.waitFor(Toolchain.TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            process.destroyForcibly();
        }
    }

    /**
     * Wires the Transit Sims components by hand, with TestDoerService instead of a database, and passes each line of stdin
     * to TaskRunner; writes each response as a line to stdout. The task gets its id in {@code insert}: an anonymous
     * subclass of Task, because {@code Task.setId} is protected.
     */
    static final String MAIN = """
            package transitsims;

            import transitsims.validation.CallTrace;
            import transitsims.validation.ConcurrencyLimitOne;
            import transitsims.validation.ConcurrencyQueues;
            import transitsims.validation.DoerMethodNextClass;
            import transitsims.validation.DoerMethodStatuses;
            import transitsims.validation.ErrorDescribers;
            import transitsims.validation.ErrorMethods;
            import transitsims.validation.GeneratedCodeFailures;
            import transitsims.validation.GeneratedCodeMethods;
            import transitsims.validation.GeneratedCodeTaskData;
            import transitsims.validation.TaskRunner;
            import transitsims.validation.TransactionMethods;
            import transitsims.validation.ValidationResource;
            import com.doer.Task;
            import demo.test.TestDoerService;
            import java.io.BufferedReader;
            import java.io.InputStreamReader;
            import java.nio.charset.StandardCharsets;

            public class Main {
                public static void main(String[] args) throws Exception {
                    var doer = new TestDoerService() {
                        long lastId;

                        @Override
                        public void insert(Task task) {
                            long id = ++lastId;
                            Task inserted = new Task() {
                                {
                                    setId(id);
                                }
                            };
                            inserted.setStatus(task.getStatus());
                            inserted.setInProgress(task.isInProgress());
                            inserted.setFailingSince(task.getFailingSince());
                            task.assignFieldsFrom(inserted);
                        }
                    };
                    var callTrace = new CallTrace();
                    var generatedCodeMethods = new GeneratedCodeMethods();
                    generatedCodeMethods.setCallTrace(callTrace);
                    var generatedCodeFailures = new GeneratedCodeFailures();
                    generatedCodeFailures.setCallTrace(callTrace);
                    var generatedCodeTaskData = new GeneratedCodeTaskData();
                    generatedCodeTaskData.setCallTrace(callTrace);
                    var validationResource = new ValidationResource();
                    validationResource.setDoerService(doer);
                    doer._inject_concurrencyLimitOne(new ConcurrencyLimitOne());
                    doer._inject_concurrencyQueues(new ConcurrencyQueues());
                    doer._inject_doerMethodNextClass(new DoerMethodNextClass());
                    doer._inject_doerMethodStatuses(new DoerMethodStatuses());
                    doer._inject_errorDescribers(new ErrorDescribers());
                    doer._inject_errorMethods(new ErrorMethods());
                    doer._inject_transactionMethods(new TransactionMethods());
                    doer._inject_validationResource(validationResource);
                    doer._inject_generatedCodeFailures(generatedCodeFailures);
                    doer._inject_generatedCodeMethods(generatedCodeMethods);
                    doer._inject_generatedCodeTaskData(generatedCodeTaskData);
                    var taskRunner = new TaskRunner();
                    taskRunner.setDoerService(doer);
                    taskRunner.setCallTrace(callTrace);

                    var in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
                    for (String line = in.readLine(); line != null; line = in.readLine()) {
                        String response;
                        try {
                            response = taskRunner.run(line);
                        } catch (Exception e) {
                            e.printStackTrace();
                            response = "{\\"exception\\": \\"" + e.getClass().getName() + "\\"}";
                        }
                        System.out.println(response);
                        System.out.flush();
                    }
                }
            }
            """;
}
