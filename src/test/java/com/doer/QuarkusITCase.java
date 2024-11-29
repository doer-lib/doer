package com.doer;

import static com.doer.Utils.exec;
import static com.doer.Utils.selectLongValue;
import static com.doer.Utils.selectStringValue;
import static com.doer.Utils.sqlUpdate;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.postgresql.ds.PGSimpleDataSource;

import io.restassured.RestAssured;
import io.restassured.path.json.JsonPath;

@DisabledOnJre({JRE.JAVA_8, JRE.JAVA_11})
public class QuarkusITCase {
    static Path localMaven;
    static Path quarkusDir;
    static Process quarkus;
    static int quarkusPort;
    static String doerLibVersion = System.getProperty("doer.lib.version", "1.0-SNAPSHOT");

    @BeforeAll
    static void initTestFolder() throws Exception {
        localMaven = Paths.get("target", "e2e-doer-m2").toAbsolutePath();
        quarkusDir = Paths.get("target", "e2e-doer-quarkus").toAbsolutePath();

        Path projectDir = new File(".").toPath().toAbsolutePath();
        Path jarPath = Paths.get("target", "doer-" + doerLibVersion +  ".jar");

        long t0 = System.currentTimeMillis();
        if (!Files.exists(localMaven)) {
            Files.createDirectory(localMaven);
        }

        exec(0, projectDir,
                "mvn -B install:install-file ? -DgroupId=com.doer -DartifactId=doer ? -Dpackaging=jar -DgeneratePom=true ?",
                "-Dfile=" + jarPath,
                "-Dversion=" + doerLibVersion,
                "-DlocalRepositoryPath=" + localMaven);
        System.out.printf("✔ Created local maven repository in %s seconds%n", (System.currentTimeMillis() - t0) / 1000);

        t0 = System.currentTimeMillis();
        Utils.deleteReqursivelly(quarkusDir);
        exec(0, quarkusDir.getParent(),
                "mvn -B ? io.quarkus.platform:quarkus-maven-plugin:3.17.2:create -DprojectGroupId=tst.demo " +
                        "-DprojectArtifactId=e2e-doer-quarkus " +
                        "-Dextensions=io.quarkus:quarkus-resteasy,io.quarkus:quarkus-jdbc-postgresql," +
                        "io.quarkus:quarkus-flyway,io.quarkus:quarkus-smallrye-health,io.quarkus:quarkus-resteasy-jsonb",
                "-Dmaven.repo.local=" + localMaven);
        Path testsFolder = quarkusDir.resolve("src/test/java/tst");
        Utils.deleteReqursivelly(testsFolder);

        Path properties = quarkusDir.resolve("src/main/resources/application.properties");
        System.out.printf("✔ Quarkus app created in %s seconds %s%n", (System.currentTimeMillis() - t0) / 1000,
                quarkusDir);

        t0 = System.currentTimeMillis();
        PGSimpleDataSource ds = Utils.getPostgresDataSource();
        int dbPort = ds.getPortNumbers()[0];
        System.out.printf("✔ Postgres started in %s seconds on port %s%n", (System.currentTimeMillis() - t0) / 1000,
                dbPort);

        t0 = System.currentTimeMillis();
        Files.write(properties, Arrays.asList(
                "quarkus.flyway.migrate-at-start=true",
                "quarkus.datasource.db-kind=postgresql",
                "quarkus.datasource.username=doer",
                "quarkus.datasource.password=doer",
                "quarkus.datasource.jdbc.url=jdbc:postgresql://localhost:" + dbPort + "/doer",
                "quarkus.datasource.jdbc.max-size=4",
                "quarkus.log.console.format=%d{yyyy-MM-dd HH:mm:ss,SSS} %p {%t} %c{1.} - %s%e%n"));

        String pomXml = String.join(System.lineSeparator(), Files.readAllLines(quarkusDir.resolve("pom.xml")));
        String updatedPomXlm = Pattern.compile("</dependencies>\\s*<build>", Pattern.MULTILINE)
                .matcher(pomXml)
                .replaceFirst("<dependency>\n" +
                        "  <groupId>org.flywaydb</groupId>\n" +
                        "  <artifactId>flyway-database-postgresql</artifactId>\n" +
                        "</dependency>\n" +
                        "<dependency>\n" +
                        "  <groupId>com.doer</groupId>\n" +
                        "  <artifactId>doer</artifactId>\n" +
                        "  <version>" + doerLibVersion + "</version>\n" +
                        "</dependency>\n" +
                        "</dependencies>\n<build>");
        Files.write(quarkusDir.resolve("pom.xml"), updatedPomXlm.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.TRUNCATE_EXISTING);

        exec(0, projectDir, "cp -r src/test/resources/e2e-code/tst ?",
                quarkusDir.resolve("src/main/java"));
        exec(0, projectDir, "cp -r src/test/resources/e2e-code/test/tst ?",
                quarkusDir.resolve("src/test/java"));
        exec(0, projectDir, "cp -r src/test/resources/e2e-code/db ?",
                quarkusDir.resolve("src/main/resources"));

        // 1. First build to generate sql files
        exec(0, quarkusDir, "mvn -B ? package", "-Dmaven.repo.local=" + localMaven);

        Path generatedDir = quarkusDir.resolve("target/classes/com/doer/generated");
        Path flywayFile = quarkusDir.resolve("src/main/resources/db/migration/V20240224_00__create_doer_tables.sql");
        Files.copy(generatedDir.resolve("CreateSchema.sql"),
                flywayFile, StandardCopyOption.REPLACE_EXISTING);
        try (OutputStream out = Files.newOutputStream(flywayFile, StandardOpenOption.APPEND)) {
            Files.copy(generatedDir.resolve("CreateIndexes.sql"), out);
        }
        // 2. Building app with updated FlyWay sql
        exec(0, quarkusDir, "mvn -B ? package", "-Dmaven.repo.local=" + localMaven);

        Path quarkusOut = quarkusDir.resolve("quarkus-app-out.txt");
        Files.createFile(quarkusOut);
        quarkus = new ProcessBuilder("java", "-Dquarkus.http.port=0", "-jar", "target/quarkus-app/quarkus-run.jar")
                .directory(quarkusDir.toFile())
                .redirectError(Redirect.INHERIT)
                .redirectOutput(quarkusOut.toFile())
                .start();
        quarkusPort = locateQuarkusPortFromStdIn(quarkusOut);
        RestAssured.baseURI = "http://localhost:" + quarkusPort;
        System.out.printf("✔ Quarkus app compiled and started in %s seconds on %s%n",
                (System.currentTimeMillis() - t0) / 1000, RestAssured.baseURI);
        Files.copy(quarkusOut, System.out);
        System.out.println("    See Quarkus Application output in file:");
        System.out.println("    " + quarkusOut);
    }

    @AfterAll
    static void stopQuarkus() {
        if (quarkus != null) {
            try {
                quarkus.getOutputStream().close();
                quarkus.waitFor(200, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (quarkus.isAlive()) {
                    quarkus.destroy();
                    quarkus.waitFor(200, TimeUnit.MILLISECONDS);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (quarkus.isAlive()) {
                    quarkus.destroyForcibly();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            quarkus = null;
        }
    }

    static int locateQuarkusPortFromStdIn(Path quarkusOut) throws Exception {
        long t0 = System.currentTimeMillis();
        while (true) {
            Optional<Integer> portOptional = Files.readAllLines(quarkusOut)
                    .stream()
                    .filter(s -> s.contains("Listening on: http://0.0.0.0:"))
                    .map(s -> s.split("Listening on: http://0.0.0.0:")[1])
                    .map(Integer::valueOf)
                    .findFirst();
            if (portOptional.isPresent()) {
                return portOptional.get();
            }
            if (!quarkus.isAlive() || System.currentTimeMillis() - t0 > 15000) {
                Files.copy(quarkusOut, System.out);
                throw new RuntimeException("Quarkus startup timeout");
            }
            Thread.sleep(500);
        }
    }

    @BeforeEach
    void initResteasy() throws Exception {
        RestAssured.baseURI = "http://localhost:" + quarkusPort;
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    }

    @Test
    void quarkus__should_start() throws Exception {
        given()
                .when()
                .get("/q/health")
                .then()
                .statusCode(200)
                .body("status", equalTo("UP"));
    }

    @Test
    void jaxrs_resource_can_have_doer_method() {
        resetServer();
        long id = pushTask("A");
        assertNull(waitTaskStatus(id, null));
    }

    @Test
    void load_should_happen_in_the_same_transaction_with_task_start() {
        resetServer();
        long taskId = pushTask("Car need polishing");
        assertEquals("Car is polished", waitTaskStatus(taskId, "Car is polished"));
        List<DemoLogRow> logs = loadDemoLogs(taskId);
        assertEquals(4, logs.size());

        DemoLogRow taskStart = logs.get(0);
        DemoLogRow carLoaded = logs.get(1);
        DemoLogRow taskStop = logs.get(2);
        assertEquals("task", taskStart.type);
        assertEquals("Car", carLoaded.type);
        assertEquals("task", taskStop.type);
        assertEquals(taskStart.txId, carLoaded.txId);
        assertNotEquals(carLoaded.txId, taskStop.txId);
    }

    @Test
    void unload_should_happen_in_the_same_transaction_with_task_stop() {
        resetServer();
        long taskId = pushTask("Car need polishing");
        assertEquals("Car is polished", waitTaskStatus(taskId, "Car is polished"));
        List<DemoLogRow> logs = loadDemoLogs(taskId);
        assertEquals(4, logs.size());

        DemoLogRow taskStart = logs.get(0);
        DemoLogRow taskStop = logs.get(2);
        DemoLogRow carUnloaded = logs.get(3);
        assertEquals("task", taskStart.type);
        assertEquals("task", taskStop.type);
        assertEquals("Car", carUnloaded.type);
        assertEquals(taskStop.txId, carUnloaded.txId);
        assertNotEquals(carUnloaded.txId, taskStart.txId);
    }

    @Test
    void task_updated_in_doer_method_should_run_in_its_own_transaction() {
        resetServer();
        long taskId = pushTask("Need wash hands");
        assertEquals("Washed", waitTaskStatus(taskId, "Washed"));

        List<DemoLogRow> logs = loadDemoLogs(taskId);
        assertEquals(3, logs.size());
        DemoLogRow taskStart = logs.get(0);
        DemoLogRow taskUpdate = logs.get(1);
        DemoLogRow taskStop = logs.get(2);
        assertEquals("task", taskStart.type);
        assertEquals("task", taskUpdate.type);
        assertEquals("task", taskStop.type);
        assertNotEquals(taskStart.txId, taskUpdate.txId);
        assertNotEquals(taskUpdate.txId, taskStop.txId);
        assertNotEquals(taskStop.txId, taskStart.txId);
    }

    @Test
    void concurrency_1_should_run_only_1_method_at_a_time() {
        resetServer();
        long task1 = pushTask("Need call taxi");
        pushTask("Need order pizza");
        pushTask("Time to cleanup");
        pushTask("Need call taxi");
        pushTask("Need order pizza");
        long task2 = pushTask("Time to cleanup");
        assertEquals("Cleanup finished", waitTaskStatus(task2, "Cleanup finished"));

        Task firstTask = restGetTask(task1);
        Task lastTask = restGetTask(task2);
        Duration timeToFinish6TasksBy100msEach = Duration.between(firstTask.getCreated(), lastTask.getModified());
        assertTrue(timeToFinish6TasksBy100msEach.compareTo(Duration.ofMillis(600)) >= 0);
    }

    @Test
    void concurrency_10_should_run_10_methods_in_parallel() {
        resetServer();
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        pushTask("Customer wants to make an order");
        long lastTaskId = pushTask("Customer wants to make an order");
        assertEquals("Order accepted", waitTaskStatus(lastTaskId, "Order accepted"));
        Long timeSpendMs = Utils.selectLongValue(
                "SELECT (extract(EPOCH FROM max(modified) - min(created)) * 1000)::INT FROM tasks");
        Long timeSleeped = Utils.selectLongValue("SELECT sum(duration_ms) FROM task_logs");
        assertTrue(timeSpendMs < 800);
        assertTrue(timeSleeped >= 1000);
    }

    @Test
    void task_should_be_instantly_processed_by_different_classes() {
        resetServer();
        long taskId = pushTask("Want a coffee");
        assertEquals("Payed", waitTaskStatus(taskId, "Payed"));
    }

    @Test
    void task_should_be_instantly_processed_by_different_methods() {
        resetServer();
        long taskId = pushTask("Need bubblegum");
        assertEquals("Sayed goodbay", waitTaskStatus(taskId, "Sayed goodbay"));
    }

    @Test
    void delayed_method_should_be_called_after_delay() throws Exception {
        resetServer();
        long taskId = pushTask("Receipt print started");
        Instant deadLine = Instant.now().plus(Duration.ofSeconds(5));
        while (Instant.now().isBefore(deadLine)) {
            checkReadyTasks();
            Thread.sleep(200);
            Task task = restGetTask(taskId);
            if ("Receipt printed".equals(task.getStatus())) {
                break;
            }
        }
        Task task = restGetTask(taskId);
        assertEquals("Receipt printed", task.getStatus());
        Instant processed = Instant.ofEpochMilli(
                Utils.selectLongValue("SELECT (extract(EPOCH FROM min(created)) * 1000)::BIGINT FROM task_logs"));
        long actualDelay = Duration.between(task.getCreated(), processed).toMillis();
        assertTrue(actualDelay >= 2000);
        // 200ms latency of calling checkReadyTasks(),
        // and more 200ms transaction toll
        assertTrue(actualDelay < 2400);
    }

    @Test
    void on_exception_data_should_not_be_saved() throws Exception {
        resetServer();
        long taskId = pushTask("Should send email");
        Instant deadLine = Instant.now().plus(Duration.ofSeconds(2));
        while (Instant.now().isBefore(deadLine)) {
            if (loadDemoLogs(taskId).size() > 2) {
                Thread.sleep(100); // letting transactions finish
                break;
            }
            Thread.sleep(100);
        }
        List<DemoLogRow> logs = loadDemoLogs(taskId);
        assertEquals(3, logs.size());
        DemoLogRow taskStart = logs.get(0);
        DemoLogRow carLoaded = logs.get(1);
        DemoLogRow taskStop = logs.get(2);
        assertEquals(taskStart.txId, carLoaded.txId);
        assertNotEquals(taskStart.txId, taskStop.txId);
    }

    @Test
    void on_exception_extra_json_appender_should_append_to_ext() throws Exception {
        resetServer();
        long taskId1 = pushTask("Should send email");
        long taskId2 = pushTask("Should check email");
        Thread.sleep(100);

        // taskId1 - throws Exception - extra_json should not have "e2":
        // "RuntimeException" value
        String extraJson1 = selectStringValue("SELECT extra_json::VARCHAR FROM task_logs WHERE task_id = " + taskId1);
        assertTrue(extraJson1.contains("\"e1\": \"Exception\""), extraJson1);
        assertFalse(extraJson1.contains("e2"), extraJson1);

        // taskId2 - throws RuntimeExcpetion - extra_json should have both "Exceoption",
        // and "RuntimeExeption" lines
        String extraJson2 = selectStringValue("SELECT extra_json::VARCHAR FROM task_logs WHERE task_id = " + taskId2);
        assertTrue(extraJson2.contains("\"e1\": \"Exception\""), extraJson2);
        assertTrue(extraJson2.contains("\"e2\": \"RuntimeException\""), extraJson2);
    }

    @Test
    void no_onException_method_should_be_retried_in_5_min() throws Exception {
        resetServer();
        pushTask("Should send email");
        Thread.sleep(200);
        assertEquals(1, Utils.selectLongValue("SELECT count(*) FROM task_logs"));

        Utils.sqlUpdate("UPDATE tasks SET modified = modified - '5min'::INTERVAL"); // Pretending task was updated long
                                                                                    // ago
        reloadQueues();

        Thread.sleep(200);
        assertEquals(2, Utils.selectLongValue("SELECT count(*) FROM task_logs"));
    }

    @Test
    void onException_should_setStatus_on_retry_timeout_elapsed() throws Exception {
        resetServer();
        long taskId = pushTask("Should check email");
        Instant deadline = Instant.now().plus(Duration.ofSeconds(11));
        while (deadline.isAfter(Instant.now())) {
            Thread.sleep(200);
            checkReadyTasks();
        }
        assertEquals("Email check failed", waitTaskStatus(taskId, "Email check failed"));
        assertEquals(6, Utils.selectLongValue("SELECT count(*) FROM task_logs"));
    }

    @Test
    void queues__should_grow_and_shrink() throws Exception {
        resetServer();
        pauseServer();
        LinkedList<Long> idList1 = new LinkedList<>(); // Failing but ready for retry
        LinkedList<Long> idList2 = new LinkedList<>(); // Asap tasks
        LinkedList<Long> idList3 = new LinkedList<>(); // Failing but not ready for retry
        for (int i = 0; i < 150; i++) {
            idList1.add(pushTask("Should send email"));
        }
        sqlUpdate("UPDATE tasks SET modified = now() - '6 min'::INTERVAL, " +
                "failing_since = now() - '10 min'::INTERVAL, " +
                "created = now() - '12 min'::INTERVAL");
        for (int i = 0; i < 300; i++) {
            idList2.add(pushTask("Customer wants to make an order"));
        }
        sqlUpdate("UPDATE tasks SET modified = now() - '6 min'::INTERVAL, " +
                "created = now() - '11 min'::INTERVAL " +
                "WHERE id >= " + idList2.peekFirst());
        for (int i = 0; i < 150; i++) {
            idList3.add(pushTask("Should send email"));
        }
        sqlUpdate("UPDATE tasks SET modified = now() - '1 min'::INTERVAL, " +
                "failing_since = now() - '10 min'::INTERVAL, " +
                "created = now() - '10 min'::INTERVAL " +
                "WHERE id >= " + idList3.peekFirst());
        Path quarkusOut = quarkusDir.resolve("quarkus-app-out.txt");
        int linesToSkip = Files.readAllLines(quarkusOut).size();
        resumeServer(false);
        for (Long id : idList2) {
            waitTaskStatus(id, "Order accepted");
        }
        Thread.sleep(250);
        checkReadyTasks();
        Thread.sleep(250);
        checkReadyTasks();
        Thread.sleep(250);
        List<List<Integer>> limitsTable = Files.lines(quarkusOut)
            .skip(linesToSkip)
            .filter(line -> line.contains(" Limits: "))
            .map(this::parseLimitsLine)
            .collect(Collectors.toList());

        // All readyToRetry and asapTasks should be updated exactly 1 time
        // No one failed but not ready task should be updated
        for (Long id : idList1) {
            assertEquals(1L, selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = " + id));
        }
        for (Long id : idList2) {
            assertEquals(1L, selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = " + id));
        }
        for (Long id : idList3) {
            assertEquals(0L, selectLongValue("SELECT count(*) FROM task_logs WHERE task_id = " + id));
        }
        // All 150 readyToRetry should be updated first, but few of ASAP tasks may also be updated
        // (When readyToRetry queue become empty and DoerService start loading bigger queue from DB
        // it continues processing other queues - in our case ASAP queue).
        Long lastReadyToRetryLog = selectLongValue("SELECT max(id) FROM task_logs WHERE task_id <= " + idList1.peekLast());
        Long aheadOfTime = selectLongValue(
                "SELECT count(*) FROM task_logs WHERE id < " + lastReadyToRetryLog + " AND task_id >= " + idList2.peekFirst());
        int max_ahead_of_time = 50;
        // DUMP logs just for debug purpose
        if (aheadOfTime > max_ahead_of_time) {
            String sql = "select json_agg(row_to_json(x)) from (select task_id, created, final_status, duration_ms from task_logs order by created) x;";
            String logs = selectStringValue(sql);
            System.out.println(logs);
        }
        System.out.println("Limits");
        for (List<Integer> limits : limitsTable) {
            System.out.println(limits);
        }
        assertTrue(aheadOfTime <= max_ahead_of_time, "Expected only few ASAP task updated ahead of time " + aheadOfTime + " <= " + max_ahead_of_time);

        List<Integer> firstRow = limitsTable.get(0);
        List<Integer> middleRow = limitsTable.get(limitsTable.size() / 2);
        List<Integer> lastRow = limitsTable.get(limitsTable.size() - 1);
        for (int i = 0; i < firstRow.size(); i++) {
            assertEquals(10, firstRow.get(i), "Test should start with minimal limits for all queues");
        }
        int retryQueueIndex = maxValueIndex(middleRow);
        int asapQueuIndex = maxValueIndex(lastRow);
        for (List<Integer> row : limitsTable) {
            for (int i = 0; i < row.size(); i++) {
                int value = row.get(i);
                assertTrue(value >= 10, "Minimal limit value should be 10. But found " + value);
                if (i != retryQueueIndex && i != asapQueuIndex) {
                    assertEquals(10, value, "Queues, not affected by the test, shold have limit = 10. But found " + value);
                }
            }
        }
        assertTrue(firstRow.get(retryQueueIndex) < middleRow.get(retryQueueIndex), "RedyToRetry queue limits should groow til middleRow");
        assertTrue(middleRow.get(retryQueueIndex) > lastRow.get(retryQueueIndex),
                "RedyToRetry queue limits should shrink from middleRow til lastRow");
        assertEquals(firstRow.get(asapQueuIndex), middleRow.get(asapQueuIndex), "Asap queue shold remain minimal til middleRow");
        assertTrue(middleRow.get(asapQueuIndex) < lastRow.get(asapQueuIndex), "Asap queue should grow from middleRow til lastRow");
    }

    private int maxValueIndex(List<Integer> row) {
        int maxValue = row.get(0);
        int indexOfMaxValue = 0;
        for (int i = 1; i < row.size(); i++) {
            int value = row.get(i);
            if (value > maxValue) {
                maxValue = value;
                indexOfMaxValue = i;
            }
        }
        return indexOfMaxValue;
    }

    List<Integer> parseLimitsLine(String line) {
        String[] arr = line.split("Limits: \\[")[1].split("]")[0].split(",\\s*");
        ArrayList<Integer> result = new ArrayList<>();
        for (String s : arr) {
            result.add(Integer.valueOf(s));
        }
        return result;
    }

    void pauseServer() {
        given()
                .when()
                .get("doer/stop")
                .then()
                .statusCode(200)
                .body("status", equalTo("Doer Stopped"));
    }

    void resumeServer(boolean enableMonitor) {
        given()
                .when()
                .queryParam("m", enableMonitor)
                .get("doer/start")
                .then()
                .statusCode(200)
                .body("status", equalTo("Doer Started"));
    }

    void resetServer() {
        given()
                .when()
                .get("doer/reset")
                .then()
                .statusCode(200)
                .body("status", equalTo("Doer Reset"));
    }

    void checkReadyTasks() {
        given()
                .when()
                .get("doer/check")
                .then()
                .statusCode(200)
                .body("status", equalTo("check"));
    }

    void reloadQueues() {
        given()
                .when()
                .get("doer/load")
                .then()
                .statusCode(200)
                .body("status", equalTo("Doer Reloaded"));
    }

    long pushTask(String status) {
        return given()
                .when()
                .queryParam("s", status)
                .get("doer/add_task")
                .then()
                .statusCode(200)
                .body("status", equalTo(status))
                .extract()
                .jsonPath()
                .getLong("id");
    }

    String waitTaskStatus(long id, String status) {
        while (true) {
            Task task = restGetTask(id);
            if (Objects.equals(status, task.getStatus())) {
                return task.getStatus();
            }
            if (task.getModified().plus(Duration.ofSeconds(2)).isBefore(Instant.now())) {
                return task.getStatus();
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return task.getStatus();
            }
        }
    }

    Task restGetTask(Long taskId) {
        JsonPath json = given()
                .when()
                .queryParam("id", taskId)
                .get("doer/task")
                .then()
                .statusCode(200)
                .extract()
                .jsonPath();
        Task task = new Task();
        task.setId(json.getLong("id"));
        task.setCreated(Instant.parse(json.getString("created")));
        task.setModified(Instant.parse(json.getString("modified")));
        if (json.getString("failingSince") != null) {
            task.setFailingSince(Instant.parse(json.getString("failingSince")));
        } else {
            task.setFailingSince(null);
        }
        task.setStatus(json.getString("status"));
        task.setInProgress(json.getBoolean("inProgress"));
        task.setVersion(json.getInt("version"));
        return task;
    }

    static class DemoLogRow {
        long id;
        String type;
        Long taskId;
        Boolean inProgress;
        String txId;
    }

    DemoLogRow readDemoLogRow(ResultSet rs) throws SQLException {
        DemoLogRow row = new DemoLogRow();
        row.id = rs.getLong("id");
        row.type = rs.getString("object_type");
        row.taskId = rs.getLong("task_id");
        row.inProgress = rs.getBoolean("in_progress");
        row.txId = rs.getString("tx_id");
        return row;
    }

    List<DemoLogRow> loadDemoLogs(long taskId) {
        ArrayList<DemoLogRow> list = new ArrayList<>();
        try (Connection con = Utils.getPostgresDataSource().getConnection();
                PreparedStatement pst = con
                        .prepareStatement("SELECT * FROM demo_log_tasks WHERE task_id = ? ORDER BY id")) {
            pst.setLong(1, taskId);
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    list.add(readDemoLogRow(rs));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }
}
