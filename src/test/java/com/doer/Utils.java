package com.doer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

public class Utils {

    static PGSimpleDataSource _ds;
    static PostgreSQLContainer<?> _pgContainer;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(Utils::closeAll));
    }

    public static void closeAll() {
        _ds = null;
        if (_pgContainer != null) {
            try {
                _pgContainer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            _pgContainer = null;
        }
    }

    public static String exec(int expectedStatus, Path workDir, String commandTemplate, Object... args)
            throws InterruptedException, IOException, TimeoutException {
        String[] cmd = commandTemplate.split("\\s+");
        LinkedList<Object> argsList = new LinkedList<>(Arrays.asList(args));
        for (int i = 0; i < cmd.length; i++) {
            if ("?".equals(cmd[i])) {
                cmd[i] = String.valueOf(argsList.pollFirst());
            }
        }
        Path stdOut = workDir.resolve("out.txt");
        Path stdErr = workDir.resolve("err.txt");
        int code = exec(workDir.toFile(), stdOut.toFile(), stdErr.toFile(), 60, Arrays.asList(cmd));
        if (expectedStatus != code) {
            System.err.println("Command failed");
            System.err.println(String.join(" ", cmd));
            Files.copy(stdOut, System.out);
            Files.copy(stdErr, System.err);
        }

        String result = new String(Files.readAllBytes(stdOut), UTF_8);
        Files.deleteIfExists(stdOut);
        Files.deleteIfExists(stdErr);
        assertEquals(expectedStatus, code);
        return result;
    }

    public static int exec(File workDir, File stdOut, File stdErr, int timeoutSeconds, List<String> cmd)
            throws InterruptedException, IOException, TimeoutException {
        Process process = new ProcessBuilder(cmd)
                .redirectOutput(stdOut)
                .redirectError(stdErr)
                .directory(workDir)
                .start();
        boolean exited = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
        if (!exited) {
            try {
                process.getOutputStream().close();
                process.destroy();
                process.destroyForcibly();
                process.waitFor(100, TimeUnit.MILLISECONDS);
                Files.copy(stdOut.toPath(), System.out);
                Files.copy(stdErr.toPath(), System.err);
            } catch (Exception e) {
                System.err.println("Ignored exception");
                e.printStackTrace(System.err);
            }
            System.err.println("Command timed out");
            System.err.println(String.join(" ", cmd));
            throw new TimeoutException("Command have not finished in " + timeoutSeconds + " seconds");
        }
        return process.exitValue();
    }

    public static void deleteReqursivelly(Path path) {
        try {
            if (Files.isDirectory(path)) {
                Files.list(path).forEach(Utils::deleteReqursivelly);
            }
            if (Files.exists(path)) {
                Files.delete(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static PGSimpleDataSource getPostgresDataSource() {
        if (_ds != null && _pgContainer != null && _pgContainer.isRunning()) {
            return _ds;
        }
        _ds = null;
        if (_pgContainer != null) {
            _pgContainer.close();
            _pgContainer = null;
        }
        PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:12.3")
                .withDatabaseName("doer")
                .withUsername("doer")
                .withPassword("doer")
                .withNetworkAliases("doerdb");
        container.start();
        _pgContainer = container;
        _ds = new PGSimpleDataSource();
        _ds.setDatabaseName(_pgContainer.getDatabaseName());
        _ds.setServerNames(new String[] { _pgContainer.getHost() });
        _ds.setPortNumbers(new int[] { _pgContainer.getMappedPort(5432) });
        _ds.setUser(_pgContainer.getUsername());
        _ds.setPassword(_pgContainer.getPassword());
        return _ds;
    }

    static void createDbSchema(Connection con) throws SQLException, IOException {
        String sql;
        try (InputStream is = DoerService.class.getResourceAsStream("SchemaForTests.sql");
                Scanner scanner = new Scanner(is, "UTF-8")) {
            sql = scanner.useDelimiter("\\A").next();
        }
        try (PreparedStatement pst = con.prepareStatement(sql)) {
            pst.executeUpdate();
        }
    }

    public static void sqlUpdate(String sql) {
        try (Connection con = getPostgresDataSource().getConnection();
                PreparedStatement pst = con.prepareStatement(sql)) {
            pst.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Long selectLongValue(String sql) {
        try (Connection con = getPostgresDataSource().getConnection();
                PreparedStatement pst = con.prepareStatement(sql);
                ResultSet rs = pst.executeQuery()) {
            if (rs.next()) {
                long x = rs.getLong(1);
                if (!rs.wasNull()) {
                    return x;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String selectStringValue(String sql) {
        try (Connection con = getPostgresDataSource().getConnection();
                PreparedStatement pst = con.prepareStatement(sql);
                ResultSet rs = pst.executeQuery()) {
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
