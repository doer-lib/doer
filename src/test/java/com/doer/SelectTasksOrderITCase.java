package com.doer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.io.CleanupMode.NEVER;

import com.doer.testkit.InWorkspace;
import com.doer.testkit.Toolchain;
import com.doer.testkit.Workspaces;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The sub-queues of the generated service match the selects of the generated SelectTasks.sql one by one: DoerService
 * gives the LIMIT of each sub-queue by its position, and puts the rows of a select to the sub-queue at its position.
 * The order is set in two places, {@code DoerProcessor.generateSelectTaskSql} and the constructor of
 * {@link ConcurrencyDomain}.
 * <p>
 * The generated service is loaded from the compiled classes of the workspace, with this build's DoerService as its
 * superclass.
 */
public class SelectTasksOrderITCase implements InWorkspace {

    /** Delays and retry intervals whose order as durations differs from their order as strings. */
    static final String BILLING = """
            package demo.order;
            import com.doer.*;
            public class Billing {
                @AcceptStatus("NEW")
                @AcceptStatus(value = "REMIND", delay = "2m")
                @AcceptStatus(value = "ARCHIVE", delay = "90s")
                @RetryPolicy(interval = "1m")
                public void bill(Task task) {
                    task.setStatus("BILLED");
                }

                @AcceptStatus("CHARGE")
                @AcceptStatus(value = "RECHARGE", delay = "90s")
                @RetryPolicy(interval = "20s")
                public void charge(Task task) {
                    task.setStatus("PAID");
                }
            }
            """;
    /** The default retry interval (5 minutes) between two declared ones; a domain without delayed statuses. */
    static final String SHIPPING = """
            package demo.order;
            import com.doer.*;
            public class Shipping {
                @AcceptStatus("PACK")
                public void pack(Task task) {
                    task.setStatus("DELIVER");
                }

                @AcceptStatus("DELIVER")
                @RetryPolicy(interval = "10m")
                public void deliver(Task task) {
                    task.setStatus("TRACK");
                }

                @AcceptStatus("TRACK")
                @RetryPolicy(interval = "2m")
                public void track(Task task) {
                    task.setStatus("DONE");
                }
            }
            """;

    /** One select of SelectTasks.sql, or the select a sub-queue expects. */
    record Select(boolean failing, Set<String> statuses, String orderBy) {
        Select {
            statuses = new TreeSet<>(statuses);
        }

        static Select of(SubQueue queue) {
            return new Select(queue.isFailing(), queue.statuses(), queue.isAsap() ? "created" : "modified");
        }
    }

    static final Pattern SELECT = Pattern.compile("\\(SELECT \\* FROM tasks WHERE NOT in_progress AND failing_since "
            + "(IS NULL|IS NOT NULL) AND status IN \\((.*?)\\) ORDER BY (\\w+) LIMIT \\?\\)", Pattern.DOTALL);
    static final Pattern STATUS = Pattern.compile("'((?:[^']|'')*)'");

    @TempDir(factory = Workspaces.class, cleanup = NEVER)
    Path workspace;

    @Override
    public Path getWorkspace() {
        return workspace;
    }

    @Test
    void queues__should_match_selects_of_SelectTasks_sql() throws Exception {
        writeSource("Billing.java", BILLING);
        writeSource("Shipping.java", SHIPPING);
        javac("Billing.java", "Shipping.java").assertStatus(0);

        List<Select> selects = parseSelects(Files.readString(generated("SelectTasks.sql")));
        List<Select> queues;
        try (URLClassLoader loader = generatedClassLoader()) {
            DoerService service = (DoerService) loader.loadClass("com.doer.generated._GeneratedDoerService")
                    .getDeclaredConstructor().newInstance();
            queues = service.queues().stream().map(Select::of).toList();
        }

        assertEquals(selects, queues);
        assertEquals(9, queues.size(), "Billing: asap, 90s, 2m, retry 20s, 1m; Shipping: asap, retry 2m, 5m, 10m");
    }

    private static List<Select> parseSelects(String sql) {
        List<Select> result = new ArrayList<>();
        Matcher select = SELECT.matcher(sql);
        while (select.find()) {
            List<String> statuses = new ArrayList<>();
            Matcher status = STATUS.matcher(select.group(2));
            while (status.find()) {
                statuses.add(status.group(1).replace("''", "'"));
            }
            result.add(new Select("IS NOT NULL".equals(select.group(1)), Set.copyOf(statuses), select.group(3)));
        }
        return result;
    }

    /**
     * Loads the classes compiled in the workspace. The classes of com.doer, the superclass of the generated service
     * among them, are this build's, from the parent class loader; the Jakarta API is added for the annotations and
     * the signatures of the generated service.
     */
    private URLClassLoader generatedClassLoader() throws Exception {
        List<URL> urls = new ArrayList<>();
        urls.add(workspace.resolve("classes").toUri().toURL());
        for (String jar : Toolchain.compileClasspath().split(File.pathSeparator)) {
            urls.add(Path.of(jar).toUri().toURL());
        }
        return new URLClassLoader(urls.toArray(URL[]::new), getClass().getClassLoader());
    }

    @Test
    void parseSelects__should_read_failing_flag_statuses_and_order() {
        String sql = """
                (SELECT * FROM tasks WHERE NOT in_progress AND failing_since IS NULL AND status IN (
                  'A',
                  'B''s'
                ) ORDER BY created LIMIT ?)
                UNION ALL
                (SELECT * FROM tasks WHERE NOT in_progress AND failing_since IS NOT NULL AND status IN (
                  'A'
                ) ORDER BY modified LIMIT ?)
                UNION ALL
                (SELECT * FROM tasks WHERE in_progress)
                """;

        assertEquals(List.of(new Select(false, Set.of("A", "B's"), "created"),
                new Select(true, Set.of("A"), "modified")), parseSelects(sql));
    }
}
