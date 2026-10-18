package com.doer.e2e;

import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.BlkioStatEntry;
import com.github.dockerjava.api.model.Statistics;
import com.github.dockerjava.api.model.StatisticNetworksConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.testcontainers.DockerClientFactory;

/**
 * Resources a container of the application used from its start: CPU time, disk I/O and network I/O, as
 * {@code docker stats} counts them (the cgroup of the container). Measured just before the node is stopped: a
 * container of {@code docker run --rm} has no statistics after it exits, so its shutdown is not counted.
 *
 * @param container  name of the container
 * @param node       the node the container ran
 * @param cpuNanos   CPU time, user and system, in nanoseconds
 * @param diskRead   bytes read from block devices
 * @param diskWrite  bytes written to block devices
 * @param netIn      bytes received on all interfaces
 * @param netOut     bytes sent on all interfaces
 */
record ContainerUsage(String container, int node, long cpuNanos, long diskRead, long diskWrite, long netIn,
        long netOut) {
    static final int STATS_TIMEOUT_SECONDS = 10;

    /**
     * Reads the statistics of the running container from the Docker API; {@code stream=false} makes Docker wait for a
     * second sample, about a second.
     */
    static ContainerUsage measure(String container, int node) throws InterruptedException {
        Statistics[] stats = new Statistics[1];
        try (ResultCallback.Adapter<Statistics> callback = new ResultCallback.Adapter<>() {
            @Override
            public void onNext(Statistics statistics) {
                stats[0] = statistics;
            }
        }) {
            DockerClientFactory.instance().client().statsCmd(container).withNoStream(true).exec(callback)
                    .awaitCompletion(STATS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Statistics s = Objects.requireNonNull(stats[0], "No statistics of the container " + container);
        Long cpu = s.getCpuStats() == null || s.getCpuStats().getCpuUsage() == null ? null
                : s.getCpuStats().getCpuUsage().getTotalUsage();
        // cgroup v1 names the operations Read and Write, cgroup v2 read and write
        List<BlkioStatEntry> io = s.getBlkioStats() == null ? null : s.getBlkioStats().getIoServiceBytesRecursive();
        return new ContainerUsage(container, node, cpu == null ? 0 : cpu, disk(io, "read"), disk(io, "write"),
                net(s.getNetworks(), StatisticNetworksConfig::getRxBytes),
                net(s.getNetworks(), StatisticNetworksConfig::getTxBytes));
    }

    private static long disk(List<BlkioStatEntry> entries, String op) {
        return entries == null ? 0 : entries.stream()
                .filter(e -> op.equalsIgnoreCase(e.getOp()) && e.getValue() != null)
                .mapToLong(BlkioStatEntry::getValue)
                .sum();
    }

    private static long net(Map<String, StatisticNetworksConfig> networks,
            Function<StatisticNetworksConfig, Long> bytes) {
        return networks == null ? 0 : networks.values().stream()
                .map(bytes)
                .filter(Objects::nonNull)
                .mapToLong(Long::longValue)
                .sum();
    }

    /** For the separator line in the logs of the node. */
    String summary() {
        return String.format(Locale.ROOT, "CPU %.1f s, disk read %s, write %s, network in %s, out %s",
                cpuNanos / 1e9, bytes(diskRead), bytes(diskWrite), bytes(netIn), bytes(netOut));
    }

    /** Decimal units, as {@code docker stats}. */
    private static String bytes(long bytes) {
        String[] units = { "B", "kB", "MB", "GB" };
        double value = bytes;
        int unit = 0;
        while (value >= 1000 && unit < units.length - 1) {
            value /= 1000;
            unit++;
        }
        return String.format(Locale.ROOT, unit == 0 ? "%.0f %s" : "%.1f %s", value, units[unit]);
    }

    /**
     * The usage of all containers of the run as JSON, for the test summary of CI (.github/scripts/test-summary.py): the
     * containers and their total.
     */
    static String json(String runtime, List<ContainerUsage> usages) {
        StringBuilder json = new StringBuilder("{\n  \"runtime\": \"" + runtime + "\",\n  \"containers\": [");
        for (int i = 0; i < usages.size(); i++) {
            ContainerUsage u = usages.get(i);
            json.append(i == 0 ? "\n" : ",\n")
                    .append("    {\"container\": \"").append(u.container).append("\", \"node\": ").append(u.node)
                    .append(", ").append(fields(u)).append('}');
        }
        ContainerUsage total = new ContainerUsage("total", 0,
                usages.stream().mapToLong(ContainerUsage::cpuNanos).sum(),
                usages.stream().mapToLong(ContainerUsage::diskRead).sum(),
                usages.stream().mapToLong(ContainerUsage::diskWrite).sum(),
                usages.stream().mapToLong(ContainerUsage::netIn).sum(),
                usages.stream().mapToLong(ContainerUsage::netOut).sum());
        return json.append(usages.isEmpty() ? "],\n" : "\n  ],\n")
                .append("  \"total\": {").append(fields(total)).append("}\n}\n")
                .toString();
    }

    private static String fields(ContainerUsage u) {
        return String.format(Locale.ROOT, "\"cpuSeconds\": %.3f, \"diskReadBytes\": %d, \"diskWriteBytes\": %d, "
                + "\"netInBytes\": %d, \"netOutBytes\": %d", u.cpuNanos / 1e9, u.diskRead, u.diskWrite, u.netIn,
                u.netOut);
    }
}
