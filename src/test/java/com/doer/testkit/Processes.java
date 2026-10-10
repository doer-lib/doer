package com.doer.testkit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Runs commands in a folder and keeps their output there. */
public final class Processes {

    private Processes() {
    }

    /**
     * Runs the command in workDir and keeps its output there: {@code <logName>-cmd.txt}, {@code <logName>-out.txt} and
     * {@code <logName>-err.txt}. The exit status is not checked.
     */
    public static RunResult run(Path workDir, String logName, int timeoutSeconds, List<String> cmd)
            throws InterruptedException, IOException, TimeoutException {
        String commandLine = String.join(" ", cmd);
        Path stdOut = workDir.resolve(logName + "-out.txt");
        Path stdErr = workDir.resolve(logName + "-err.txt");
        Files.writeString(workDir.resolve(logName + "-cmd.txt"), commandLine + "\n");
        long t0 = System.currentTimeMillis();
        Process process = new ProcessBuilder(cmd)
                .redirectOutput(stdOut.toFile())
                .redirectError(stdErr.toFile())
                .directory(workDir.toFile())
                .start();
        if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(100, TimeUnit.MILLISECONDS);
            throw new TimeoutException("Command has not finished in " + timeoutSeconds + " seconds, see "
                    + workDir.resolve(logName + "-*.txt") + ": " + commandLine);
        }
        long runMilliseconds = System.currentTimeMillis() - t0;
        return new RunResult(Files.readString(stdOut), Files.readString(stdErr), process.exitValue(),
                runMilliseconds);
    }
}
