package com.doer.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Result of a command run by {@link Processes#run}. */
public record RunResult(String stdOut, String stdErr, int status, long runMilliseconds) {

    /** Asserts the exit status; on mismatch the failure message has the command output. */
    public RunResult assertStatus(int expected) {
        assertEquals(expected, status, this::toString);
        return this;
    }

    @Override
    public String toString() {
        return "status: " + status + ", time: " + runMilliseconds + " ms\n"
                + "----- stdout -----\n" + stdOut
                + "----- stderr -----\n" + stdErr
                + "------------------";
    }
}
