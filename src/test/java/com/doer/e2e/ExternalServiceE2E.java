package com.doer.e2e;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.tomakehurst.wiremock.client.WireMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A doer method calls an external service, simulated by WireMock, through the JAX-RS client of the runtime; its URL
 * comes from an environment variable. See {@code transitsims.validation.ExternalServiceMethods}.
 */
class ExternalServiceE2E extends E2eTestBase {
    static final String TASKS = "/external/tasks/\\d+";

    @BeforeEach
    void reset() {
        WireMock.reset();
        resetServer();
    }

    @Test
    void successful_call_should_move_task_on() {
        stubFor(get(urlPathMatching(TASKS)).willReturn(aResponse().withStatus(200)));

        long taskId = pushTask("External service call");

        assertEquals("External service called", waitTaskStatus(taskId, "External service called"));
        verify(1, getRequestedFor(urlEqualTo("/external/tasks/" + taskId)));
    }

    @Test
    void error_response_should_make_task_failing() throws Exception {
        stubFor(get(urlPathMatching(TASKS)).willReturn(aResponse().withStatus(503)));

        long taskId = pushTask("External service call");

        assertEquals("IllegalStateException", waitTaskLog(taskId));
        RestTask task = restGetTask(taskId);
        assertEquals("External service call", task.status());
        assertNotNull(task.failingSince());
        verify(1, getRequestedFor(urlEqualTo("/external/tasks/" + taskId)));
    }

    @Test
    void timeout_should_make_task_failing() throws Exception {
        stubFor(get(urlPathMatching(TASKS)).willReturn(aResponse().withStatus(200).withFixedDelay(5000)));

        long taskId = pushTask("External service call");

        assertTrue(waitTaskLog(taskId).endsWith("ProcessingException"), "a ProcessingException of JAX-RS");
        RestTask task = restGetTask(taskId);
        assertEquals("External service call", task.status());
        assertNotNull(task.failingSince());
        long durationMs = selectLongValue("SELECT duration_ms FROM task_logs WHERE task_id = " + taskId);
        assertTrue(durationMs >= 1000 && durationMs < 5000, "the read timeout of 1 s ends the call: " + durationMs);
    }

    /** Waits for the first task log of the task; returns the simple name of its exception type. */
    private static String waitTaskLog(long taskId) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            String type = selectStringValue("SELECT coalesce(exception_type, 'none') FROM task_logs WHERE task_id = "
                    + taskId);
            if (type != null) {
                return type.substring(type.lastIndexOf('.') + 1);
            }
            Thread.sleep(100);
        }
        return fail("No task log of task " + taskId + " in 10 s");
    }
}
