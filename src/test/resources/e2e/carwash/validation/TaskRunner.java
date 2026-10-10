package carwash.validation;

import com.doer.DoerService;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;
import java.io.StringReader;
import java.time.Duration;

/**
 * Runs one task through {@code runTask} of the generated service, for GeneratedCodeTest: in e2e through
 * {@code POST /api/validation/run-task}, in the processor tests through {@code carwash.Main}.
 * <p>
 * Request: {@code {"status": "Generated code 1 param", "failingFor": "PT25H"}}; {@code failingFor} is optional, how long
 * ago the task started failing. Response: {@code {"status": ..., "failing": true|false, "trace": [calls]}}.
 */
@ApplicationScoped
public class TaskRunner {
    DoerService doerService;
    CallTrace callTrace;

    @Inject
    public void setDoerService(DoerService doerService) {
        this.doerService = doerService;
    }

    @Inject
    public void setCallTrace(CallTrace callTrace) {
        this.callTrace = callTrace;
    }

    public String run(String request) throws Exception {
        JsonObject json;
        try (JsonReader reader = Json.createReader(new StringReader(request))) {
            json = reader.readObject();
        }
        Task task = new Task();
        task.setStatus(json.getString("status"));
        // Inserted in progress, so that Doer's scheduler does not take it: only runTask runs it
        task.setInProgress(true);
        if (json.containsKey("failingFor")) {
            Duration failingFor = Duration.parse(json.getString("failingFor"));
            task.setFailingSince(doerService.getDbNow().minus(failingFor));
        }
        doerService.insert(task);
        doerService.runTask(task);

        JsonObjectBuilder response = Json.createObjectBuilder();
        if (task.getStatus() == null) {
            response.addNull("status");
        } else {
            response.add("status", task.getStatus());
        }
        response.add("failing", task.getFailingSince() != null);
        response.add("trace", Json.createArrayBuilder(callTrace.take(task.getId())));
        return response.build().toString();
    }
}
