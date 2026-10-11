package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.TimeUnit;

/**
 * A doer method that calls an external service through the JAX-RS client API, for ExternalServiceE2E. The task moves
 * on when the service answers 2xx, and fails on an error response or after the read timeout of 1 s. The client is
 * created on first use, so the bean can be constructed without a JAX-RS implementation.
 */
@ApplicationScoped
public class ExternalServiceMethods {
    static final long READ_TIMEOUT_MS = 1000;

    ExternalServiceConfig config;
    private Client client;

    @Inject
    public void setConfig(ExternalServiceConfig config) {
        this.config = config;
    }

    @AcceptStatus("External service call")
    public void call(Task task) {
        try (Response response = client().target(config.externalUrl())
                .path("external/tasks/{id}")
                .resolveTemplate("id", task.getId())
                .request()
                .get()) {
            if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
                throw new IllegalStateException("External service answered " + response.getStatus());
            }
        }
        task.setStatus("External service called");
    }

    private synchronized Client client() {
        if (client == null) {
            client = ClientBuilder.newBuilder()
                    .connectTimeout(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .readTimeout(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .build();
        }
        return client;
    }
}
