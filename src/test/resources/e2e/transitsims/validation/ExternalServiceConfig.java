package transitsims.validation;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * URLs of external services from environment variables: MicroProfile Config is missing in some runtimes. In e2e the
 * external service is WireMock.
 */
@ApplicationScoped
public class ExternalServiceConfig {
    /** {@code E2E_EXTERNAL_URL}; the default is for a run from the IDE (doer.e2e.runtime=external). */
    public String externalUrl() {
        String url = System.getenv("E2E_EXTERNAL_URL");
        return url == null ? "http://localhost:8089" : url;
    }
}
