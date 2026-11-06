package transitsims;

import com.doer.DoerService;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;

/**
 * The Helidon version of DoerLifecycle. Helidon starts its web server in an observer of the initialization of the
 * application context (priority PLATFORM_AFTER + 100), and Weld fires Startup after it: on Startup the application
 * accepts requests before Doer is started. So Doer starts on the initialization, before the web server, and stops
 * when the application context is about to be destroyed, after the web server has stopped (priority PLATFORM_BEFORE).
 * Copied by the e2e suite (HelidonApp).
 */
@ApplicationScoped
public class DoerLifecycle {
    @Inject
    DoerService doerService;

    void onAppStart(@Observes @Priority(Interceptor.Priority.APPLICATION) @Initialized(ApplicationScoped.class)
                    Object event) {
        doerService.start();
    }

    void onAppStop(@Observes @Priority(Interceptor.Priority.APPLICATION) @BeforeDestroyed(ApplicationScoped.class)
                   Object event) {
        doerService.stop();
    }
}
