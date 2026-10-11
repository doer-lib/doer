package transitsims;

import com.doer.DoerService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Shutdown;
import jakarta.enterprise.event.Startup;
import jakarta.inject.Inject;

/** Starts Doer, with the monitor of delayed tasks, when the application starts, and stops it when the application stops. */
@ApplicationScoped
public class DoerLifecycle {
    @Inject
    DoerService doerService;

    void onAppStart(@Observes Startup event) {
        doerService.start(true);
    }

    void onAppStop(@Observes Shutdown event) {
        doerService.stop();
    }
}
