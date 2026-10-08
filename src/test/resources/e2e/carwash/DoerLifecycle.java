package carwash;

import com.doer.DoerService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Shutdown;
import jakarta.enterprise.event.Startup;
import jakarta.inject.Inject;

/** Starts Doer when the application starts and stops it when the application stops. */
@ApplicationScoped
public class DoerLifecycle {
    @Inject
    DoerService doerService;

    void onAppStart(@Observes Startup event) {
        doerService.start(false);
    }

    void onAppStop(@Observes Shutdown event) {
        doerService.stop();
    }
}
