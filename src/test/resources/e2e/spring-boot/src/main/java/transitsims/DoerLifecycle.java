package transitsims;

import com.doer.DoerService;
import jakarta.inject.Inject;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * The Spring version of DoerLifecycle (Spring does not fire the CDI events Startup and Shutdown). Doer starts, with the
 * monitor of delayed tasks, before Tomcat accepts requests and stops after Tomcat has stopped accepting them, before the
 * DataSource and the Executor are destroyed: the phase is below the ones of the web server. Written by the e2e suite
 * (SpringBootApp).
 */
@Component
public class DoerLifecycle implements SmartLifecycle {
    @Inject
    DoerService doerService;

    volatile boolean running;

    @Override
    public void start() {
        doerService.start(true);
        running = true;
    }

    @Override
    public void stop() {
        doerService.stop();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 0;
    }
}
