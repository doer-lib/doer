package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.ConcurrencyLimit;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;

/** One task at a time in the whole class, for ConcurrencyE2E; each method takes 100 ms. */
@ConcurrencyLimit(1)
@ApplicationScoped
public class ConcurrencyLimitOne {
    @AcceptStatus("Concurrency limit 1 first")
    @AcceptStatus("Concurrency limit 1 second")
    public void slow(Task task) throws Exception {
        Thread.sleep(100);
        task.setStatus("Concurrency limit 1 done");
    }

    @AcceptStatus("Concurrency limit 1 other")
    public void otherSlow(Task task) throws Exception {
        Thread.sleep(100);
        task.setStatus("Concurrency limit 1 other done");
    }
}
