package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.ConcurrencyGroup;
import com.doer.ConcurrencyLimit;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * The concurrency group "Concurrency group" on a class, with the limit of the group: one task at a time, together
 * with the method of {@link ConcurrencyGroupSecond} in the same group. For ConcurrencyE2E; each method takes 100 ms.
 */
@ConcurrencyGroup("Concurrency group")
@ConcurrencyLimit(1)
@ApplicationScoped
public class ConcurrencyGroupFirst {
    @AcceptStatus("Concurrency group first")
    public void slow(Task task) throws Exception {
        Thread.sleep(100);
        task.setStatus("Concurrency group first done");
    }
}
