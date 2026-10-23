package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.ConcurrencyGroup;
import com.doer.ConcurrencyLimit;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * The concurrency group of this class, declared with the limit and a reference to the class itself: one task at a
 * time, together with the method of {@link ConcurrencyGroupSecond} that joins the group. For ConcurrencyE2E; each
 * method takes 100 ms.
 */
@ConcurrencyGroup(ConcurrencyGroupFirst.class)
@ConcurrencyLimit(1)
@ApplicationScoped
public class ConcurrencyGroupFirst {
    @AcceptStatus("Concurrency group first")
    public void slow(Task task) throws Exception {
        Thread.sleep(100);
        task.setStatus("Concurrency group first done");
    }
}
