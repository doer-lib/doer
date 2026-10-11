package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.ConcurrencyGroup;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;

/** The concurrency group of {@link ConcurrencyGroupFirst} on a method of another class. */
@ApplicationScoped
public class ConcurrencyGroupSecond {
    @ConcurrencyGroup("Concurrency group")
    @AcceptStatus("Concurrency group second")
    public void slow(Task task) throws Exception {
        Thread.sleep(100);
        task.setStatus("Concurrency group second done");
    }
}
