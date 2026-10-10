package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;

/** Statuses of doer methods for DoerMethodsE2E: a task handed over to another class, and a delayed status. */
@ApplicationScoped
public class DoerMethodStatuses {
    @AcceptStatus("Doer method hand over")
    public void handOver(Task task) {
        task.setStatus("Doer method next class");
    }

    @AcceptStatus(value = "Doer method delayed", delay = "2s")
    public void delayed(Task task) {
        task.setStatus("Doer method delayed done");
    }
}
