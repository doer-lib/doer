package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;

/** Takes over the task handed over by {@link DoerMethodStatuses}. */
@ApplicationScoped
public class DoerMethodNextClass {
    @AcceptStatus("Doer method next class")
    public void takeOver(Task task) {
        task.setStatus("Doer method next class done");
    }
}
