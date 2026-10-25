package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Statuses of doer methods for DoerMethodsE2E: a task handed over to another class, a delayed status, and statuses set
 * from constants of another class, from a {@code switch} and from a lambda.
 */
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

    @AcceptStatus("Doer method constant")
    public void fromConstant(Task task) {
        task.setStatus(DoerMethodStatusNames.SWITCH);
    }

    @AcceptStatus(DoerMethodStatusNames.SWITCH)
    public void fromSwitch(Task task) {
        task.setStatus(switch (task.getStatus()) {
            case DoerMethodStatusNames.SWITCH -> DoerMethodStatusNames.LAMBDA;
            default -> "Doer method switch unexpected";
        });
    }

    @AcceptStatus(DoerMethodStatusNames.LAMBDA)
    public void fromLambda(Task task) {
        Runnable setter = () -> task.setStatus("Doer method sources done");
        setter.run();
    }
}
