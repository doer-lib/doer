package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** A doer method with an interceptor binding: the generated service calls it through the interceptor. */
@ApplicationScoped
public class BeanKindIntercepted {
    CallTrace callTrace;

    @Inject
    public void setCallTrace(CallTrace callTrace) {
        this.callTrace = callTrace;
    }

    @BeanKindTraced
    @AcceptStatus("Bean kind intercepted")
    public void run(Task task) {
        callTrace.add(task, "BeanKindIntercepted.run");
        task.setStatus("Bean kind intercepted done");
    }
}
