package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.Task;

/**
 * A bean created by {@link BeanKindProducers}: the class has no bean-defining annotation and no constructor a container
 * could call, so the generated service gets the produced instance.
 */
public class BeanKindProduced {
    final CallTrace callTrace;
    final String producedBy;

    public BeanKindProduced(CallTrace callTrace, String producedBy) {
        this.callTrace = callTrace;
        this.producedBy = producedBy;
    }

    @AcceptStatus("Bean kind produced")
    public void run(Task task) {
        callTrace.add(task, "BeanKindProduced.run(" + producedBy + ")");
        task.setStatus("Bean kind produced done");
    }
}
