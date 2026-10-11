package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.Task;
import jakarta.inject.Inject;

/**
 * An abstract class with a doer method: the generated service injects it by this type and gets
 * {@link BeanKindInherited}, its only bean.
 */
public abstract class BeanKindBase {
    CallTrace callTrace;

    @Inject
    public void setCallTrace(CallTrace callTrace) {
        this.callTrace = callTrace;
    }

    @AcceptStatus("Bean kind inherited")
    public void run(Task task) {
        callTrace.add(task, "BeanKindBase.run in " + name());
        task.setStatus("Bean kind inherited done");
    }

    protected abstract String name();
}
