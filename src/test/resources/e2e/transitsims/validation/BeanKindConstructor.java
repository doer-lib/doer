package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * A bean with constructor injection. The client proxy of the scope needs a no-arg constructor as well; it is
 * protected, so that only the container calls it.
 */
@ApplicationScoped
public class BeanKindConstructor {
    final CallTrace callTrace;

    @Inject
    public BeanKindConstructor(CallTrace callTrace) {
        this.callTrace = callTrace;
    }

    protected BeanKindConstructor() {
        this(null);
    }

    @AcceptStatus("Bean kind constructor")
    public void run(Task task) {
        callTrace.add(task, "BeanKindConstructor.run");
        task.setStatus("Bean kind constructor done");
    }
}
