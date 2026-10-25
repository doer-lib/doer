package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.Task;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/** A {@code @Singleton} bean: a pseudo-scope without a client proxy, which Spring and CDI treat differently. */
@Singleton
public class BeanKindSingleton {
    CallTrace callTrace;

    @Inject
    public void setCallTrace(CallTrace callTrace) {
        this.callTrace = callTrace;
    }

    @AcceptStatus("Bean kind singleton")
    public void run(Task task) {
        callTrace.add(task, "BeanKindSingleton.run");
        task.setStatus("Bean kind singleton done");
    }
}
