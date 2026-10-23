package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Holds {@link Nested}: a bean that is a public static nested class. */
public final class BeanKinds {
    private BeanKinds() {
    }

    @ApplicationScoped
    public static class Nested {
        CallTrace callTrace;

        @Inject
        public void setCallTrace(CallTrace callTrace) {
            this.callTrace = callTrace;
        }

        @AcceptStatus("Bean kind nested")
        public void run(Task task) {
            callTrace.add(task, "BeanKinds.Nested.run");
            task.setStatus("Bean kind nested done");
        }
    }
}
