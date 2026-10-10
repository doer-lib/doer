package carwash.validation;

import com.doer.AcceptStatus;
import com.doer.RetryPolicy;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Doer methods that always fail, for GeneratedCodeTest: the status after a failure depends on failingSince. */
@ApplicationScoped
public class GeneratedCodeFailures {
    CallTrace callTrace;

    @Inject
    public void setCallTrace(CallTrace callTrace) {
        this.callTrace = callTrace;
    }

    @AcceptStatus("Generated code checked exception")
    public void checkedException(Task task, GeneratedCodeFirstData first) throws Exception {
        callTrace.add(task, "GeneratedCodeFailures.checkedException(" + first + ")");
        task.setStatus("Generated code checked exception done");
        throw new Exception("Generated code checked exception");
    }

    @AcceptStatus("Generated code runtime exception")
    public void runtimeException(Task task) {
        callTrace.add(task, "GeneratedCodeFailures.runtimeException");
        task.setStatus("Generated code runtime exception done");
        throw new IllegalStateException("Generated code runtime exception");
    }

    @AcceptStatus("Generated code retry policy")
    @RetryPolicy(interval = "1m", duration = "1h", fallbackStatus = "Generated code retry policy fallback")
    public void retryPolicy(Task task) {
        callTrace.add(task, "GeneratedCodeFailures.retryPolicy");
        task.setStatus("Generated code retry policy done");
        throw new IllegalStateException("Generated code retry policy");
    }
}
