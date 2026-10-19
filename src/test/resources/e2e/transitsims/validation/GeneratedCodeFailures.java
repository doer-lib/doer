package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.ExceptionDescriber;
import com.doer.RetryPolicy;
import com.doer.Task;
import com.doer.TaskDataLoader;
import com.doer.TaskDataSaver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.json.JsonObjectBuilder;

/**
 * Doer methods that always fail, for GeneratedCodeTest: the status after a failure depends on failingSince. The
 * failure is in the method itself, in its loader, in its saver, or in the exception describer of what it throws.
 */
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

    @AcceptStatus("Generated code loader exception")
    public void loaderException(Task task, GeneratedCodeFailingData data) {
        callTrace.add(task, "GeneratedCodeFailures.loaderException(" + data + ")");
        task.setStatus("Generated code loader exception done");
    }

    @AcceptStatus("Generated code saver exception")
    public void saverException(Task task, GeneratedCodeFailingData data) {
        callTrace.add(task, "GeneratedCodeFailures.saverException(" + data + ")");
        task.setStatus("Generated code saver exception done");
    }

    @AcceptStatus("Generated code describer error")
    public void describerError(Task task) throws GeneratedCodeUndescribableException {
        callTrace.add(task, "GeneratedCodeFailures.describerError");
        task.setStatus("Generated code describer error done");
        throw new GeneratedCodeUndescribableException("Generated code describer error");
    }

    /** Fails for the task of {@link #loaderException}. */
    @TaskDataLoader
    public GeneratedCodeFailingData loadFailing(Task task) {
        callTrace.add(task, "GeneratedCodeFailures.loadFailing");
        if ("Generated code loader exception".equals(task.getStatus())) {
            throw new IllegalStateException("Generated code loader exception");
        }
        return new GeneratedCodeFailingData();
    }

    @TaskDataSaver
    public void saveFailing(Task task, GeneratedCodeFailingData data) {
        callTrace.add(task, "GeneratedCodeFailures.saveFailing(" + data + ")");
        throw new IllegalStateException("Generated code saver exception");
    }

    /** Fails with an error, not an exception: the generated createExtraJson catches only exceptions. */
    @ExceptionDescriber
    public void describeUndescribable(Task task, GeneratedCodeUndescribableException e, JsonObjectBuilder builder) {
        throw new AssertionError("Generated code describer error");
    }
}
