package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.ExceptionDescriber;
import com.doer.RetryPolicy;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.JsonObjectBuilder;

/**
 * Doer methods that always fail, for ErrorsE2E, and the describer of RuntimeException, next to the one of Exception
 * in {@link ErrorDescribers}. The exception of {@link #causeAndSuppressed} has a cause and a suppressed exception,
 * which Doer describes too.
 */
@ApplicationScoped
public class ErrorMethods {
    @AcceptStatus("Error checked exception")
    public void checkedException(Task task) throws Exception {
        throw new Exception("Error checked exception");
    }

    @AcceptStatus("Error retry policy")
    @RetryPolicy(interval = "2 sec", duration = "10 seconds", fallbackStatus = "Error retry policy fallback")
    public void retryPolicy(Task task) {
        throw new RuntimeException("Error retry policy");
    }

    @AcceptStatus("Error retry forever")
    @RetryPolicy(interval = "2 sec")
    public void retryForever(Task task) {
        throw new RuntimeException("Error retry forever");
    }

    @AcceptStatus("Error cause and suppressed")
    public void causeAndSuppressed(Task task) throws Exception {
        Exception exception = new Exception("Error with cause", new IllegalStateException("Error cause"));
        exception.addSuppressed(new IllegalArgumentException("Error suppressed"));
        throw exception;
    }

    @ExceptionDescriber
    public void describeRuntimeException(Task task, RuntimeException e, JsonObjectBuilder builder) {
        builder.add("e2", "RuntimeException");
    }
}
