package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.ExceptionDescriber;
import com.doer.RetryPolicy;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.JsonObjectBuilder;

/**
 * Doer methods that always fail, for ErrorsE2E, and the describer of RuntimeException, next to the one of Exception
 * in {@link ErrorDescribers}.
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

    @ExceptionDescriber
    public void describeRuntimeException(Task task, RuntimeException e, JsonObjectBuilder builder) {
        builder.add("e2", "RuntimeException");
    }
}
