package transitsims.validation;

import com.doer.ExceptionDescriber;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.JsonObjectBuilder;

/** The describer of Exception, in a bean of its own. */
@ApplicationScoped
public class ErrorDescribers {
    @ExceptionDescriber
    public void describeException(Task task, Exception e, JsonObjectBuilder builder) {
        builder.add("e1", "Exception");
    }
}
