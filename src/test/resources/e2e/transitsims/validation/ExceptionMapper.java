package transitsims.validation;

import com.doer.ExceptionDescriber;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.JsonObjectBuilder;

@ApplicationScoped
public class ExceptionMapper {
    @ExceptionDescriber
    public void appendExceptionJson(Task task, Exception e, JsonObjectBuilder builder) {
        builder.add("e1", "Exception");
    }
}
