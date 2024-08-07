package tst.demo;

import com.doer.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.JsonObjectBuilder;

@ApplicationScoped
public class ExceptionMapper {
    @DoerExtraJson
    public void appendExceptionJson(Task task, Exception e, JsonObjectBuilder builder) {
        builder.add("e1", "Exception");
    }
}
