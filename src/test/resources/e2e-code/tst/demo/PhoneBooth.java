package tst.demo;

import com.doer.AcceptStatus;
import com.doer.DoerConcurrency;
import com.doer.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.json.JsonObjectBuilder;

@DoerConcurrency(1)
@ApplicationScoped
public class PhoneBooth {
    @AcceptStatus("Need call taxi")
    @AcceptStatus("Need order pizza")
    public void makeACall(Task task) throws Exception {
        Thread.sleep(100);
        task.setStatus("Call finished");
    }

    @AcceptStatus("Time to cleanup")
    public void cleanUp(Task task) throws Exception {
        Thread.sleep(100);
        task.setStatus("Cleanup finished");
    }

    @DoerExtraJson
    public void appendRuntimeExceptionJson(Task task, RuntimeException e, JsonObjectBuilder builder) {
        builder.add("e2", "RuntimeException");
    }
}
