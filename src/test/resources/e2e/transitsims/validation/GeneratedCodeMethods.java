package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.Task;
import com.doer.TaskDataLoader;
import com.doer.TaskDataSaver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Doer methods with 1, 2 and 3 parameters for GeneratedCodeTest, and the loader and saver of
 * {@link GeneratedCodeSecondData} next to them.
 */
@ApplicationScoped
public class GeneratedCodeMethods {
    CallTrace callTrace;

    @Inject
    public void setCallTrace(CallTrace callTrace) {
        this.callTrace = callTrace;
    }

    @AcceptStatus("Generated code 1 param")
    public void oneParam(Task task) {
        callTrace.add(task, "GeneratedCodeMethods.oneParam");
        task.setStatus("Generated code 1 param done");
    }

    @AcceptStatus("Generated code 2 params")
    public void twoParams(GeneratedCodeFirstData first, Task task) {
        callTrace.add(task, "GeneratedCodeMethods.twoParams(" + first + ")");
        first.touch("twoParams");
        task.setStatus("Generated code 2 params done");
    }

    @AcceptStatus("Generated code 3 params")
    public void threeParams(Task task, GeneratedCodeFirstData first, GeneratedCodeSecondData second) {
        callTrace.add(task, "GeneratedCodeMethods.threeParams(" + first + ", " + second + ")");
        first.touch("threeParams");
        second.touch("threeParams");
        task.setStatus("Generated code 3 params done");
    }

    @TaskDataLoader
    public GeneratedCodeSecondData loadSecond(Task task) {
        callTrace.add(task, "GeneratedCodeMethods.loadSecond");
        GeneratedCodeSecondData second = new GeneratedCodeSecondData();
        second.touch("loadSecond");
        return second;
    }

    @TaskDataSaver
    public void saveSecond(Task task, GeneratedCodeSecondData second) {
        callTrace.add(task, "GeneratedCodeMethods.saveSecond(" + second + ")");
    }
}
