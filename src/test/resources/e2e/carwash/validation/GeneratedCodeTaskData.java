package carwash.validation;

import com.doer.Task;
import com.doer.TaskDataLoader;
import com.doer.TaskDataSaver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Loader and saver of {@link GeneratedCodeFirstData}, in a bean without doer methods. */
@ApplicationScoped
public class GeneratedCodeTaskData {
    CallTrace callTrace;

    @Inject
    public void setCallTrace(CallTrace callTrace) {
        this.callTrace = callTrace;
    }

    @TaskDataLoader
    public GeneratedCodeFirstData loadFirst(Task task) {
        callTrace.add(task, "GeneratedCodeTaskData.loadFirst");
        GeneratedCodeFirstData first = new GeneratedCodeFirstData();
        first.touch("loadFirst");
        return first;
    }

    @TaskDataSaver
    public void saveFirst(Task task, GeneratedCodeFirstData first) {
        callTrace.add(task, "GeneratedCodeTaskData.saveFirst(" + first + ")");
    }
}
