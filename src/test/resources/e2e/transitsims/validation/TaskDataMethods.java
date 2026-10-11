package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.Task;
import com.doer.TaskDataLoader;
import jakarta.enterprise.context.ApplicationScoped;

/** The loader of {@link TaskDataRecord}, without a saver, and a doer method that gets the record. */
@ApplicationScoped
public class TaskDataMethods {
    @TaskDataLoader
    public TaskDataRecord load(Task task) {
        return new TaskDataRecord(task.getId(), "TaskDataMethods.load");
    }

    @AcceptStatus("Task data record")
    public void withRecord(Task task, TaskDataRecord data) {
        if (data.taskId() != task.getId() || !"TaskDataMethods.load".equals(data.loadedBy())) {
            throw new IllegalStateException("Unexpected task data " + data + " of task " + task.getId());
        }
        task.setStatus("Task data record done");
    }
}
