package transitsims.validation;

import com.doer.Task;
import com.doer.TaskSnapshot;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Calls of the GeneratedCode* and BeanKind* doer methods, loaders, savers and interceptors, by task: what
 * {@link TaskRunner} reports back for the task it ran.
 */
@ApplicationScoped
public class CallTrace {
    final Map<Long, List<String>> calls = new ConcurrentHashMap<>();

    public void add(Task task, String call) {
        add(task.getId(), call);
    }

    public void add(TaskSnapshot task, String call) {
        add(task.id(), call);
    }

    private void add(long taskId, String call) {
        calls.computeIfAbsent(taskId, id -> new CopyOnWriteArrayList<>()).add(call);
    }

    /** Calls made for the task, removed from the trace. */
    public List<String> take(long taskId) {
        List<String> taskCalls = calls.remove(taskId);
        return taskCalls == null ? List.of() : taskCalls;
    }
}
