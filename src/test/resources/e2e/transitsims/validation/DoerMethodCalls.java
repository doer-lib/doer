package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.DoerService;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.SQLException;

/**
 * A doer method that calls DoerService itself: it inserts a new task, and Doer runs it too. DoerService comes through
 * the constructor, a circular reference that Spring can not resolve even with allow-circular-references; it works
 * because the generated service injects a Provider of this bean. The client proxy of the scope needs a no-arg
 * constructor as well.
 */
@ApplicationScoped
public class DoerMethodCalls {
    final DoerService doerService;

    @Inject
    public DoerMethodCalls(DoerService doerService) {
        this.doerService = doerService;
    }

    protected DoerMethodCalls() {
        this(null);
    }

    @AcceptStatus("Doer method calls insert")
    public void insertTask(Task task) throws SQLException {
        Task inserted = new Task();
        inserted.setStatus("Doer method calls inserted");
        doerService.insert(inserted);
        doerService.triggerTaskReloadFromDb(inserted.getId());
        task.setStatus("Doer method calls insert done");
    }

    @AcceptStatus("Doer method calls inserted")
    public void runInserted(Task task) {
        task.setStatus("Doer method calls inserted done");
    }
}
