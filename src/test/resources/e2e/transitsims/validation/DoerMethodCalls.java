package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.DoerService;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.SQLException;

/** A doer method that calls DoerService itself: it inserts a new task, and Doer runs it too. */
@ApplicationScoped
public class DoerMethodCalls {
    DoerService doerService;

    @Inject
    public void setDoerService(DoerService doerService) {
        this.doerService = doerService;
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
