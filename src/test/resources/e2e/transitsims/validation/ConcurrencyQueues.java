package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.ConcurrencyLimit;
import com.doer.Task;
import jakarta.enterprise.context.Dependent;

/**
 * Ten tasks at a time in the class, for ConcurrencyE2E. The slow and the failing method share the queues of one
 * domain: the asap queue and the retry queue. The chain moves a task from the domain of the class to the domain of a
 * method with its own limit, and back.
 */
@ConcurrencyLimit(10)
@Dependent
public class ConcurrencyQueues {
    @AcceptStatus("Concurrency queues slow")
    public void slow(Task task) throws Exception {
        Thread.sleep(100);
        task.setStatus("Concurrency queues slow done");
    }

    @AcceptStatus("Concurrency queues failing")
    public void failing(Task task) throws Exception {
        throw new Exception("Concurrency queues failing");
    }

    @AcceptStatus("Concurrency chain class")
    public void chainClass(Task task) {
        task.setStatus("Concurrency chain method");
    }

    @ConcurrencyLimit(2)
    @AcceptStatus("Concurrency chain method")
    public void chainMethod(Task task) {
        task.setStatus("Concurrency chain class again");
    }

    @AcceptStatus("Concurrency chain class again")
    public void chainClassAgain(Task task) {
        task.setStatus("Concurrency chain done");
    }
}
