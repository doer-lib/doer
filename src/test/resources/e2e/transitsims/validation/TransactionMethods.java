package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.DoerService;
import com.doer.Task;
import com.doer.TaskDataLoader;
import com.doer.TaskDataSaver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Transactions around doer methods, for TransactionsE2E and CoordinatedUpdateE2E. The loader and the saver of
 * {@link TransactionData} write a row of demo_log_tasks with the id of the current transaction, and the trigger on
 * tasks writes one for each update of a task: a test sees which of them ran in the same transaction. A doer method with
 * its own {@code @Transactional} writes two rows in its transaction.
 */
@ApplicationScoped
public class TransactionMethods {
    DataSource ds;
    DoerService doerService;

    @Inject
    public void setDataSource(DataSource ds) {
        this.ds = ds;
    }

    @Inject
    public void setDoerService(DoerService doerService) {
        this.doerService = doerService;
    }

    @AcceptStatus("Transaction data")
    public void withData(TransactionData data, Task task) {
        task.setStatus("Transaction data done");
    }

    @AcceptStatus("Transaction data failing")
    public void withDataFailing(Task task, TransactionData data) throws Exception {
        throw new Exception("Transaction data failing");
    }

    /** Updates the task in the doer method: the update runs in a transaction of its own. */
    @AcceptStatus("Transaction update in method")
    public void updateInMethod(Task task) throws SQLException {
        doerService.updateAndBumpVersion(task);
        task.setStatus("Transaction update in method done");
    }

    /** Its own transaction, separate from the ones of Doer: Doer calls a doer method outside a transaction. */
    @Transactional
    @AcceptStatus("Transaction own")
    public void ownTransaction(Task task) throws SQLException {
        logTransaction(task, "TransactionOwn");
        logTransaction(task, "TransactionOwn");
        task.setStatus("Transaction own done");
    }

    @TaskDataLoader
    public TransactionData load(Task task) throws SQLException {
        logTransaction(task, "TransactionData");
        return new TransactionData();
    }

    @TaskDataSaver
    public void save(Task task, TransactionData data) throws SQLException {
        logTransaction(task, "TransactionData");
    }

    private void logTransaction(Task task, String objectType) throws SQLException {
        String sql = "insert into demo_log_tasks (object_type, task_id, in_progress, tx_id) "
                + "values (?, ?, ?, txid_current())";
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setString(1, objectType);
            pst.setLong(2, task.getId());
            pst.setBoolean(3, task.isInProgress());
            pst.executeUpdate();
        }
    }
}
