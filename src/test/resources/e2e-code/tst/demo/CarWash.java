package tst.demo;

import com.doer.AcceptStatus;
import com.doer.ConcurrencyLimit;
import com.doer.TaskDataLoader;
import com.doer.Task;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class CarWash {
    Logger log = LoggerFactory.getLogger(getClass());

    @Inject
    DataSource ds;

    @AcceptStatus("Car is dusty")
    public void washTheCar(Task task, Car car, Shampoo shampoo) {
        log.info("wash the car");
        task.setStatus("Car is washed");
    }

    @ConcurrencyLimit(1)
    @AcceptStatus("Car is washed")
    @AcceptStatus("Car need polishing")
    public void polishTheCar(Car car, Task task) throws Exception {
        log.info("polish the car");
        task.setStatus("Car is polished");
    }

    @ConcurrencyLimit(10)
    @AcceptStatus("Customer is ready to pay")
    public void checkIn(Task task) {
        task.setStatus("Payed");
    }

    @TaskDataLoader
    public Shampoo loadShampoo(Task task) throws Exception {
        log.info("Load shampoo");
        String sql = "insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Shampoo', ?, ?, txid_current());";
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setLong(1, task.getId());
            pst.setBoolean(2, task.isInProgress());
            pst.executeUpdate();
        }
        return null;
    }
}
