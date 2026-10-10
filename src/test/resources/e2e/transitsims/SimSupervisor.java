package transitsims;

import com.doer.AcceptStatus;
import com.doer.Task;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.sql.SQLException;

/** Completes a running simulation when all passengers have arrived and all buses are parked. */
@Dependent
public class SimSupervisor {
    BusRepository busRepository;

    @Inject
    public void setBusRepository(BusRepository busRepository) {
        this.busRepository = busRepository;
    }

    @AcceptStatus(value = SimStatus.RUNNING, delay = "1s")
    public void checkCompletion(Task task, Simulation simulation) throws SQLException {
        if (simulation.allArrived() && busRepository.countNotParked(simulation.id) == 0) {
            task.setStatus(SimStatus.COMPLETED);
        }
    }
}
