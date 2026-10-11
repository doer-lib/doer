package transitsims;

import com.doer.DoerService;
import com.doer.Task;
import com.doer.TaskDataLoader;
import com.doer.TaskDataSaver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;

/** Simulations in the table sims; the task data of the Sim task. */
@ApplicationScoped
public class SimRepository {
    DataSource ds;
    DoerService doerService;
    BusRepository busRepository;

    @Inject
    public void setDataSource(DataSource ds) {
        this.ds = ds;
    }

    @Inject
    public void setDoerService(DoerService doerService) {
        this.doerService = doerService;
    }

    @Inject
    public void setBusRepository(BusRepository busRepository) {
        this.busRepository = busRepository;
    }

    /** Inserts the simulation, its buses and their tasks in one transaction; the tasks have no status yet. */
    @Transactional(rollbackOn = Exception.class)
    public List<Bus> create(Simulation simulation) throws SQLException {
        simulation.id = UUID.randomUUID();
        simulation.status = Simulation.Status.READY;
        simulation.taskId = insertTask();
        String sql = "INSERT INTO sims (id, task_id, json_data) VALUES (?, ?, ?::json)";
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setObject(1, simulation.id);
            pst.setLong(2, simulation.taskId);
            pst.setString(3, JsonData.JSONB.toJson(simulation));
            pst.executeUpdate();
        }
        List<Bus> buses = simulation.newBuses();
        for (Bus bus : buses) {
            bus.id = UUID.randomUUID();
            bus.taskId = insertTask();
            busRepository.insert(bus);
        }
        return buses;
    }

    private long insertTask() throws SQLException {
        Task task = new Task();
        doerService.insert(task);
        return task.getId();
    }

    public Simulation find(UUID id) throws SQLException {
        return selectOne("SELECT id, task_id, json_data FROM sims WHERE id = ?", id);
    }

    @TaskDataLoader
    public Simulation load(Task task) throws SQLException {
        return selectOne("SELECT id, task_id, json_data FROM sims WHERE task_id = ?", task.getId());
    }

    @TaskDataSaver
    public void save(Task task, Simulation simulation) throws SQLException {
        update(simulation);
    }

    public void update(Simulation simulation) throws SQLException {
        String sql = "UPDATE sims SET json_data = ?::json, modified = now() WHERE id = ?";
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setString(1, JsonData.JSONB.toJson(simulation));
            pst.setObject(2, simulation.id);
            pst.executeUpdate();
        }
    }

    private Simulation selectOne(String sql, Object key) throws SQLException {
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setObject(1, key);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() ? read(rs, 1) : null;
            }
        }
    }

    /** The simulation from the columns id, task_id and json_data, starting at the column {@code first}. */
    static Simulation read(ResultSet rs, int first) throws SQLException {
        Simulation simulation = JsonData.JSONB.fromJson(rs.getString(first + 2), Simulation.class);
        simulation.id = rs.getObject(first, UUID.class);
        simulation.taskId = rs.getLong(first + 1);
        return simulation;
    }
}
