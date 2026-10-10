package transitsims;

import com.doer.Task;
import com.doer.TaskDataLoader;
import com.doer.TaskDataSaver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * Buses in the table buses; the task data of the Bus task. A bus is loaded with its simulation and the database
 * time, to be read; only the bus is saved.
 */
@ApplicationScoped
public class BusRepository {
    static final String SELECT = "SELECT b.id, b.task_id, b.json_data, s.id, s.task_id, s.json_data, now() "
            + "FROM buses b JOIN sims s ON s.id = (b.json_data ->> 'simulationId')::uuid";

    DataSource ds;

    @Inject
    public void setDataSource(DataSource ds) {
        this.ds = ds;
    }

    public void insert(Bus bus) throws SQLException {
        String sql = "INSERT INTO buses (id, task_id, json_data) VALUES (?, ?, ?::json)";
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setObject(1, bus.id);
            pst.setLong(2, bus.taskId);
            pst.setString(3, JsonData.JSONB.toJson(bus));
            pst.executeUpdate();
        }
    }

    public Bus find(UUID id) throws SQLException {
        List<Bus> buses = select(SELECT + " WHERE b.id = ?", id);
        return buses.isEmpty() ? null : buses.get(0);
    }

    public List<Bus> findBySimulation(UUID simulationId) throws SQLException {
        return select(SELECT + " WHERE b.json_data ->> 'simulationId' = ? ORDER BY b.task_id",
                simulationId.toString());
    }

    /** The buses of the simulation whose task is not parked. */
    public long countNotParked(UUID simulationId) throws SQLException {
        String sql = "SELECT count(*) FROM buses b JOIN tasks t ON t.id = b.task_id "
                + "WHERE b.json_data ->> 'simulationId' = ? AND t.status IS DISTINCT FROM ?";
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setString(1, simulationId.toString());
            pst.setString(2, BusStatus.PARKED);
            try (ResultSet rs = pst.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    @TaskDataLoader
    public Bus load(Task task) throws SQLException {
        List<Bus> buses = select(SELECT + " WHERE b.task_id = ?", task.getId());
        return buses.isEmpty() ? null : buses.get(0);
    }

    @TaskDataSaver
    public void save(Task task, Bus bus) throws SQLException {
        String sql = "UPDATE buses SET json_data = ?::json, modified = now() WHERE id = ?";
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setString(1, JsonData.JSONB.toJson(bus));
            pst.setObject(2, bus.id);
            pst.executeUpdate();
        }
    }

    private List<Bus> select(String sql, Object key) throws SQLException {
        List<Bus> buses = new ArrayList<>();
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setObject(1, key);
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    Bus bus = JsonData.JSONB.fromJson(rs.getString(3), Bus.class);
                    bus.id = rs.getObject(1, UUID.class);
                    bus.taskId = rs.getLong(2);
                    bus.simulation = SimRepository.read(rs, 4);
                    bus.now = rs.getObject(7, OffsetDateTime.class).toInstant();
                    buses.add(bus);
                }
            }
        }
        return buses;
    }
}
