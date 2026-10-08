package carwash.validation;

import carwash.Car;
import carwash.Shampoo;
import com.doer.AcceptStatus;
import com.doer.TaskDataLoader;
import com.doer.DoerService;
import com.doer.TaskDataSaver;
import com.doer.Task;
import jakarta.inject.Inject;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import javax.sql.DataSource;

@Path("/validation")
@Produces(MediaType.APPLICATION_JSON)
public class DoerResource {
    DoerService doerService;
    DataSource ds;

    @Inject
    public void setDoerService(DoerService doerService) {
        this.doerService = doerService;
    }

    @Inject
    public void setDataSource(DataSource ds) {
        this.ds = ds;
    }

    @TaskDataLoader
    public Car loadCar(Task task) throws Exception {
        String sql = "insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Car', ?, ?, txid_current());";
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setLong(1, task.getId());
            pst.setBoolean(2, task.isInProgress());
            pst.executeUpdate();
        }
        return null;
    }

    @TaskDataSaver
    public void storeCar(Task task, Car car) throws Exception {
        String sql = "insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Car', ?, ?, txid_current());";
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setLong(1, task.getId());
            pst.setBoolean(2, task.isInProgress());
            pst.executeUpdate();
        }
    }

    @TaskDataSaver
    public void storeShampoo(Task task, Shampoo shampoo) throws Exception {
        String sql = "insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Shampoo', ?, ?, txid_current());";
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setLong(1, task.getId());
            pst.setBoolean(2, task.isInProgress());
            pst.executeUpdate();
        }
    }

    /** The runtime the application runs in, from the environment variable {@code E2E_RUNTIME}. */
    @Path("info")
    @GET
    public String info() {
        String runtime = System.getenv("E2E_RUNTIME");
        JsonObjectBuilder builder = Json.createObjectBuilder();
        if (runtime == null) {
            builder.addNull("runtime");
        } else {
            builder.add("runtime", runtime);
        }
        return builder.build().toString();
    }

    @Path("start")
    @GET
    public String startDoer(@QueryParam("m") @DefaultValue("false") boolean monitor) {
        doerService.start(monitor);
        return "{\"status\": \"Doer Started\", \"monitor\": " + monitor + "}";
    }

    @Path("stop")
    @GET
    public String stopDoer() {
        doerService.stop();
        return "{\"status\": \"Doer Stopped\"}";
    }

    @Path("load")
    @GET
    public String reload() {
        doerService.triggerQueuesReloadFromDb();
        return "{\"status\": \"Doer Reloaded\"}";
    }

    @Path("check")
    @GET
    public String checkTasksBecomeReady() {
        doerService.checkTasksBecomeReady();
        return "{\"status\": \"check\"}";
    }

    @Path("fix")
    @GET
    public String fixStalled() throws Exception {
        doerService.resetStalledInProgressTasks(Duration.ofSeconds(1));
        return "{\"status\":\"fixed\"}";
    }

    @Path("reset")
    @GET
    public String resetDoer() throws Exception {
        doerService.stop();
        String sql = "DELETE FROM task_logs; DELETE FROM tasks; DELETE FROM demo_log_tasks";
        int updatedLogs;
        int updatedTasks;
        int updatedDemoLog;
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            updatedLogs = pst.executeUpdate();
            pst.getMoreResults();
            updatedTasks = pst.getUpdateCount();
            pst.getMoreResults();
            updatedDemoLog = pst.getUpdateCount();
        }
        doerService.start(false);
        return "{\"status\": \"Doer Reset\",\n\"cleared\": {\n\"tasks\": " + updatedTasks + ",\n\"logs\": " + updatedLogs + ",\n\"demo_logs\": " + updatedDemoLog + "}}";
    }

    @Path("add_task")
    @GET
    public Task startNewTask(@QueryParam("s") String status) throws Exception {
        Task task = new Task();
        task.setStatus(status);
        doerService.insert(task);
        doerService.triggerTaskReloadFromDb(task.getId());
        return task;
    }

    @Path("task")
    @GET
    public Task getTask(@QueryParam("id") Long id) throws Exception {
        Task task = doerService.loadTask(id);
        return task;
    }

    @Path("coordinated_update")
    @GET
    public Task coordinatedUpdate(@QueryParam("id") long id, @QueryParam("s") String status,
            @QueryParam("hijack") @DefaultValue("false") boolean hijack,
            @QueryParam("wait") @DefaultValue("300") long waitMs,
            @QueryParam("fail") @DefaultValue("false") boolean fail) throws Exception {
        return doerService.facilitateCoordinatedUpdate(id, Duration.ofMillis(waitMs), hijack, task -> {
            task.setStatus(status);
            if (fail) {
                throw new IllegalStateException("updater failed");
            }
        });
    }

    @Path("coordinated_car_update")
    @GET
    public Task coordinatedCarUpdate(@QueryParam("id") long id, @QueryParam("s") String status) throws Exception {
        return doerService.facilitateCoordinatedUpdate(id, Duration.ZERO, false, Car.class,
                (task, car) -> task.setStatus(status));
    }

    /** There is no @TaskDataLoader for String. */
    @Path("coordinated_update_without_loader")
    @GET
    public Task coordinatedUpdateWithoutLoader(@QueryParam("id") long id, @QueryParam("s") String status)
            throws Exception {
        return doerService.facilitateCoordinatedUpdate(id, Duration.ZERO, false, String.class,
                (task, data) -> task.setStatus(status));
    }

    @Path("coordinated_update_in_transaction")
    @GET
    @Transactional
    public Task coordinatedUpdateInTransaction(@QueryParam("id") long id, @QueryParam("s") String status)
            throws Exception {
        return doerService.facilitateCoordinatedUpdate(id, Duration.ZERO, false, task -> task.setStatus(status));
    }

    @AcceptStatus("A")
    public void consumeTaskA(Task task) {
        task.setStatus("B");
    }

    @AcceptStatus("B")
    public void consumeTaskB(Task task) {
        task.setStatus(null);
    }

    @AcceptStatus("Need wash hands")
    public void washHands(Task task) throws SQLException {
        // update task in doer method, to check it is run in separated transaction
        doerService.updateAndBumpVersion(task);
        task.setStatus("Washed");
    }

}
