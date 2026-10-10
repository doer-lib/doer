package transitsims.validation;

import com.doer.AcceptStatus;
import com.doer.DoerService;
import com.doer.Task;
import com.doer.TaskDataLoader;
import com.doer.TaskDataSaver;
import jakarta.inject.Inject;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
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
public class ValidationResource {
    DoerService doerService;
    DataSource ds;
    TaskRunner taskRunner;

    @Inject
    public void setDoerService(DoerService doerService) {
        this.doerService = doerService;
    }

    @Inject
    public void setDataSource(DataSource ds) {
        this.ds = ds;
    }

    @Inject
    public void setTaskRunner(TaskRunner taskRunner) {
        this.taskRunner = taskRunner;
    }

    @TaskDataLoader
    public Car loadCar(Task task) throws Exception {
        logTaskData("Car", task);
        return null;
    }

    @TaskDataSaver
    public void storeCar(Task task, Car car) throws Exception {
        logTaskData("Car", task);
    }

    @TaskDataSaver
    public void storeShampoo(Task task, Shampoo shampoo) throws Exception {
        logTaskData("Shampoo", task);
    }

    /** Records the access to the task data with the id of the current transaction, to check it is Doer's. */
    private void logTaskData(String objectType, Task task) throws SQLException {
        String sql = "insert into demo_log_tasks (object_type, task_id, in_progress, tx_id) values (?, ?, ?, txid_current())";
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setString(1, objectType);
            pst.setLong(2, task.getId());
            pst.setBoolean(3, task.isInProgress());
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

    /** Stops Doer, deletes all tasks and logs, and starts Doer again, with the monitor when {@code m} is true. */
    @Path("reset")
    @GET
    public String resetDoer(@QueryParam("m") @DefaultValue("false") boolean monitor) throws Exception {
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
        doerService.start(monitor);
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

    /** Runs a task through runTask of the generated service, see {@link TaskRunner}. */
    @Path("run-task")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public String runTask(String request) throws Exception {
        return taskRunner.run(request);
    }

    @Path("task")
    @GET
    public Task getTask(@QueryParam("id") Long id) throws Exception {
        return doerService.loadTask(id);
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
