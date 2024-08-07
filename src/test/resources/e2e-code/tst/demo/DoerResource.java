package tst.demo;

import com.doer.AcceptStatus;
import com.doer.DoerLoader;
import com.doer.DoerService;
import com.doer.DoerUnloader;
import com.doer.Task;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Shutdown;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;

@Path("/doer")
@Produces(MediaType.APPLICATION_JSON)
public class DoerResource {
    @Inject
    DoerService doerService;

    @Inject
    DataSource ds;

    void onAppStart(@Observes StartupEvent event) {
        doerService.start(false);
    }

    void onAppStop(@Observes Shutdown event) {
        doerService.stop();
    }

    @DoerLoader
    public Car loadCar(Task task) throws Exception {
        String sql = "insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Car', ?, ?, txid_current());";
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setLong(1, task.getId());
            pst.setBoolean(2, task.isInProgress());
            pst.executeUpdate();
        }
        return null;
    }

    @DoerUnloader
    public void storeCar(Task task, Car car) throws Exception {
        String sql = "insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Car', ?, ?, txid_current());";
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setLong(1, task.getId());
            pst.setBoolean(2, task.isInProgress());
            pst.executeUpdate();
        }
    }

    @DoerUnloader
    public void storeShampoo(Task task, Shampoo shampoo) throws Exception {
        String sql = "insert into demo_log_tasks (object_type , task_id, in_progress, tx_id) values ('Shampoo', ?, ?, txid_current());";
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setLong(1, task.getId());
            pst.setBoolean(2, task.isInProgress());
            pst.executeUpdate();
        }
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
