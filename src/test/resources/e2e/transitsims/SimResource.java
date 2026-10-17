package transitsims;

import com.doer.DoerService;
import com.doer.Task;
import com.doer.TaskAndDataUpdater;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/** Simulations: create, start, pause, resume, and the passengers. */
@Path("/sims")
@Produces(MediaType.APPLICATION_JSON)
public class SimResource {
    /** How long a coordinated update waits for the task to be free. */
    static final Duration WAIT = Duration.ofSeconds(5);

    DoerService doerService;
    SimRepository simRepository;
    BusRepository busRepository;

    @Inject
    public void setDoerService(DoerService doerService) {
        this.doerService = doerService;
    }

    @Inject
    public void setSimRepository(SimRepository simRepository) {
        this.simRepository = simRepository;
    }

    @Inject
    public void setBusRepository(BusRepository busRepository) {
        this.busRepository = busRepository;
    }

    public record SimView(UUID id, long time, Simulation simulation) {
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Map<String, UUID> create(Simulation simulation) throws Exception {
        simRepository.create(simulation);
        return Map.of("id", simulation.id);
    }

    @GET
    @Path("{sim}")
    public SimView get(@PathParam("sim") UUID id) throws Exception {
        Simulation simulation = find(id);
        return new SimView(id, simulation.time(doerService.getDbNow()), simulation);
    }

    /**
     * The start, the pause and the resume are coordinated updates of the Sim task. The tasks of the buses get their
     * statuses in the same transaction, and Doer reloads its queues after the commit.
     */
    @POST
    @Path("{sim}/start")
    public SimView start(@PathParam("sim") UUID id) throws Exception {
        SimView view = update(id, (task, simulation) -> {
            requireStatus(simulation, Simulation.Status.READY);
            simulation.startedAt = doerService.getDbNow();
            simulation.status = Simulation.Status.RUNNING;
            task.setStatus(SimStatus.RUNNING);
            setBusStatuses(id, BusStatus.RESUME);
        });
        doerService.triggerQueuesReloadFromDb();
        return view;
    }

    @POST
    @Path("{sim}/pause")
    public SimView pause(@PathParam("sim") UUID id) throws Exception {
        SimView view = update(id, (task, simulation) -> {
            requireStatus(simulation, Simulation.Status.RUNNING);
            simulation.pausedAt = doerService.getDbNow();
            simulation.status = Simulation.Status.PAUSED;
            task.setStatus(SimStatus.PAUSED);
            setBusStatuses(id, null);
        });
        doerService.triggerQueuesReloadFromDb();
        return view;
    }

    @POST
    @Path("{sim}/resume")
    public SimView resume(@PathParam("sim") UUID id) throws Exception {
        SimView view = update(id, (task, simulation) -> {
            requireStatus(simulation, Simulation.Status.PAUSED);
            simulation.pausedMs += Duration.between(simulation.pausedAt, doerService.getDbNow()).toMillis();
            simulation.pausedAt = null;
            simulation.status = Simulation.Status.RUNNING;
            task.setStatus(SimStatus.RUNNING);
            setBusStatuses(id, BusStatus.RESUME);
        });
        doerService.triggerQueuesReloadFromDb();
        return view;
    }

    /** Registers a passenger before the start; returns its number. */
    @POST
    @Path("{sim}/passengers")
    public Map<String, Integer> addPassenger(@PathParam("sim") UUID id) throws Exception {
        AtomicInteger passenger = new AtomicInteger();
        update(id, (task, simulation) -> {
            requireStatus(simulation, Simulation.Status.READY);
            passenger.set(++simulation.passengers);
        });
        return Map.of("passenger", passenger.get());
    }

    @POST
    @Path("{sim}/passengers/{passenger}/arrived")
    public SimView arrived(@PathParam("sim") UUID id, @PathParam("passenger") int passenger) throws Exception {
        return update(id, (task, simulation) -> {
            requireStatus(simulation, Simulation.Status.RUNNING, Simulation.Status.PAUSED);
            if (passenger < 1 || passenger > simulation.passengers) {
                throw conflict("No passenger " + passenger);
            }
            simulation.arrived++;
        });
    }

    /**
     * Sets the status of all buses of the simulation, in the transaction of the caller. A bus whose step is in
     * progress is hijacked: the step fails on the version of the task, and its data is not saved.
     */
    private void setBusStatuses(UUID simulationId, String status) throws SQLException {
        for (Task task : doerService.loadTasks(busRepository.taskIds(simulationId)).values()) {
            task.setInProgress(false);
            task.setFailingSince(null);
            task.setStatus(status);
            if (!doerService.updateAndBumpVersion(task)) {
                throw conflict("The task " + task.getId() + " of a bus has changed meanwhile, try again");
            }
        }
    }

    private SimView update(UUID id, TaskAndDataUpdater<Simulation> updater) throws Exception {
        doerService.facilitateCoordinatedUpdate(find(id).taskId, WAIT, false, Simulation.class, updater);
        return get(id);
    }

    private Simulation find(UUID id) throws Exception {
        Simulation simulation = simRepository.find(id);
        if (simulation == null) {
            throw new NotFoundException("No simulation " + id);
        }
        return simulation;
    }

    private static void requireStatus(Simulation simulation, Simulation.Status... statuses) {
        if (!List.of(statuses).contains(simulation.status)) {
            throw conflict("The simulation is " + simulation.status + ", not " + List.of(statuses));
        }
    }

    /** 409 with the reason as JSON. */
    static WebApplicationException conflict(String message) {
        return new WebApplicationException(Response.status(Response.Status.CONFLICT)
                .type(MediaType.APPLICATION_JSON)
                .entity(Map.of("conflict", message))
                .build());
    }
}
