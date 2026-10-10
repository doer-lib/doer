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
import java.time.Duration;
import java.time.Instant;
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

    @Inject
    public void setDoerService(DoerService doerService) {
        this.doerService = doerService;
    }

    @Inject
    public void setSimRepository(SimRepository simRepository) {
        this.simRepository = simRepository;
    }

    public record SimView(UUID id, String status, long time, Simulation simulation) {
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Map<String, UUID> create(Simulation simulation) throws Exception {
        List<Bus> buses = simRepository.create(simulation);
        for (Bus bus : buses) {
            doerService.triggerTaskReloadFromDb(bus.taskId);
        }
        return Map.of("id", simulation.id);
    }

    @GET
    @Path("{sim}")
    public SimView get(@PathParam("sim") UUID id) throws Exception {
        Simulation simulation = find(id);
        Task task = doerService.loadTask(simulation.taskId);
        return new SimView(id, task.getStatus(), simulation.time(doerService.getDbNow()), simulation);
    }

    @POST
    @Path("{sim}/start")
    public SimView start(@PathParam("sim") UUID id) throws Exception {
        return update(id, (task, simulation) -> {
            requireStatus(task, SimStatus.READY);
            simulation.startedAt = doerService.getDbNow();
            task.setStatus(SimStatus.RUNNING);
        });
    }

    @POST
    @Path("{sim}/pause")
    public SimView pause(@PathParam("sim") UUID id) throws Exception {
        return update(id, (task, simulation) -> {
            requireStatus(task, SimStatus.RUNNING);
            simulation.pausedAt = doerService.getDbNow();
            task.setStatus(SimStatus.PAUSED);
        });
    }

    @POST
    @Path("{sim}/resume")
    public SimView resume(@PathParam("sim") UUID id) throws Exception {
        return update(id, (task, simulation) -> {
            requireStatus(task, SimStatus.PAUSED);
            simulation.pausedMs += Duration.between(simulation.pausedAt, doerService.getDbNow()).toMillis();
            simulation.pausedAt = null;
            task.setStatus(SimStatus.RUNNING);
        });
    }

    /** Registers a passenger before the start; returns its number. */
    @POST
    @Path("{sim}/passengers")
    public Map<String, Integer> addPassenger(@PathParam("sim") UUID id) throws Exception {
        AtomicInteger passenger = new AtomicInteger();
        update(id, (task, simulation) -> {
            requireStatus(task, SimStatus.READY);
            passenger.set(++simulation.passengers);
        });
        return Map.of("passenger", passenger.get());
    }

    @POST
    @Path("{sim}/passengers/{passenger}/arrived")
    public SimView arrived(@PathParam("sim") UUID id, @PathParam("passenger") int passenger) throws Exception {
        return update(id, (task, simulation) -> {
            if (passenger < 1 || passenger > simulation.passengers) {
                throw conflict("No passenger " + passenger);
            }
            simulation.arrived++;
        });
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

    private static void requireStatus(Task task, String status) {
        if (!status.equals(task.getStatus())) {
            throw conflict("The status is " + task.getStatus() + ", not " + status);
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
