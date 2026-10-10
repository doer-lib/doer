package transitsims;

import static transitsims.SimResource.WAIT;
import static transitsims.SimResource.conflict;

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
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** The buses of a simulation; passengers board and alight while a bus stands at their stop. */
@Path("/sims/{sim}/buses")
@Produces(MediaType.APPLICATION_JSON)
public class BusResource {
    DoerService doerService;
    BusRepository busRepository;

    @Inject
    public void setDoerService(DoerService doerService) {
        this.doerService = doerService;
    }

    @Inject
    public void setBusRepository(BusRepository busRepository) {
        this.busRepository = busRepository;
    }

    public record BusView(UUID id, String status, String stop, long time, Bus bus) {
    }

    public record Boarding(int passenger, String stop) {
    }

    @GET
    public List<BusView> list(@PathParam("sim") UUID simId) throws Exception {
        List<Bus> buses = busRepository.findBySimulation(simId);
        Map<Long, Task> tasks = doerService.loadTasks(buses.stream().map(b -> b.taskId).toList());
        return buses.stream().map(b -> view(b, tasks.get(b.taskId))).toList();
    }

    @POST
    @Path("{bus}/board")
    @Consumes(MediaType.APPLICATION_JSON)
    public BusView board(@PathParam("sim") UUID simId, @PathParam("bus") UUID busId, Boarding boarding)
            throws Exception {
        return update(simId, busId, (task, bus) -> {
            requireStandingAt(task, bus, boarding.stop());
            if (bus.passengers.size() >= bus.capacity) {
                throw conflict("The bus is full");
            }
            bus.passengers.add(boarding.passenger());
        });
    }

    @POST
    @Path("{bus}/alight")
    @Consumes(MediaType.APPLICATION_JSON)
    public BusView alight(@PathParam("sim") UUID simId, @PathParam("bus") UUID busId, Boarding boarding)
            throws Exception {
        return update(simId, busId, (task, bus) -> {
            requireStandingAt(task, bus, boarding.stop());
            if (!bus.passengers.remove(boarding.passenger())) {
                throw conflict("Passenger " + boarding.passenger() + " is not on the bus");
            }
        });
    }

    private BusView update(UUID simId, UUID busId, TaskAndDataUpdater<Bus> updater) throws Exception {
        Bus bus = busRepository.find(busId);
        if (bus == null || !bus.simulationId.equals(simId)) {
            throw new NotFoundException("No bus " + busId + " in simulation " + simId);
        }
        Task task = doerService.facilitateCoordinatedUpdate(bus.taskId, WAIT, false, Bus.class, updater);
        return view(busRepository.find(busId), task);
    }

    private static void requireStandingAt(Task task, Bus bus, String stop) {
        boolean standing = BusStatus.AT_STOP.equals(task.getStatus()) || BusStatus.AT_TERMINAL.equals(task.getStatus());
        if (!standing || !bus.stopId().equals(stop)) {
            throw conflict("The bus does not stand at stop " + stop);
        }
    }

    private static BusView view(Bus bus, Task task) {
        return new BusView(bus.id, task.getStatus(), bus.stopId(), bus.time(), bus);
    }
}
