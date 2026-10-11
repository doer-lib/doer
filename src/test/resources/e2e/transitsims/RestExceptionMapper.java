package transitsims;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

/** Responds 500 with the exception type, so that tests can tell the exceptions apart. */
@Provider
public class RestExceptionMapper implements jakarta.ws.rs.ext.ExceptionMapper<Exception> {
    @Override
    public Response toResponse(Exception exception) {
        if (exception instanceof WebApplicationException e) {
            return e.getResponse();
        }
        return Response.serverError()
                .type(MediaType.APPLICATION_JSON)
                .entity("{\"exception\": \"" + exception.getClass().getSimpleName() + "\"}")
                .build();
    }
}
