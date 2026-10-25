package transitsims.validation;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

/** The producer of {@link BeanKindProduced}. */
@ApplicationScoped
public class BeanKindProducers {
    CallTrace callTrace;

    @Inject
    public void setCallTrace(CallTrace callTrace) {
        this.callTrace = callTrace;
    }

    @Produces
    public BeanKindProduced produce() {
        return new BeanKindProduced(callTrace, "BeanKindProducers.produce");
    }
}
