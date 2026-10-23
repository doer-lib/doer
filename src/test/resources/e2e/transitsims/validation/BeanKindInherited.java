package transitsims.validation;

import jakarta.enterprise.context.ApplicationScoped;

/** The bean of {@link BeanKindBase}: inherits its doer method. */
@ApplicationScoped
public class BeanKindInherited extends BeanKindBase {
    @Override
    protected String name() {
        return "BeanKindInherited";
    }
}
