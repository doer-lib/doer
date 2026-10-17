package transitsims;

import jakarta.annotation.Resource;
import jakarta.enterprise.concurrent.ManagedExecutorService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.util.concurrent.Executor;
import javax.sql.DataSource;
import transitsims.validation.BeanKindSingleton;
import transitsims.validation.CallTrace;

/** The DataSource and the Executor of the server as CDI beans. Copied by the e2e suite (WildflyApp). */
@ApplicationScoped
public class Producers {
    @Resource(lookup = "java:jboss/datasources/TransitSims")
    DataSource dataSource;

    /**
     * The threads of Doer get the context of the application, and each task its own local transaction
     * containment: a connection taken outside a transaction goes back to the pool when the task ends (Open
     * Liberty). Not a pool of the ManagedThreadFactory: the containment of its threads never ends.
     */
    @Resource
    ManagedExecutorService executor;

    @Produces
    DataSource dataSource() {
        return dataSource;
    }

    @Produces
    Executor executor() {
        return executor;
    }

    /**
     * @Singleton alone does not make a class a CDI bean: it is not a bean defining annotation, so a WAR
     * without beans.xml (bean discovery mode annotated) does not discover BeanKindSingleton.
     */
    @Produces
    @Singleton
    BeanKindSingleton beanKindSingleton(CallTrace callTrace) {
        BeanKindSingleton bean = new BeanKindSingleton();
        bean.setCallTrace(callTrace);
        return bean;
    }
}
