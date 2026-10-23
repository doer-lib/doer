package com.doer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method of a bean that loads task data of its return type. The method
 * takes a {@link TaskSnapshot} and returns the data of that task. Declare one
 * loader per type. Each doer method ({@link AcceptStatus}) parameter other than
 * {@link Task} needs a loader of its type.
 *
 * <h2>Example</h2>
 * <pre>
 * &#64;TaskDataLoader
 * public OrderData loadOrder(TaskSnapshot task) {
 *     String jpql = "SELECT o FROM OrderData o WHERE o.taskId = :taskId";
 *     return em.createQuery(jpql, OrderData.class)
 *             .setParameter("taskId", task.id())
 *             .getSingleResult();
 * }
 * </pre>
 *
 * <h2>Transactions of a doer method</h2>
 * The loader runs in the transaction that marks the task in progress. This
 * transaction commits before the doer method is called: the doer method runs
 * outside the transactions of Doer, and the {@link TaskDataSaver} of the same
 * type, if there is one, runs in a new transaction after it. When the loader
 * fails, its transaction is rolled back, the doer method is not called, and the
 * failure is recorded as a failure of the doer method (see {@link RetryPolicy}).
 *
 * <h2>One transaction in facilitateCoordinatedUpdate</h2>
 * {@link DoerService#facilitateCoordinatedUpdate(long, java.time.Duration, boolean, Class, TaskAndDataUpdater)}
 * calls the loader, the updater and the saver in <b>one</b> transaction, with the task
 * row locked.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.CLASS)
public @interface TaskDataLoader {
}
