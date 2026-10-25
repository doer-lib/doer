package com.doer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method of a bean that saves task data of the type of its second
 * parameter. The method takes a {@link TaskSnapshot} and the data, and returns
 * void. Declare at most one saver per type; data of a type without a saver is
 * not saved by Doer.
 *
 * <h2>Example</h2>
 * <pre>
 * &#64;TaskDataSaver
 * public void saveOrder(TaskSnapshot task, OrderData order) {
 *     em.merge(order);
 * }
 * </pre>
 *
 * <h2>Transactions of a doer method</h2>
 * Doer runs a doer method ({@link AcceptStatus}) in three steps:
 * <ol>
 * <li>the first transaction marks the task in progress and calls the
 * {@link TaskDataLoader}s;</li>
 * <li>the doer method, outside the transactions of Doer: a
 * {@code @Transactional} doer method runs in a transaction of its own, which
 * commits when the method returns;</li>
 * <li>after the doer method succeeds, the second transaction writes the new
 * status of the task and calls the savers.</li>
 * </ol>
 * So the saver runs neither in the first transaction, with the loader, nor in
 * the one of the doer method. With a transaction-scoped persistence context, an entity the
 * loader returned is detached by then, so the saver merges it. The saver gets
 * the snapshot of the task with the new status. When the saver fails, its
 * transaction is rolled back: neither the data nor the new status is saved, and
 * the failure is recorded as a failure of the doer method (see
 * {@link RetryPolicy}). What a {@code @Transactional} doer method has written
 * stays committed, and the method runs again on the retry.
 *
 * <h2>One transaction in facilitateCoordinatedUpdate</h2>
 * {@link DoerService#facilitateCoordinatedUpdate(long, java.time.Duration, boolean, Class, TaskAndDataUpdater)}
 * calls the loader, the updater and the saver in <b>one</b> transaction, with the task
 * row locked; the saver runs after the task is written.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.CLASS)
public @interface TaskDataSaver {
}
