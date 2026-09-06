package com.chaconneai.openspreader.sync;

import java.util.concurrent.TimeUnit;

/**
 * A cross-process semaphore: at most N holders cluster-wide at any moment.
 *
 * <p>Used exactly as {@code java.util.concurrent.Semaphore} is, and note that the check
 * comes before the release:
 * <pre>{@code
 * if (semaphore.tryAcquire()) {
 *     try {
 *         // At most N threads cluster-wide reach here at once
 *     } finally {
 *         semaphore.release();
 *     }
 * }
 * }</pre>
 *
 * <h2>How it differs from {@code ProcessingMutex}</h2>
 * <ul>
 *   <li><b>Not reentrant</b> -- one thread acquiring twice has taken two permits and must
 *       return two. That is what a semaphore means.</li>
 *   <li><b>Not bound to a thread</b> -- thread A acquiring and thread B releasing is
 *       permitted, as with {@code java.util.concurrent.Semaphore}. The price is that
 *       releasing more than was acquired is not stopped, and the surplus conjures permits
 *       out of nothing -- so always pair them.</li>
 * </ul>
 *
 * <h2>What it guarantees</h2>
 * During normal operation, no more than {@link #permits()} are held at once; and when a
 * holder crashes, departs, or gets stuck and stops renewing, the permits it held are
 * released.
 *
 * <h2>What it does not guarantee</h2>
 * Its strength <b>does not exceed that of "there is one leader"</b>. Under a network
 * partition across machines each side may have a leader of its own, each admitting by its
 * own ledger, and the total exceeds the configured value.
 *
 * <p>So: rate limiting and bounding concurrency are fine ("a few extra through merely means
 * slower"). <b>Do not</b> use it for a hard constraint that must never exceed N -- billing
 * by permit count, for instance.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface ProcessingSemaphore {

    /** The semaphore's full name, including the granularity prefix. */
    String name();

    /** The total number of permits. */
    int permits();

    /** Takes one permit, without waiting. */
    boolean tryAcquire();

    /** Takes one permit, retrying until the timeout if none is available. */
    boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Takes one permit, retrying until the timeout if none is available.
     *
     * <p>Behaves identically to {@link #tryAcquire(long, TimeUnit)}; both exist only to suit
     * different calling habits.
     */
    boolean acquire(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Takes one permit, waiting until it is acquired.
     *
     * <p>While leadership is vacant it keeps retrying, resuming once a new leader takes over.
     * An interrupted thread gives up and returns false.
     *
     * <p><b>There is no deadlock detection</b> and the wait is unbounded. To hold several
     * different semaphores at once, use the timed form, or agree on a fixed acquisition
     * order globally.
     */
    boolean acquire();

    /** Returns one permit. Holding none, it does nothing, so it is safe in a finally block. */
    void release();

    /**
     * How many permits remain available cluster-wide.
     *
     * <p>It queries the leader rather than deciding locally. What comes back is the value at
     * <b>that instant</b>, and someone may take it the moment it is read. Treat it as an
     * indication; do not reason "there is room, so my acquisition will succeed".
     *
     * @return how many permits remain, or -1 when leadership is vacant or the query failed
     */
    int availablePermits();

    /** How many permits this node currently holds. Decided locally, with no network request. */
    int heldPermits();
}
