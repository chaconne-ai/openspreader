package com.chaconneai.openspreader.sync;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A cross-process barrier: N parties wait for one another, proceed together once all have
 * arrived, and can then run another round.
 *
 * <p>Used exactly as {@code java.util.concurrent.CyclicBarrier} is:
 * <pre>{@code
 * ProcessingCyclicBarrier barrier = factory.application("phase-sync", 5);
 *
 * for (int phase = 0; phase < 3; phase++) {
 *     doPhase(phase);                       // each process does its own work
 *     barrier.await(5, TimeUnit.MINUTES);   // wait for everyone to finish this phase
 * }
 * }</pre>
 *
 * <h2>Choosing between this and a latch</h2>
 * <ul>
 *   <li>{@link ProcessingCountDownLatch}: <b>one-shot</b>, with waiters and counters being
 *       different parties. "Wait for five things to finish"</li>
 *   <li>This interface: <b>repeatable</b>, with the participants waiting for one another --
 *       each is both a waiter and an arriver. "Five parties align once per round"</li>
 * </ul>
 *
 * <h2>A participant is a thread</h2>
 * As in the JDK, one {@link #await} counts as one arrival. {@code parties=5} may be one
 * thread in each of five processes, five threads in one process, or any mixture.
 *
 * <h2>Once broken it must be reset</h2>
 * The moment one party can wait no longer -- a timeout, an interrupt, its node departing --
 * or the leader changes, this generation is <b>broken</b> and every other party immediately
 * receives a {@link BrokenBarrierException}, rather than going on waiting for a count that
 * will never be reached. This is exactly what the single-machine version does. To carry on,
 * {@link #reset()} first.
 *
 * <h2>What not to use it for</h2>
 * Its safety does not exceed the strength of "there is one leader". Under a network
 * partition each side has a leader of its own, each fills the barrier, and one round is
 * released twice. Aligning progress with it is fine; do not treat "everyone has arrived" as
 * a transactional guarantee of any kind.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface ProcessingCyclicBarrier {

    /** The barrier's full name, including the granularity prefix. */
    String name();

    /** The party count fixed at declaration. */
    int parties();

    /**
     * Arrives and waits for the others, without a time limit.
     *
     * @return the arrival index; {@code parties() - 1} means this party arrived last -- as in
     *         the JDK, that is how the last arrival can be given some closing work to do
     * @throws BrokenBarrierException when the barrier was broken
     */
    long await() throws InterruptedException, BrokenBarrierException;

    /**
     * Arrives and waits for the others, with a timeout.
     *
     * @return the arrival index
     * @throws TimeoutException       when the wait timed out. <b>Note this breaks the
     *                                barrier</b>, and every other party then receives a
     *                                {@link BrokenBarrierException} -- as in the JDK
     * @throws BrokenBarrierException when someone else has already broken it
     */
    long await(long timeout, TimeUnit unit)
            throws InterruptedException, BrokenBarrierException, TimeoutException;

    /**
     * How many have arrived this generation.
     *
     * <p>It queries the leader rather than deciding locally.
     */
    int arrivedCount();

    /** Whether this generation is broken. Also a network query. */
    boolean isBroken();

    /**
     * Resets: discards this generation and starts afresh.
     *
     * <p>Parties still waiting immediately receive a {@link BrokenBarrierException}. After a
     * break, this must be called before the barrier can be used again.
     */
    void reset();
}
