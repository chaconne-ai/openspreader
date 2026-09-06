package com.chaconneai.openspreader.sync;

import java.util.concurrent.TimeUnit;

/**
 * A cross-process latch: wait for N things to finish, wherever they happen.
 *
 * <p>Used exactly as {@code java.util.concurrent.CountDownLatch} is:
 * <pre>{@code
 * // wait for all five shards to finish importing
 * ProcessingCountDownLatch latch = factory.application("import-done", 5);
 *
 * // each process counts down once it has done its share
 * latch.countDown(shardId);
 *
 * // any process that wants to wait, and there may be several
 * if (latch.await(10, TimeUnit.MINUTES)) {
 *     // all five are done
 * }
 * }</pre>
 *
 * <h2>Pass a participantId</h2>
 * {@link #countDown(String)} carries a participant identifier and the leader
 * <b>de-duplicates</b> by it: one participant counting down repeatedly counts once. That
 * brings two benefits, neither optional:
 * <ul>
 *   <li><b>Retry safety</b> -- when the count-down's network request times out, resending it
 *       does not decrement twice</li>
 *   <li><b>Recovery after a change of leader</b> -- the count returns to its initial value on
 *       the new leader, and every party counting down again arrives back at the right number.
 *       The no-argument {@link #countDown()} <b>cannot recover</b></li>
 * </ul>
 *
 * <h2>One-shot; it cannot be reused</h2>
 * As in the JDK, once the count reaches zero it stays there. To synchronise round after
 * round, use
 * {@link ProcessingCyclicBarrier}。
 *
 * <h2>A change of leader invalidates the latch</h2>
 * The register lives only in the leader's memory. After a change, {@link #await}
 * <b>throws {@link IllegalStateException}</b> rather than continuing to wait -- continuing
 * would mean hanging forever.
 * Callers should be ready to run the round again -- with participantId de-duplication, the
 * cost of doing so is one recount per party.
 *
 * <h2>What not to use it for</h2>
 * Its safety does not exceed the strength of "there is one leader". Under a network partition
 * each side may have a leader of its own and each reach zero. Coordinating something where
 * one extra release is merely wasteful is fine; do not make it the commit point of a
 * distributed transaction.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface ProcessingCountDownLatch {

    /** The latch's full name, including the granularity prefix. */
    String name();

    /** The initial count fixed at declaration. */
    long initialCount();

    /**
     * Counts down by one, de-duplicated per participant.
     *
     * <p><b>Prefer this to {@link #countDown()}</b>; the class documentation says why.
     *
     * @param participantId the participant's identifier. One participant counting down
     *                      repeatedly counts once. A shard number or task number -- something
     *                      naturally unique in the application -- is the usual choice
     * @return the count remaining afterwards, or -1 on failure
     */
    long countDown(String participantId);

    /**
     * Counts down by one, plainly: one decrement per call.
     *
     * <p>Semantically the JDK's {@code CountDownLatch.countDown()}, but with two traps in a
     * distributed setting: resending after a timed-out request <b>decrements twice</b>, and
     * the count <b>cannot recover</b> from a change of leader. Unless your case genuinely is
     * "anyone may count, as long as enough counts arrive", use {@link #countDown(String)}.
     */
    void countDown();

    /**
     * Waits for the count to reach zero.
     *
     * @return true when it reached zero, false on timeout
     * @throws IllegalStateException when a change of leader invalidated the latch
     */
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Waits for the count to reach zero, without a time limit.
     *
     * <p>While leadership is vacant it keeps retrying, resuming once a new leader takes over.
     *
     * @throws IllegalStateException when a change of leader invalidated the latch
     */
    void await() throws InterruptedException;

    /**
     * The count remaining.
     *
     * <p>It queries the leader rather than deciding locally.
     *
     * @return the count remaining, or -1 when it cannot be found
     */
    long remaining();

    /** Whether the count has already reached zero. Also a network query. */
    boolean isSatisfied();
}
