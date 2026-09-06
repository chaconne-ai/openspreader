package com.chaconneai.openspreader.example.mutex;

import com.chaconneai.openspreader.sync.ProcessingCountDownLatch;
import com.chaconneai.openspreader.sync.ProcessingCyclicBarrier;
import com.chaconneai.openspreader.sync.ProcessingMutex;
import com.chaconneai.openspreader.sync.ProcessingSyncService;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * How to use the {@code mutex} package's three coordination primitives: locks, latches and
 * barriers.
 *
 * <h2>Choosing the right one first</h2>
 * <table border="1">
 *   <caption>Which primitive to use</caption>
 *   <tr><th>What you want to do</th><th>What to use</th></tr>
 *   <tr><td>Let one party do it at a time</td><td>{@link ProcessingMutex}</td></tr>
 *   <tr><td>Wait for N things to finish, once</td><td>{@link ProcessingCountDownLatch}</td></tr>
 *   <tr><td>Have N parties wait for each other, round by round</td>
 *       <td>{@link ProcessingCyclicBarrier}</td></tr>
 * </table>
 *
 * <h2>Two granularities, the same across all three factories</h2>
 * <ul>
 *   <li>{@code cluster(key, ...)} -- one across the whole cluster, <b>without regard to
 *       application</b>. An order service and a reporting service sharing a key hold each
 *       other back</li>
 *   <li>{@code application(key, ...)} -- in effect only <b>among instances of the same
 *       application</b>, with another application's identical key unaffected</li>
 * </ul>
 * A key may be any string; the factory adds the granularity prefix itself -- for instance
 * {@code my-cluster/order-service/daily-report}.
 *
 * <h2>All three share one ceiling</h2>
 * Their safety <b>does not exceed the strength of "there is one leader"</b>. Across machines,
 * that uniqueness is only an agreement about timing, and under a network partition each side
 * may have a leader of its own. So: coordinating things where repeating the work is merely
 * wasteful is fine; <b>do not</b> use them to protect a transfer or a debit, where repeating
 * it causes real harm.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MutexBestPractice {

    private final ProcessingSyncService syncs;

    public MutexBestPractice(ProcessingSyncService syncs) {
        this.syncs = syncs;
    }

    // ==================================================================
    // 1. Locks
    // ==================================================================

    /**
     * The commonest usage: take it and work, or skip the round.
     *
     * <p><b>Always use try/finally</b>, and release on the same thread -- the lock is bound to
     * its thread, exactly as {@code ReentrantLock} is.
     */
    public void runExclusively() {
        ProcessingMutex mutex = syncs.applicationMutex("nightly-rebuild");
        if (!mutex.tryAcquire()) {
            // Another instance is running it, so skip this round. This is the commonest shape
            // for a scheduled task
            return;
        }
        try {
            rebuildIndex();
        } finally {
            mutex.release();
        }
    }

    /**
     * When it really has to be acquired: wait a while.
     *
     * <p><b>Do not use the no-argument {@code acquire()}</b>, which has neither a timeout nor
     * deadlock detection. Two processes each holding one lock and waiting on the other's are
     * stuck for good.
     */
    public void runAfterWaiting() throws InterruptedException {
        ProcessingMutex mutex = syncs.clusterMutex("db-migration");
        if (!mutex.acquire(30, TimeUnit.SECONDS)) {
            throw new IllegalStateException("the migration lock could not be acquired within "
                    + "30 seconds; giving up on this attempt");
        }
        try {
            migrate();
        } finally {
            mutex.release();
        }
    }

    /**
     * Where a task finishes in milliseconds, exclusion alone is not enough.
     *
     * <p>An ordinary release frees the lock at once, another instance takes it immediately, and
     * the same scheduling period runs several times. <b>"One at a time" and "once per round"
     * are different things.</b> Releasing with a cooldown, which makes the lock unobtainable by
     * anyone for a while, is the latter.
     */
    public void runOncePerPeriod() {
        ProcessingMutex mutex = syncs.applicationMutex("heartbeat-report");
        if (!mutex.tryAcquire()) {
            return;
        }
        long started = System.currentTimeMillis();
        try {
            sendHeartbeat();          // may well finish in two milliseconds
        } finally {
            // The scheduling period is five seconds, so hold it down for five seconds less
            // what it actually took
            mutex.release(5_000L - (System.currentTimeMillis() - started));
        }
    }

    /**
     * Where slow work happens while holding the lock, <b>re-check</b> along the way.
     *
     * <p>A change of leader takes the register with it, and the lock in hand is invalidated at
     * once. Without re-checking, you carry on believing you hold it exclusively when you have
     * already lost it.
     */
    public void longRunningWithRecheck() {
        ProcessingMutex mutex = syncs.applicationMutex("bulk-export");
        if (!mutex.tryAcquire()) {
            return;
        }
        try {
            for (int chunk = 0; chunk < 1000; chunk++) {
                if (!mutex.isHeld()) {
                    // The lock is gone, most likely a change of leader. Stopping is safer than
                    // carrying on under a false assumption
                    throw new IllegalStateException("the lock was lost partway through the "
                            + "export; stopped at chunk " + chunk);
                }
                exportChunk(chunk);
            }
        } finally {
            mutex.release();
        }
    }

    // ==================================================================
    // 2. Latches: waiting for N things to finish
    // ==================================================================

    /**
     * Each shard counts down, and the coordinator waits for all of them.
     *
     * <p><b>Always count down with a participantId.</b> There are two reasons, and neither is
     * optional:
     * <ul>
     *   <li>A count-down request resent after a timeout does not decrement twice</li>
     *   <li>A change of leader returns the count to its initial value, and every party counting
     *       down again recovers it; the no-argument {@code countDown()} cannot recover</li>
     * </ul>
     */
    public void importShard(int shardId) {
        ProcessingCountDownLatch latch = syncs.applicationLatch("import-done", 5);
        doImport(shardId);
        latch.countDown("shard-" + shardId);      // idempotent, so a resend is safe
    }

    /**
     * Build the index once every shard has finished importing.
     *
     * <p>{@code await} has three outcomes, and all three want handling:
     * <ul>
     *   <li>true -- it reached zero</li>
     *   <li>false -- it timed out. <b>It never hangs</b></li>
     *   <li>{@link IllegalStateException} -- the leader changed and the latch is invalid. This
     *       is not an error but a fact you have to know: without it you would wait for ever on
     *       a count that no longer exists</li>
     * </ul>
     */
    public void buildIndexAfterImport() throws InterruptedException {
        ProcessingCountDownLatch latch = syncs.applicationLatch("import-done", 5);
        try {
            if (!latch.await(10, TimeUnit.MINUTES)) {
                throw new IllegalStateException("after ten minutes, " + latch.remaining()
                        + " shard(s) have still not finished importing");
            }
        } catch (IllegalStateException e) {
            // The leader changed. Having counted down with a participantId, another round
            // costs no more than each party counting down again
            throw new IllegalStateException("the latch is invalid and the round must be redone: "
                    + e.getMessage(), e);
        }
        buildIndex();
    }

    // ==================================================================
    // 3. Barriers: aligning round by round
    // ==================================================================

    /**
     * Five instances work in phases, each waiting for everyone at the end of a phase.
     *
     * <p>It differs from a latch in that a barrier <b>can be reused</b>: completing one round
     * moves to a new generation, ready for the next. And every party is both a waiter and an
     * arriver.
     *
     * <p>{@code await} returns the arrival index, and {@code parties - 1} means arriving last
     * -- which is how the last party can do a little tidying up, exactly as in the JDK.
     */
    public void processInPhases() throws InterruptedException {
        ProcessingCyclicBarrier barrier = syncs.applicationBarrier("phase-sync", 5);
        for (int phase = 0; phase < 3; phase++) {
            doPhase(phase);
            try {
                long index = barrier.await(5, TimeUnit.MINUTES);
                if (index == barrier.parties() - 1L) {
                    // Last to arrive, so write this phase's summary while here
                    summarizePhase(phase);
                }
            } catch (TimeoutException e) {
                // This party timed out. Note that this breaks the barrier, and the others
                // receive a BrokenBarrierException immediately afterwards
                throw new IllegalStateException("timed out waiting for the other instances in "
                        + "phase " + phase, e);
            } catch (BrokenBarrierException e) {
                // Someone else broke it first: a party timed out, was interrupted, or its node
                // departed
                throw new IllegalStateException("an instance fell behind in phase " + phase
                        + ", so the round is void", e);
            }
        }
    }

    /**
     * Using it again after it breaks means resetting it first.
     *
     * <p>And <b>one</b> instance should do the reset -- with every instance resetting, the
     * later resets break the parties that have only just arrived to wait. So the typical shape
     * guards it with a lock.
     */
    public void resetAfterFailure() {
        ProcessingCyclicBarrier barrier = syncs.applicationBarrier("phase-sync", 5);
        if (!barrier.isBroken()) {
            return;
        }
        ProcessingMutex mutex = syncs.applicationMutex("phase-sync-reset");
        if (mutex.tryAcquire()) {
            try {
                barrier.reset();
            } finally {
                // Held down a while, so another instance does not reset it straight after
                mutex.release(2_000L);
            }
        }
    }

    // ==================================================================

    private void rebuildIndex() {
    }

    private void migrate() {
    }

    private void sendHeartbeat() {
    }

    private void exportChunk(int chunk) {
    }

    private void doImport(int shardId) {
    }

    private void buildIndex() {
    }

    private void doPhase(int phase) {
    }

    private void summarizePhase(int phase) {
    }
}
