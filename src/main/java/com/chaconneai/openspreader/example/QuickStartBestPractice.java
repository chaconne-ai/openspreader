package com.chaconneai.openspreader.example;

import com.chaconneai.openspreader.cache.ProcessingCache;
import com.chaconneai.openspreader.sync.ProcessingMutex;
import com.chaconneai.openspreader.sync.ProcessingCountDownLatch;
import com.chaconneai.openspreader.sync.ProcessingSyncService;
import com.chaconneai.openspreader.pooling.ProcessingPool;
import com.chaconneai.openspreader.sync.ProcessingSemaphore;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Up and running in five minutes.
 *
 * <h2>Step one: add the dependency, and configure nothing</h2>
 * Adding {@code spreader-commons} is enough; auto-configuration builds the cluster and every
 * component below. <b>Not a line of configuration is needed</b> -- discovery defaults to
 * {@code 127.0.0.1}, so two instances on one machine, on different {@code server.port}s, form
 * a cluster by themselves.
 *
 * <p>Exactly one line has to change for production:
 * <pre>
 * spring.spreader.ip-addresses=192.168.0.111,192.168.0.63,192.168.0.77
 * </pre>
 * The cluster name ({@code spring.spreader.name}) is worth setting too, being the only way to
 * keep dev, staging and prod apart. The thirty or so remaining knobs live under
 * {@code spring.spreader.advanced.*}, their defaults calibrated by measurement, and the great
 * majority of projects never touch one from launch to retirement.
 *
 * <h2>Step two: inject what you need</h2>
 * They are all ordinary Spring beans; constructor injection is all it takes, as below.
 *
 * <h2>Step three: decide which one you want</h2>
 * <table border="1">
 *   <caption>What each of the seven components is for</caption>
 *   <tr><th>What you want to do</th><th>What to use</th></tr>
 *   <tr><td>Let one party do it at a time</td><td>{@link ProcessingMutex}</td></tr>
 *   <tr><td>Let at most N parties do it cluster-wide</td><td>{@link ProcessingSemaphore}</td></tr>
 *   <tr><td>Wait for N things to finish, once</td><td>{@code ProcessingCountDownLatch}</td></tr>
 *   <tr><td>Have N parties wait for each other, round by round</td>
 *       <td>{@code ProcessingCyclicBarrier}</td></tr>
 *   <tr><td>Run a scheduled task on one instance per round</td>
 *       <td>{@code @MultiProcessingScheduled}</td></tr>
 *   <tr><td>Hand computation to another replica</td><td>{@link ProcessingPool}</td></tr>
 *   <tr><td>Share data between processes, Redis-style</td><td>{@link ProcessingCache}</td></tr>
 * </table>
 *
 * <p>Each component's detailed usage lives in its own example package:
 * {@code example.mutex}, {@code example.semaphore}, {@code example.scheduling},
 * {@code example.pooling}, {@code example.cache}.
 *
 * <h2>Before starting, know where this stops</h2>
 * Every coordination capability rests on <b>one thing</b>: whoever takes the cluster port is
 * the leader, and the write path for locks, permits, latches, barriers and the cache is all
 * there. Two premises follow, and both have to be accepted:
 *
 * <ol>
 *   <li><b>Leader uniqueness is an agreement about timing, not a consensus protocol.</b>
 *       Under a network partition each side may have a leader, and at that moment two parties
 *       hold the same lock. So: coordinating things where repeating the work is merely
 *       wasteful is fine -- scheduled tasks, cache warming, batch de-duplication -- but
 *       <b>do not</b> use it to protect a transfer or a debit, where repeating it once causes
 *       real harm. That belongs in a database transaction, or a design with an idempotence
 *       key</li>
 *   <li><b>A change of leader leaves a gap of a few seconds.</b> Takeover is measured at 3.3
 *       to 4.4 seconds, during which lock acquisitions and cache writes retry. Leave business
 *       timeouts enough headroom, or you will see "it failed once and was fine a few seconds
 *       later"</li>
 * </ol>
 *
 * <p>What it buys in return: no ZooKeeper, no etcd, no Redis -- one jar and one line of
 * configuration. The trade is plain enough; whether your situation takes it is yours to
 * judge.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class QuickStartBestPractice {

    private final ProcessingSyncService syncs;
    private final ProcessingSyncService semaphores;
    private final ProcessingPool pool;
    private final ProcessingCache cache;

    /**
     * All by constructor injection. <b>Leave out what you do not need</b> -- each component
     * switches off on its own with
     * {@code spring.spreader.multiprocessing.<component>.enabled=false}, after which not even
     * the bean is created.
     */
    public QuickStartBestPractice(ProcessingSyncService syncs,
                             ProcessingSyncService semaphores,
                             ProcessingPool pool,
                             ProcessingCache cache) {
        this.syncs = syncs;
        this.semaphores = semaphores;
        this.pool = pool;
        this.cache = cache;
    }

    // ==================================================================
    // The shortest working code for each component
    // ==================================================================

    /**
     * A lock: take it and work, or skip.
     *
     * <p>{@code application(...)} means "among this application's replicas" and
     * {@code cluster(...)} means "the whole cluster, without regard to application". The former
     * is what is wanted nearly always.
     */
    public void mutexInThreeLines() {
        ProcessingMutex mutex = syncs.applicationMutex("daily-report");
        if (mutex.tryAcquire()) {
            try {
                generateReport();
            } finally {
                mutex.release();          // in a finally, always
            }
        }
    }

    /** A semaphore: at most five concurrent calls across the application, whatever the
     *  replica count. */
    public void semaphoreInThreeLines() throws InterruptedException {
        ProcessingSemaphore sem = semaphores.applicationSemaphore("third-party-api", 5);
        if (sem.tryAcquire(3, TimeUnit.SECONDS)) {
            try {
                callThirdParty();
            } finally {
                sem.release();
            }
        }
    }

    /**
     * A latch: wait for all three shards to finish before going on.
     *
     * <p>The three shards run in three different processes, and <b>every process runs this
     * code</b>: finish its own share, {@code countDown}, then wait for the other two.
     * {@code await} is woken by a <b>push</b> from the leader rather than by polling.
     *
     * <p>Without the {@code countDown} line, the three processes are three parties all waiting
     * and none decrementing -- hanging until the timeout.
     */
    public void latchInThreeLines() throws InterruptedException {
        ProcessingCountDownLatch latch = syncs.applicationLatch("import-batch-42", 3);
        processMyShard();
        latch.countDown();
        latch.await(5, TimeUnit.MINUTES);
        onAllShardsDone();
    }

    private void processMyShard() {
        // The one shard this process is responsible for
    }

    /**
     * A barrier: three parties wait for each other, moving forward round by round.
     *
     * <p>It differs from a latch in that it <b>can be reused</b>: once this round's parties
     * have all arrived they are released together, and the next round begins automatically.
     * Phased batch processing wants this.
     */
    public void barrierInThreeLines() throws Exception {
        syncs.applicationBarrier("phase-sync", 3).await(5, TimeUnit.MINUTES);
        startNextPhase();
    }

    /**
     * The task pool: sends computation to another replica.
     *
     * <p>The target method must carry {@code @MultiProcessingCall}. With a single replica it
     * runs locally of its own accord, so local development and production are the same code.
     */
    public String poolInThreeLines(String month) throws Exception {
        return pool.<String>submit("reportService", "render", month)
                .get(30, TimeUnit.SECONDS);
    }

    /**
     * The cache: data shared between processes, with operations matching Redis.
     *
     * <p>Reads come from <b>local memory</b> without touching the network -- measured in the
     * millions per second -- while writes are forwarded to the leader and broadcast back
     * incrementally, measured at 150,000 a second. So it naturally suits shared state that is
     * <b>read far more than written</b>: configuration, allow-lists, rate-limit counters,
     * sessions.
     *
     * <p>A write becomes visible on other nodes after a delay of milliseconds -- it is
     * <b>eventually consistent</b>, and is not a strongly consistent database.
     */
    public void cacheInThreeLines() {
        cache.set("feature:new-checkout", "on".getBytes(StandardCharsets.UTF_8));
        byte[] v = cache.get("feature:new-checkout");
        if (v != null && "on".equals(new String(v, StandardCharsets.UTF_8))) {
            useNewCheckout();
        }
    }

    /**
     * Scheduled-task exclusion needs nothing injected; one annotation does it:
     *
     * <pre>{@code
     * @Scheduled(cron = "0 0 2 * * *")
     * @MultiProcessingScheduled                  // this line, and no more
     * public void syncOrdersDaily() { ... }
     * }</pre>
     *
     * <p><b>Do not</b> write {@code if (cluster.isLeader())} -- the leader is a cluster-level
     * notion that does not distinguish applications, and where a cluster runs several
     * applications your task may never execute at all. See {@code example.scheduling}.
     */
    public void schedulingNeedsNoInjection() {
    }

    // ==================================================================

    private void generateReport() {
    }

    private void callThirdParty() {
    }

    private void onAllShardsDone() {
    }

    private void startNextPhase() {
    }

    private void useNewCheckout() {
    }
}
