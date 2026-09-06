package com.chaconneai.openspreader.example.scheduling;

import com.chaconneai.openspreader.Scope;
import com.chaconneai.openspreader.scheduling.MultiProcessingScheduled;
import com.chaconneai.openspreader.scheduling.MultiProcessingTaskStats;

import java.util.Map;

/**
 * Cluster-wide exclusion for scheduled tasks: scale the application to N replicas, and the
 * task still runs once per round.
 *
 * <h2>It adds one line</h2>
 * The task definition remains Spring's own {@code @Scheduled} entirely -- cron, fixedDelay,
 * fixedRate, {@code @Async}, transactions, all unchanged. {@link MultiProcessingScheduled}
 * merely inserts "is this round mine?" before execution.
 *
 * <p>It is therefore <b>pluggable</b>: remove the annotation and the behaviour returns at once
 * to what it was before this package; set
 * {@code spring.spreader.multiprocessing.scheduling.enabled=false} and the components are not
 * even created.
 *
 * <h2>Three things to settle first</h2>
 * <ol>
 *   <li><b>Failing to take the lock skips the round rather than queueing.</b> Queueing
 *       scheduled tasks only piles them up, and the next round is along in a moment
 *       anyway</li>
 *   <li><b>A light task body needs {@code lockAtLeastMs}</b>, or the lock is released
 *       instantly and another instance runs the same round again, which amounts to no
 *       exclusion at all. The default is computed from the period, so it rarely needs setting
 *       by hand</li>
 *   <li><b>There is no guarantee which machine it runs on</b>, nor that the distribution is
 *       even. A cluster-scoped task in fact settles on the leader; see the note in
 *       {@link MultiProcessingScheduled}</li>
 * </ol>
 *
 * <h2>Do not use isLeader() instead</h2>
 * This is the commonest mistake. The leader is a <b>cluster-level</b> notion that does not
 * distinguish applications -- where an order service and a reporting service share a cluster,
 * the leader may happen to be an instance of the reporting service, and a scheduled task in
 * the order service guarded by {@code isLeader()} would then <b>never run at all</b>.
 *
 * <p>The examples below are written as Spring beans; copy them directly.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class ScheduledTaskBestPractice {

    private final MultiProcessingTaskStats stats;

    public ScheduledTaskBestPractice(MultiProcessingTaskStats stats) {
        this.stats = stats;
    }

    // ==================================================================
    // 1. The common case: one annotation and it is done
    // ==================================================================

    /**
     * <b>The standard form.</b> The granularity defaults to {@link Scope#APPLICATION}, that is
     * "one of this application's replicas runs it". The great majority of tasks want this.
     *
     * <pre>{@code
     * @Scheduled(cron = "0 0 2 * * *")
     * @MultiProcessingScheduled
     * public void syncOrdersDaily() { ... }
     * }</pre>
     *
     * <p>A blank lock name becomes "class name # method name", which is enough.
     */
    public void syncOrdersDaily() {
        // @Scheduled(cron = "0 0 2 * * *")
        // @MultiProcessingScheduled
    }

    /**
     * Cluster granularity: one run <b>across the whole cluster</b>, however many different
     * applications there are.
     *
     * <pre>{@code
     * @Scheduled(fixedDelay = 300_000)
     * @MultiProcessingScheduled(value = "cleanup-shared-tmp", scope = Scope.CLUSTER)
     * public void cleanupSharedTemp() { ... }
     * }</pre>
     *
     * <p>For tasks operating on a <b>shared resource</b> -- cleaning a shared temporary
     * directory, archiving a table every service writes to. Such tasks have nothing to do with
     * which application is running, and application granularity would have each application
     * run its own.
     *
     * <p>The cost is that it <b>settles on the leader's</b> machine: the leader takes the lock
     * without touching the network and wins every round. Heavy work should keep that in
     * mind.
     */
    public void cleanupSharedTemp() {
        // @Scheduled(fixedDelay = 300_000)
        // @MultiProcessingScheduled(value = "cleanup-shared-tmp", scope = Scope.CLUSTER)
    }

    /**
     * For two different methods that should exclude each other: <b>give them the same lock
     * name</b>.
     *
     * <pre>{@code
     * @Scheduled(cron = "0 0/5 * * * *")
     * @MultiProcessingScheduled("order-index")     // the same name
     * public void incrementalIndex() { ... }
     *
     * @Scheduled(cron = "0 0 3 * * *")
     * @MultiProcessingScheduled("order-index")     // same name, so the two never run at once
     * public void fullRebuildIndex() { ... }
     * }</pre>
     *
     * <p>An incremental rebuild and a full rebuild meeting would tread on each other; the same
     * name keeps them apart of its own accord.
     */
    public void sameLockNameMeansMutuallyExclusive() {
    }

    // ==================================================================
    // 2. The two optional parameters, and when they need changing
    // ==================================================================

    /**
     * {@code lockAtLeastMs} -- how long the lock is held down at minimum, so that one round
     * cannot run several times.
     *
     * <pre>{@code
     * @Scheduled(fixedDelay = 100)
     * @MultiProcessingScheduled(value = "fast-task", lockAtLeastMs = 400)
     * public void veryFastTask() { ... }   // the task body takes only microseconds
     * }</pre>
     *
     * <p>The default of -1 derives it from the scheduling period, and in the great majority of
     * cases needs no attention. There is one case for setting it by hand: <b>a task body far
     * quicker than the scheduling period</b> -- finishing in microseconds releases the lock,
     * another instance takes it at the very next scheduling point, and one round runs several
     * times. Only holding it down for longer than the interval leaves the other instances
     * genuinely idle.
     *
     * <p>Conversely, where the task itself runs longer than the period, this value does nothing
     * -- the lock is held throughout anyway.
     */
    public void veryFastTask() {
    }

    /**
     * {@code waitMs} -- how long to wait before giving up on the lock; 0 by default, giving up
     * at once.
     *
     * <pre>{@code
     * @Scheduled(cron = "0 0 * * * *")
     * @MultiProcessingScheduled(value = "hourly-settle", waitMs = 5_000)
     * public void hourlySettle() { ... }
     * }</pre>
     *
     * <p><b>Usually leave it alone.</b> Skipping the round is almost always right, and the
     * next one is along in a moment.
     *
     * <p>There is one case worth setting it for: a long period -- an hour, a day -- where
     * <b>the instance holding the lock may be about to exit</b>, partway through a rolling
     * deployment. A few seconds' wait picks the round up; otherwise it waits for the next
     * hour.
     */
    public void hourlySettle() {
    }

    // ==================================================================
    // 3. Checking whether it actually took effect
    // ==================================================================

    /**
     * {@link MultiProcessingTaskStats} records how many times each exclusive task executed,
     * how many it skipped, and how long it took.
     *
     * <p>How to read it:
     * <ul>
     *   <li>{@code executed} stuck at 0 means this instance has never taken the lock. That is
     *       wrong with one instance and normal with several</li>
     *   <li>{@code skipped} stuck at 0 with a single instance is correct -- one instance should
     *       take every round</li>
     *   <li>{@code skipped} stuck at 0 with several instances means <b>the exclusion is not
     *       working</b>, usually because the task body is so quick that the lock is released
     *       instantly. Set {@code lockAtLeastMs}</li>
     *   <li>A task method missing from the map altogether means the annotation was not
     *       recognised: check the method is public, and that the bean really is proxied</li>
     * </ul>
     *
     * <p>Tasks without the annotation <b>do not</b> appear here -- which is the evidence that
     * it is pluggable.
     */
    public void inspect() {
        Map<String, Map<String, Object>> snapshot = stats.snapshot();
        snapshot.forEach((task, m) -> {
            long executed = (Long) m.getOrDefault("executed", 0L);
            long skipped = (Long) m.getOrDefault("skipped", 0L);
            metrics(task + ".executed", executed);
            metrics(task + ".skipped", skipped);
        });
    }

    // ==================================================================

    private void metrics(String name, long value) {
    }
}
