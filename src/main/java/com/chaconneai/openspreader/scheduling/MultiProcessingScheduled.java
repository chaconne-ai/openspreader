package com.chaconneai.openspreader.scheduling;

import com.chaconneai.openspreader.Scope;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Placed on a {@code @Scheduled} method, this makes the task run on <b>one instance in the
 * cluster at a time</b>.
 *
 * <pre>{@code
 * @Scheduled(fixedDelay = 60_000)
 * @MultiProcessingScheduled                 // this one line is enough
 * public void syncOrders() {
 *     // Scale the application to five replicas, and one replica still runs each round
 * }
 * }</pre>
 *
 * <h2>How it works</h2>
 * Before each firing it takes a
 * {@link com.chaconneai.openspreader.sync.ProcessingMutex}: taking it runs the task, and
 * failing to take it <b>skips the round</b> -- it does not queue, which would only pile tasks
 * up.
 *
 * <p>The lock <b>is not released the moment the task finishes</b>; it is held down to the end
 * of the round -- see {@link #lockAtLeastMs()}. Without that, a task finishing in a few
 * milliseconds releases the lock, another instance takes it at once, and the same round runs
 * several times, which amounts to no exclusion at all.
 *
 * <h2>Every instance still schedules; those that miss the lock simply spin once</h2>
 * All three instances' schedulers <b>fire on time as usual</b>; the one holding the lock runs
 * the body and the other two return. What is saved is the repeated execution of the body, not
 * the scheduling. Each spin costs one network round trip for the lock -- milliseconds -- and
 * an immediate return.
 *
 * <p>So with a light body and dense scheduling -- a round every 100 milliseconds, say -- that
 * spinning is a fair share of the cost, and such a case is worth reconsidering: is exclusion
 * across instances really needed?
 *
 * <h2>Which machine the task actually lands on</h2>
 * Once the cooldown passes everyone races again, but <b>the odds are not even</b>: the leader
 * takes the lock by consulting a register in its own memory, while every other node needs a
 * network round trip -- measured at about 1.5 milliseconds' difference. The leader gets there
 * first every round. The upshot:
 *
 * <table border="1">
 *   <caption>Where tasks actually run</caption>
 *   <tr><th>Task granularity</th><th>Where it lands</th></tr>
 *   <tr><td>{@link Scope#CLUSTER}</td>
 *       <td><b>Fixed on the leader</b> -- the whole cluster races and the leader always
 *           wins</td></tr>
 *   <tr><td>{@link Scope#APPLICATION}, where this application is the leader's</td>
 *       <td><b>Fixed on the leader</b></td></tr>
 *   <tr><td>{@link Scope#APPLICATION}, other applications</td>
 *       <td><b>Rotates</b> among their own instances -- none of them is the leader, their
 *           latencies match, and the competition is fair. But it is only "whoever's scheduling
 *           point comes first takes it", not strict round-robin; two instances have been
 *           measured splitting 8 to 2</td></tr>
 * </table>
 *
 * <p>So the leader's machine additionally carries "its own application's tasks plus every
 * cluster-scoped task". With light tasks -- sending notifications, refreshing a cache,
 * scanning a small table -- that is of no consequence; with heavy ones, note that this machine
 * also hosts the lock register, so the load compounds.
 *
 * <p>Tasks move to the new leader on a change of leadership, without interruption. This
 * mechanism <b>does not guarantee</b> "always runs on the same machine"; stickiness needs
 * another approach.
 *
 * <h2>Do not use "am I the leader" instead</h2>
 * {@code cluster.isLeader()} is <b>cluster-level</b> -- one process in the whole cluster holds
 * the cluster port, and it does not distinguish applications. Where an order service and a
 * reporting service share a cluster, the leader may happen to be a reporting service instance,
 * and a scheduled task in the order service guarded by it would <b>never run at all</b>,
 * because no instance of the order service is the leader.
 *
 * <p>The leader means "who holds that port", which has nothing to do with "who should run this
 * application's tasks". A scheduled task wants "one of the instances of this application", and
 * that means taking a lock:
 * <pre>{@code
 * boolean got = mutex.tryAcquire();
 * if (got) { doTheWork(); } else { skipThisRound(); }
 * }</pre>
 * which is precisely what this annotation does inside.
 *
 * <h2>What happens when execution outlasts the lease</h2>
 * Nothing goes wrong. The lock service renews the lease while it is held, so a task may run as
 * long as it likes. Only a crashed process, or one stuck badly enough to stop renewing, loses
 * the lock to someone else.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MultiProcessingScheduled {

    /**
     * The lock name. Left blank, it becomes "class name # method name", which is usually
     * enough.
     *
     * <p>Where two different methods should exclude each other -- two entry points to the same
     * data, say -- give them the same name.
     */
    String value() default "";

    /**
     * The granularity, {@link Scope#APPLICATION} by default -- exclusive only against
     * <b>other instances of the same application</b>.
     *
     * <p>This is what a scheduled task wants: one of the order service's five replicas runs it,
     * without preventing the reporting service from running a task of its own by the same name.
     *
     * <p>{@link Scope#CLUSTER} instead means one across the whole cluster, without regard to
     * application.
     */
    Scope scope() default Scope.APPLICATION;

    /**
     * How long to wait for the lock at most, in milliseconds; 0 by default, which does not
     * wait and skips the round.
     *
     * <p>A scheduled task usually should not wait: succeeding only crowds two executions
     * together. It is worth setting only for a short task that genuinely requires "someone must
     * complete every round".
     */
    long waitMs() default 0L;

    /**
     * How long the lock is held at minimum, in milliseconds -- that is, how long it stays held
     * down after the task finishes.
     *
     * <p><b>-1 by default, meaning this task's scheduling interval</b> -- the value of
     * {@code fixedDelay} or {@code fixedRate} -- which gives "once per period", and is what a
     * scheduled task nearly always wants.
     *
     * <p>Why it is needed: a task body often finishes in a few milliseconds, and releasing the
     * lock then lets another instance take it at once, so the same round runs several times.
     * Only holding it down for a period is genuine de-duplication.
     *
     * <p>A cron task's interval cannot be worked out, so it defaults to 0 -- released as soon
     * as it finishes. Where such a task's firings are far apart the window for a repeat is
     * short and usually harmless; for more certainty, give a value here slightly below the
     * firing interval.
     *
     * <p>0 means released as soon as it finishes, and whoever takes it next has it.
     */
    long lockAtLeastMs() default -1L;
}
