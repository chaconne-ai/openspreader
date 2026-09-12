/*
 * Copyright 2026 ChaconneAI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chaconneai.openspreader.metrics;

import com.chaconneai.openspreader.MultiProcessingService;
import com.chaconneai.openspreader.cache.CacheService;
import com.chaconneai.openspreader.rpc.RpcService;
import com.chaconneai.openspreader.sync.BarrierService;
import com.chaconneai.openspreader.sync.ExchangerService;
import com.chaconneai.openspreader.sync.LatchService;
import com.chaconneai.openspreader.sync.SemaphoreService;
import com.chaconneai.openspreader.pooling.PoolService;
import com.chaconneai.openspreader.scheduling.MultiProcessingTaskStats;
import com.chaconneai.openspreader.sync.MutexService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToDoubleFunction;

/**
 * Binds the <b>component layer</b>'s runtime state to Micrometer.
 *
 * <p>{@link SpreaderMeterBinder} covers the transport layer -- channel throughput, latency
 * distributions, buffer levels -- while this class covers the components running on top of it:
 * the cache, locks, task dispatch and scheduled tasks.
 *
 * <h2>The criterion for choosing a metric: can it warn early</h2>
 * Not "report whatever there is". Every one of these corresponds to a failure that <b>makes no
 * sound in the log</b>:
 *
 * <table border="1">
 *   <caption>What each warns about</caption>
 *   <tr><th>Metric</th><th>What it warns about</th></tr>
 *   <tr><td>{@code cache.outbox.overflow}</td>
 *       <td><b>The one most worth alerting on.</b> The broadcast queue is full and updates are
 *       discarded outright. Followers never receive them and can only pull a full snapshot
 *       once they notice the version gap -- the beginning of an avalanche, while the
 *       application side merely feels that it "occasionally reads stale data"</td></tr>
 *   <tr><td>{@code cache.resync.count}</td>
 *       <td>Full synchronisations. Steady growth means broadcasts are being lost, so the
 *       network or the queue has a problem</td></tr>
 *   <tr><td>{@code mutex.contention.rate}</td>
 *       <td>Lock contention. It <b>makes no sound at all</b> in the log and shows up only as
 *       "the system feels slow"</td></tr>
 *   <tr><td>{@code mutex.acquire.timeouts}</td>
 *       <td>Persistently non-zero is a fault: requests are timing out without the lock</td></tr>
 *   <tr><td>{@code pool.remote.ratio}</td>
 *       <td>The proportion of tasks dispatched outward. Local and remote throughput differ
 *       <b>by a factor of 25</b>, so this ratio decides overall throughput outright</td></tr>
 *   <tr><td>{@code pool.local.blocked.rate}</td>
 *       <td>How many threads are stuck in join waiting on subtasks. Near 1 means the recursion
 *       divides too deeply, and a smaller max-depth is in fact faster</td></tr>
 *   <tr><td>{@code scheduled.skipped}</td>
 *       <td>How often a scheduled task failed to take the lock. Its ratio to executed should
 *       be roughly "instances - 1 : 1", and a clear departure means leadership is
 *       unsteady</td></tr>
 * </table>
 *
 * <h2>Why these are all gauges rather than counters</h2>
 * The authoritative source of these numbers is each service's own {@code AtomicLong}, and
 * Micrometer merely <b>reads</b> them. A counter would mean calling
 * {@code counter.increment()} in the code itself, splitting the counting into two places --
 * and one of them would be missed sooner or later. A gauge always reads the authoritative
 * value, and nothing disagrees after a restart.
 *
 * <p>The cost is that these cumulative figures are typed gauge rather than counter in
 * Prometheus, which is worth remembering before reaching for {@code rate()}; they are
 * monotonic, though, so {@code increase()} works as it should.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
public class ComponentMeterBinder implements MeterBinder {

    private final CacheService cache;
    private final MutexService mutex;
    private final PoolService pool;
    private final MultiProcessingTaskStats scheduled;
    private final LatchService latch;
    private final BarrierService barrier;
    private final SemaphoreService semaphore;
    private final ExchangerService exchanger;
    private final RpcService rpc;

    /** Task names already registered, so nothing is registered twice. */
    private final Set<String> registeredTasks = ConcurrentHashMap.newKeySet();

    /**
     * All four components are <b>optional</b> -- each has a switch of its own in the
     * configuration, and one that is off is passed as {@code null}. A null simply leaves that
     * group of metrics unregistered, rather than registering a set of permanent zeroes: a
     * permanently zero metric is worse than none, because it reads as "measured, and fine".
     */
    public ComponentMeterBinder(CacheService cache, MutexService mutex,
                                PoolService pool, MultiProcessingTaskStats scheduled) {
        this(cache, mutex, pool, scheduled, null, null, null, null, null);
    }

    /**
     * All nine components are <b>optional</b>, and one that is off is passed as {@code null};
     * see the note on the constructor above.
     */
    public ComponentMeterBinder(CacheService cache, MutexService mutex,
                                PoolService pool, MultiProcessingTaskStats scheduled,
                                LatchService latch, BarrierService barrier,
                                SemaphoreService semaphore, ExchangerService exchanger,
                                RpcService rpc) {
        this.cache = cache;
        this.mutex = mutex;
        this.pool = pool;
        this.scheduled = scheduled;
        this.latch = latch;
        this.barrier = barrier;
        this.semaphore = semaphore;
        this.exchanger = exchanger;
        this.rpc = rpc;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        if (cache != null) {
            bindCache(registry);
        }
        if (mutex != null) {
            bindMutex(registry);
        }
        if (pool != null) {
            bindPool(registry);
        }
        if (scheduled != null) {
            bindScheduled(registry);
        }
        if (latch != null) {
            bindLatch(registry);
        }
        if (barrier != null) {
            bindBarrier(registry);
        }
        if (semaphore != null) {
            bindSemaphore(registry);
        }
        if (exchanger != null) {
            bindExchanger(registry);
        }
        if (rpc != null) {
            bindRpc(registry);
        }
    }

    // ------------------------------------------------------------------
    // Cache
    // ------------------------------------------------------------------

    private void bindCache(MeterRegistry registry) {
        num(registry, "spreader.cache.keys", "key count", cache, c -> stat(c, "keyCount"));
        num(registry, "spreader.cache.bytes", "approximate bytes held",
                cache, c -> stat(c, "approxBytes"));
        num(registry, "spreader.cache.evicted",
                "keys evicted in total; quick growth means the capacity is too small",
                cache, c -> stat(c, "evicted"));
        num(registry, "spreader.cache.ops.applied", "operations applied in total",
                cache, c -> stat(c, "opsApplied"));
        num(registry, "spreader.cache.ops.sent", "operations broadcast in total",
                cache, c -> stat(c, "opsSent"));
        num(registry, "spreader.cache.frames.sent",
                "frames broadcast in total; ops over frames is the batching efficiency",
                cache, c -> stat(c, "framesSent"));
        num(registry, "spreader.cache.outbox.depth", "current depth of the broadcast queue",
                cache, c -> stat(c, "outboxDepth"));
        num(registry, "spreader.cache.outbox.overflow",
                "broadcast queue overflows; non-zero deserves an alert, because updates are\n"
                        + "discarded and replicas can only catch up by pulling a full snapshot",
                cache, c -> stat(c, "outboxOverflow"));
        num(registry, "spreader.cache.buffered.updates",
                "updates buffered because of a version gap; persistently non-zero means "
                        + "messages are being lost in between",
                cache, c -> stat(c, "bufferedUpdates"));
        num(registry, "spreader.cache.resync.count",
                "full synchronisations; steady growth means broadcasts are being lost",
                cache, c -> stat(c, "resyncCount"));
        num(registry, "spreader.cache.epoch",
                "the current epoch; frequent changes mean the leader is unsteady",
                cache, c -> stat(c, "epoch"));
        num(registry, "spreader.cache.applied.version", "the version applied so far",
                cache, c -> stat(c, "appliedVersion"));
        num(registry, "spreader.cache.leader", "whether this node is the cache leader, 1 or 0",
                cache, c -> boolStat(c, "leader"));
        num(registry, "spreader.cache.syncing",
                "whether a full synchronisation is in progress, 1 or 0",
                cache, c -> boolStat(c, "syncing"));
    }

    // ------------------------------------------------------------------
    // Locks
    // ------------------------------------------------------------------

    private void bindMutex(MeterRegistry registry) {
        num(registry, "spreader.mutex.acquired", "successful acquisitions in total",
                mutex, m -> mutexStat("acquired"));
        num(registry, "spreader.mutex.contended",
                "failed acquisitions in total, the lock being held by someone else",
                mutex, m -> mutexStat("contended"));
        num(registry, "spreader.mutex.contention.rate",
                "contention rate, 0 to 1; persistently above 0.5 means the lock should be "
                        + "split, or held for less time",
                mutex, m -> mutexStat("contentionRate"));
        num(registry, "spreader.mutex.acquire.timeouts",
                "acquisitions that timed out without the lock; persistently non-zero is a "
                        + "fault",
                mutex, m -> mutexStat("acquireTimeouts"));
        num(registry, "spreader.mutex.wait.avg.millis",
                "mean wait in milliseconds; more direct than the contention rate, since heavy "
                        + "contention with a few milliseconds' wait goes unnoticed",
                mutex, m -> mutexStat("avgWaitMillis"));
        num(registry, "spreader.mutex.held", "locks this process currently holds",
                mutex, m -> mutexStat("heldNow"));
        num(registry, "spreader.mutex.leaked",
                "acquired minus released; growing without falling is a leak, where something "
                        + "acquired the lock without a finally",
                mutex, m -> mutexStat("acquired") - mutexStat("released"));
        num(registry, "spreader.mutex.registered", "locks in the leader's register",
                mutex, m -> mutexStat("registered"));
    }

    // ------------------------------------------------------------------
    // Latches
    // ------------------------------------------------------------------

    private void bindLatch(MeterRegistry registry) {
        num(registry, "spreader.latch.declares", "latches declared in total",
                latch, x -> statOf(latch, "declares"));
        num(registry, "spreader.latch.countdowns", "count-downs in total",
                latch, x -> statOf(latch, "countDowns"));
        num(registry, "spreader.latch.awaits", "waits in total",
                latch, x -> statOf(latch, "awaits"));
        num(registry, "spreader.latch.satisfied", "waits that reached zero, in total",
                latch, x -> statOf(latch, "satisfied"));
        num(registry, "spreader.latch.await.timeouts",
                "waits that timed out with the latch still above zero; persistently non-zero "
                        + "is a fault, usually a party that never called countDown",
                latch, x -> statOf(latch, "awaitTimeouts"));
        num(registry, "spreader.latch.invalidations",
                "waits invalidated by a change of leader; non-zero means the leader is "
                        + "unsteady, and without a participantId on countDown the count cannot "
                        + "be recovered",
                latch, x -> statOf(latch, "invalidations"));
        num(registry, "spreader.latch.wait.avg.millis",
                "mean wait in milliseconds, counting only the waits that succeeded",
                latch, x -> statOf(latch, "avgWaitMillis"));
        num(registry, "spreader.latch.waiting",
                "waiters currently blocked; rising without falling means someone waits for ever",
                latch, x -> statOf(latch, "waitingNow"));
    }

    // ------------------------------------------------------------------
    // Barriers
    // ------------------------------------------------------------------

    private void bindBarrier(MeterRegistry registry) {
        num(registry, "spreader.barrier.awaits", "waits in total",
                barrier, x -> statOf(barrier, "awaits"));
        num(registry, "spreader.barrier.tripped", "calls released in total",
                barrier, x -> statOf(barrier, "tripped"));
        num(registry, "spreader.barrier.broken",
                "times the barrier was broken -- another party timed out, was interrupted, "
                        + "departed, or the leader changed; a different thing from timing out "
                        + "here",
                barrier, x -> statOf(barrier, "broken"));
        num(registry, "spreader.barrier.await.timeouts", "waits that timed out here",
                barrier, x -> statOf(barrier, "awaitTimeouts"));
        num(registry, "spreader.barrier.resets",
                "deliberate resets, discarding the current generation",
                barrier, x -> statOf(barrier, "resets"));
        num(registry, "spreader.barrier.wait.avg.millis",
                "mean wait in milliseconds; it reflects how slow the slowest party is, since "
                        + "everyone waits for all of them",
                barrier, x -> statOf(barrier, "avgWaitMillis"));
        num(registry, "spreader.barrier.waiting", "waiters currently blocked",
                barrier, x -> statOf(barrier, "waitingNow"));
    }

    // ------------------------------------------------------------------
    // Exchange points
    // ------------------------------------------------------------------

    private void bindExchanger(MeterRegistry registry) {
        num(registry, "spreader.exchanger.arrivals", "exchange calls in total",
                exchanger, x -> statOf(exchanger, "arrivals"));
        num(registry, "spreader.exchanger.exchanges", "calls that got an item back, in total",
                exchanger, x -> statOf(exchanger, "exchanges"));
        num(registry, "spreader.exchanger.pairings",
                "pairings made while this node was the leader; one pairing serves two "
                        + "exchanges, and a follower reports zero",
                exchanger, x -> statOf(exchanger, "pairings"));
        num(registry, "spreader.exchanger.timeouts",
                "calls that ran out of time with no partner. The usual cause is one side of "
                        + "the pairing never having been written",
                exchanger, x -> statOf(exchanger, "timeouts"));
        num(registry, "spreader.exchanger.failures",
                "calls that left empty-handed for a reason other than a timeout: interrupted, "
                        + "or the mechanism failed",
                exchanger, x -> statOf(exchanger, "failures"));
        num(registry, "spreader.exchanger.late",
                "exchanges completed after their deadline because the pairing beat the "
                        + "cancellation. Not an error, but a large number means the timeouts "
                        + "are set close to the real waiting time",
                exchanger, x -> statOf(exchanger, "lateExchanges"));
        num(registry, "spreader.exchanger.invalidations",
                "waits ended by a change of leader. Exchanges caught mid-pairing by one of "
                        + "these are the ones that lose items",
                exchanger, x -> statOf(exchanger, "invalidations"));
        num(registry, "spreader.exchanger.wait.avg.millis",
                "the mean wait of a successful exchange, which is how long the second party "
                        + "took to turn up",
                exchanger, x -> statOf(exchanger, "avgWaitMillis"));
        num(registry, "spreader.exchanger.waiting", "parties currently blocked",
                exchanger, x -> statOf(exchanger, "waitingNow"));
    }

    // ------------------------------------------------------------------
    // Semaphores
    // ------------------------------------------------------------------

    private void bindSemaphore(MeterRegistry registry) {
        num(registry, "spreader.semaphore.acquired", "permits acquired in total",
                semaphore, x -> statOf(semaphore, "acquired"));
        num(registry, "spreader.semaphore.local.blocked",
                "acquisitions blocked by this process's own quota; a larger local gate eases "
                        + "it",
                semaphore, x -> statOf(semaphore, "localBlocked"));
        num(registry, "spreader.semaphore.remote.denied",
                "acquisitions the local gate allowed but the leader refused; the whole cluster "
                        + "is under pressure, so a larger local gate will not help -- raise the "
                        + "total permits or reduce the load",
                semaphore, x -> statOf(semaphore, "remoteDenied"));
        num(registry, "spreader.semaphore.acquire.timeouts",
                "acquisitions that timed out without a permit; persistently non-zero means "
                        + "there are simply not enough permits",
                semaphore, x -> statOf(semaphore, "acquireTimeouts"));
        num(registry, "spreader.semaphore.released", "permits returned in total",
                semaphore, x -> statOf(semaphore, "released"));
        num(registry, "spreader.semaphore.stale.releases",
                "releases of permits that were not held; non-zero is usually a duplicate "
                        + "release, or a release after an epoch change had already invalidated "
                        + "the permit",
                semaphore, x -> statOf(semaphore, "staleReleases"));
        num(registry, "spreader.semaphore.held",
                "permits this process currently holds; rising without falling is a leak -- "
                        + "exhausted permits show up as everyone stalling, not as an error "
                        + "anywhere",
                semaphore, x -> statOf(semaphore, "heldNow"));
        num(registry, "spreader.semaphore.contention.rate", "contention rate, 0 to 1",
                semaphore, x -> statOf(semaphore, "contentionRate"));
        num(registry, "spreader.semaphore.wait.avg.millis", "mean wait in milliseconds",
                semaphore, x -> statOf(semaphore, "avgWaitMillis"));
    }

    // ------------------------------------------------------------------
    // RPC
    // ------------------------------------------------------------------

    private void bindRpc(MeterRegistry registry) {
        num(registry, "spreader.rpc.calls", "calls made in total",
                rpc, x -> statOf(rpc, "calls"));
        num(registry, "spreader.rpc.answered", "calls answered in total",
                rpc, x -> statOf(rpc, "answered"));
        num(registry, "spreader.rpc.call.failures", "calls that failed, in total",
                rpc, x -> statOf(rpc, "callFailures"));
        num(registry, "spreader.rpc.call.timeouts",
                "times the peer said nothing; read it apart from remote.errors -- this one "
                        + "points at the network and the peer's load",
                rpc, x -> statOf(rpc, "callTimeouts"));
        num(registry, "spreader.rpc.remote.errors",
                "times the peer answered with an explicit exception; this one points at the "
                        + "application code",
                rpc, x -> statOf(rpc, "remoteErrors"));
        num(registry, "spreader.rpc.latency.avg.millis",
                "mean round trip in milliseconds, counting only answered calls; a timed-out "
                        + "call always equals the configured timeout, and mixing those in would "
                        + "make the mean a function of the configuration",
                rpc, x -> statOf(rpc, "avgLatencyMillis"));
        num(registry, "spreader.rpc.failure.rate", "failure rate, 0 to 1",
                rpc, x -> statOf(rpc, "failureRate"));
        num(registry, "spreader.rpc.inflight", "requests in flight",
                rpc, x -> statOf(rpc, "inflight"));
        num(registry, "spreader.rpc.served",
                "requests this node completed as the serving side",
                rpc, x -> statOf(rpc, "served"));
        num(registry, "spreader.rpc.rejected",
                "requests refused because the inbound queue was full; non-zero means this node "
                        + "is already dropping requests",
                rpc, x -> statOf(rpc, "rejected"));
        num(registry, "spreader.rpc.queued",
                "requests waiting in the inbound queue; a rise here precedes overload",
                rpc, x -> statOf(rpc, "queued"));
    }

    // ------------------------------------------------------------------
    // Task dispatch
    // ------------------------------------------------------------------

    private void bindPool(MeterRegistry registry) {
        // ThreadPoolExecutor's three first, under the same names, so operations recognises
        // them at a glance
        num(registry, "spreader.pool.active", "threads running tasks, both pools together",
                pool, p -> p.poolStats().activeCount());
        num(registry, "spreader.pool.completed",
                "tasks finished, both pools together, failures included",
                pool, p -> p.poolStats().completedTaskCount());
        num(registry, "spreader.pool.queued", "tasks not yet started, both pools together",
                pool, p -> p.poolStats().queuedTaskCount());

        // The local pool: ForkJoin, running recursive tasks this process started
        num(registry, "spreader.pool.local.active", "active threads in the local pool",
                pool, p -> p.poolStats().localActive());
        num(registry, "spreader.pool.local.running",
                "threads in the local pool not blocked in join",
                pool, p -> p.poolStats().localRunning());
        num(registry, "spreader.pool.local.queued", "tasks queued in the local pool",
                pool, p -> p.poolStats().localQueued());
        num(registry, "spreader.pool.local.submitted",
                "tasks submitted from outside the pool and not yet taken up",
                pool, p -> p.poolStats().localSubmitted());
        num(registry, "spreader.pool.local.completed", "tasks finished in the local pool",
                pool, p -> p.poolStats().localCompleted());
        num(registry, "spreader.pool.local.failed", "tasks that ended by throwing",
                pool, p -> p.poolStats().localFailed());
        num(registry, "spreader.pool.local.size", "threads currently in the local pool",
                pool, p -> p.poolStats().localPoolSize());
        num(registry, "spreader.pool.local.parallelism",
                "the local pool's target parallelism",
                pool, p -> p.poolStats().localParallelism());
        num(registry, "spreader.pool.local.steals",
                "work-stealing count; unique to ForkJoin, and the larger the better -- idle "
                        + "threads really are helping",
                pool, p -> p.poolStats().localSteals());
        num(registry, "spreader.pool.local.blocked.rate",
                "blocked rate, 0 to 1: how many active threads are stuck in join. Near 1 "
                        + "means the recursion divides too deeply, and a smaller max-depth is in "
                        + "fact faster",
                pool, p -> p.poolStats().localBlockedRate());
        num(registry, "spreader.pool.failure.rate", "task failure rate, 0 to 1",
                pool, p -> p.poolStats().failureRate());

        // The inbound pool: a ThreadPoolExecutor handling tasks other processes send
        num(registry, "spreader.pool.inbound.active", "active threads in the inbound pool",
                pool, p -> p.poolStats().inboundActive());
        num(registry, "spreader.pool.inbound.queued",
                "tasks queued in the inbound pool; persistently high means others are pressing "
                        + "too hard on this node",
                pool, p -> p.poolStats().inboundQueued());
        num(registry, "spreader.pool.inbound.completed", "tasks finished in the inbound pool",
                pool, p -> p.poolStats().inboundCompleted());
        num(registry, "spreader.pool.inbound.size", "threads currently in the inbound pool",
                pool, p -> p.poolStats().inboundPoolSize());

        // The dispatch path
        num(registry, "spreader.pool.local.runs", "tasks run directly in this process",
                pool, p -> poolStat("localRuns"));
        num(registry, "spreader.pool.remote.dispatches",
                "tasks successfully sent to another process",
                pool, p -> poolStat("remoteDispatches"));
        num(registry, "spreader.pool.remote.fallbacks",
                "tasks sent out that returned no result and were recomputed locally; "
                        + "persistently non-zero means the peer has a problem",
                pool, p -> poolStat("remoteFallbacks"));
        num(registry, "spreader.pool.remote.ratio",
                "the proportion dispatched outward, 0 to 1. Local and remote throughput differ "
                        + "by a factor of 25, so this ratio decides overall throughput outright",
                pool, p -> poolStat("remoteRatio"));
        num(registry, "spreader.pool.depth.capped",
                "times the recursion hit its depth limit and ran synchronously",
                pool, p -> poolStat("depthCapped"));
        num(registry, "spreader.pool.pending.requests", "remote tasks in flight",
                pool, p -> poolStat("pendingRequests"));
        num(registry, "spreader.pool.peers",
                "other replicas of the same application available for dispatch",
                pool, PoolService::peerCount);
    }

    // ------------------------------------------------------------------
    // Scheduled tasks
    // ------------------------------------------------------------------

    private void bindScheduled(MeterRegistry registry) {
        // The global totals, for alerting on
        num(registry, "spreader.scheduled.executed",
                "rounds this instance actually executed, across every task",
                scheduled, s -> sumScheduled("executed"));
        num(registry, "spreader.scheduled.skipped",
                "rounds skipped for want of the lock. Its ratio to executed should be roughly "
                        + "\"instances - 1 : 1\", and a clear departure means leadership is "
                        + "unsteady",
                scheduled, s -> sumScheduled("skipped"));
        num(registry, "spreader.scheduled.failed", "rounds that threw while executing",
                scheduled, s -> sumScheduled("failed"));
        num(registry, "spreader.scheduled.tasks", "scheduled tasks that have run at least once",
                scheduled, s -> s.snapshot().size());
    }

    /**
     * Registers one gauge per task name, for troubleshooting.
     *
     * <p>Registration has to be <b>deferred</b>: task names appear at runtime, registered on
     * the first firing, and at startup there are none. The auto-configuration calls this
     * periodically, in the same manner as
     * {@link SpreaderMeterBinder#bindKnownChannels()}.
     */
    public void bindKnownTasks(MeterRegistry registry) {
        if (scheduled == null || registry == null) {
            return;
        }
        for (String task : scheduled.snapshot().keySet()) {
            if (!registeredTasks.add(task)) {
                continue;
            }
            Tags tags = Tags.of("task", task);
            taskGauge(registry, "spreader.scheduled.task.executed", tags,
                    "rounds this task actually executed", task, "executed");
            taskGauge(registry, "spreader.scheduled.task.skipped", tags,
                    "rounds this task skipped for want of the lock", task, "skipped");
            taskGauge(registry, "spreader.scheduled.task.failed", tags,
                    "rounds this task threw", task, "failed");
            taskGauge(registry, "spreader.scheduled.task.avg.elapsed.millis", tags,
                    "this task's mean execution time; a sudden rise usually means something "
                            + "downstream has slowed", task, "avgElapsedMs");
            taskGauge(registry, "spreader.scheduled.task.last.executed.at", tags,
                    "when it last executed, as a millisecond timestamp; growth stopping means "
                            + "the task is no longer running", task, "lastExecutedAtMs");
        }
    }

    private void taskGauge(MeterRegistry registry, String name, Tags tags,
                           String desc, String task, String key) {
        Gauge.builder(name, task, t -> {
                    Map<String, Object> m = scheduled.snapshot().get(t);
                    return m == null ? 0d : toDouble(m.get(key));
                })
                .tags(tags)
                .description(desc)
                .register(registry);
    }

    private double sumScheduled(String key) {
        return scheduled.snapshot().values().stream()
                .mapToDouble(m -> toDouble(m.get(key)))
                .sum();
    }

    // ------------------------------------------------------------------

    private <T> void num(MeterRegistry registry, String name, String desc,
                         T source, ToDoubleFunction<T> reader) {
        Gauge.builder(name, source, safe(reader))
                .tags(Tags.empty())
                .description(desc)
                .register(registry);
    }

    /**
     * Wraps a reader so that exceptions cannot escape.
     *
     * <p>Metric reads run on the collection thread, and letting one throw makes <b>the whole of
     * /actuator/prometheus return 500</b> -- one broken metric takes every metric down with it,
     * and precisely when the system is already unwell.
     */
    private <T> ToDoubleFunction<T> safe(ToDoubleFunction<T> reader) {
        return t -> {
            try {
                return reader.applyAsDouble(t);
            } catch (RuntimeException e) {
                return 0d;
            }
        };
    }

    private double stat(CacheService c, String key) {
        return toDouble(c.stats().get(key));
    }

    private double boolStat(CacheService c, String key) {
        return Boolean.TRUE.equals(c.stats().get(key)) ? 1d : 0d;
    }

    /** Reads one number through the common interface. Every component's stats() has the same
     *  shape, so this one helper suffices. */
    private double statOf(MultiProcessingService service, String key) {
        return toDouble(service.stats().get(key));
    }

    private double mutexStat(String key) {
        return toDouble(mutex.stats().get(key));
    }

    private double poolStat(String key) {
        return toDouble(pool.stats().get(key));
    }

    private static double toDouble(Object v) {
        return v instanceof Number n ? n.doubleValue() : 0d;
    }
}
