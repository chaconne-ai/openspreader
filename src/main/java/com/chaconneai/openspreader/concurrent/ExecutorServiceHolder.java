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
package com.chaconneai.openspreader.concurrent;

import com.chaconneai.spreader.metrics.BufferMetrics;
import com.chaconneai.spreader.util.ExecutorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

/**
 * <b>The single place every thread pool is maintained.</b> No component creates one of its
 * own; each takes what it needs from here, by purpose.
 *
 * <h2>Taken by purpose, not by pool</h2>
 * The only public surface is a set of {@code forXxx()} accessors, and <b>a caller does not
 * know whether the pool it received is shared</b>. That is deliberate: sharing is an
 * implementation detail of this class, so changing it later touches this class alone.
 *
 * <h2>Two separations that are not negotiable</h2>
 * <b>Scheduling and execution cannot share a pool.</b>
 * {@code ScheduledThreadPoolExecutor} hard-codes {@code DelayedWorkQueue}, whose capacity is
 * unbounded and cannot be changed. Handling inbound messages on it would turn a bounded queue
 * back into an unbounded one, and back-pressure would be gone.
 *
 * <p><b>{@link #forRenewal()} and {@link #forMaintenance()} cannot share one either.</b>
 * Renewal keeps lock and semaphore leases alive and takes microseconds; maintenance sweeps
 * expiry and cache keys and can hold a thread for hundreds of milliseconds. Queued behind a
 * sweep, a renewal misses its deadline, the leader reclaims the lock, and <b>the holder knows
 * nothing about it and is still inside the critical section</b>. That is among the gravest
 * failures this library can have.
 *
 * <h2>Rejection policy is why the inbound channels are separate</h2>
 * {@link #forPoolInbound()} and {@link #forCacheInbound()} discard when full, so neither can
 * starve the other. {@link #forRpcInbound()} and {@link #forMapReduceInbound()} reject
 * instead, because the caller needs the overload answer to take its fallback, and dropping one
 * intermediate result would make an aggregate silently wrong. {@link #forCacheSync()} and the
 * three notify channels are single-threaded: they send network messages and block, so sharing
 * one thread would let a connect timeout on one side stall the others.
 * {@link #forRecursiveTasks()} is ForkJoin, because a task that joins subtasks inside
 * {@code compute()} needs work-stealing to avoid exhausting itself.
 *
 * <h2>The lifecycle belongs to the container</h2>
 * Pools are closed in {@link #destroy()}, but created <b>on first use</b> rather than in
 * {@link #afterPropertiesSet()}. Components can be switched off individually, and creating
 * everything at startup would leave a switched-off component holding a resident pool nothing
 * submits to. So "the pool exists" means "something really uses it", and a jstack reflects
 * which features are actually enabled.
 *
 * <p>A service <b>must never</b> close a pool from here: one service shutting down should not
 * take another's threads with it.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class ExecutorServiceHolder implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(ExecutorServiceHolder.class);

    /** How long each pool is waited for during shutdown. */
    private static final long SHUTDOWN_TIMEOUT_MS = 1_000L;

    // The queue capacities carry over from what each service used when it created its own;
    // they are not new values
    private static final int POOL_INBOUND_QUEUE = 4096;
    private static final int CACHE_INBOUND_QUEUE = 8192;
    private static final int CACHE_SYNC_QUEUE = 256;
    private static final int NOTIFIER_QUEUE = 1024;

    /**
     * The aggregation inbound queue's capacity.
     *
     * <p>Far smaller than the other two inbound queues, because <b>every entry here is
     * large</b>: one shuffle message carries an entire shard's intermediate results for one
     * reducer. A queue of a few thousand of them backs up into hundreds of megabytes.
     */
    private static final int MAPREDUCE_INBOUND_QUEUE = 1024;

    /**
     * How many threads renewal uses.
     *
     * <p>Two is enough: only locks and semaphores renew in the whole process, one task each,
     * and both take microseconds. Two leaves a little headroom, so that one stalling
     * occasionally does not affect the other.
     */
    private static final int RENEWAL_THREADS = 2;

    /**
     * How many threads maintenance uses.
     *
     * <p>These tasks can be slow -- a cache sweep over a few hundred thousand keys -- so some
     * parallelism is needed, or latch and barrier idle cleanup would queue behind cache
     * maintenance throughout.
     */
    private static final int MAINTENANCE_THREADS = 2;

    private final int poolThreads;
    private final int cacheThreads;
    private final int rpcThreads;
    private final int rpcQueueCapacity;
    private final int mapReduceThreads;

    /**
     * The pools already created, keyed by thread-name prefix.
     *
     * <p>A map rather than a set of fields, so that they can be <b>created on demand</b>; see
     * {@link #pool}. Shutdown then walks this alone -- closing exactly what was created, with
     * no null checks.
     */
    private final Map<String, ExecutorService> pools = new ConcurrentHashMap<>();

    /** After shutdown no new pool may be created, or {@code computeIfAbsent} would build one
     *  again. */
    private volatile boolean destroyed;

    /**
     * @param poolThreads      task dispatch's inbound thread count, and also the recursive
     *                         tasks' parallelism
     * @param cacheThreads     the cache's inbound thread count
     * @param rpcThreads       RPC's inbound thread count
     * @param rpcQueueCapacity RPC's inbound queue capacity. It is given separately because
     *                         RPC's downstream may be a long task, and this value decides how
     *                         much backs up before overload answers begin
     */
    public ExecutorServiceHolder(int poolThreads, int cacheThreads,
                                 int rpcThreads, int rpcQueueCapacity) {
        // Aggregation runs user code too, so matching task dispatch's parallelism is a
        // reasonable starting point
        this(poolThreads, cacheThreads, rpcThreads, rpcQueueCapacity, poolThreads);
    }

    /**
     * @param mapReduceThreads distributed aggregation's inbound thread count. It runs <b>the
     *                         user's own {@code map} and {@code reduce}</b>, whose duration is
     *                         unpredictable, so this number decides outright how many shards
     *                         one node computes at a time
     */
    public ExecutorServiceHolder(int poolThreads, int cacheThreads, int rpcThreads,
                                 int rpcQueueCapacity, int mapReduceThreads) {
        this.poolThreads = Math.max(2, poolThreads);
        this.cacheThreads = Math.max(1, cacheThreads);
        this.rpcThreads = Math.max(2, rpcThreads);
        this.rpcQueueCapacity = Math.max(1, rpcQueueCapacity);
        // The floor is 1 rather than 2: the other pools need 2 because they may wait on
        // themselves -- a recursive task joins subtasks inside compute() -- and a single thread
        // would deadlock. Aggregation does not nest: a map never triggers another map, so one
        // thread is a legitimate configuration, and measuring scalability needs it -- the
        // parallelism must come from the node count rather than from the pool
        this.mapReduceThreads = Math.max(1, mapReduceThreads);
    }

    /** For use without a Spring container: plain {@code spreader} usage, and tests. It is
     *  ready as soon as it is built. */
    public static ExecutorServiceHolder standalone(int poolThreads, int cacheThreads,
                                                   int rpcThreads, int rpcQueueCapacity) {
        return new ExecutorServiceHolder(poolThreads, cacheThreads, rpcThreads, rpcQueueCapacity);
    }

    /** A small configuration suited to tests. */
    public static ExecutorServiceHolder standalone() {
        return standalone(4, 2, 4, NOTIFIER_QUEUE);
    }

    /**
     * <b>No pool is created here</b> -- each is created on first use; see {@link #pool}.
     *
     * <p>Components can be switched off individually
     * ({@code spring.spreader.multiprocessing.cache.enabled=false} and the like), and creating
     * everything at startup would leave a switched-off component holding a thread pool of
     * <b>resident threads</b> nothing would ever submit to.
     *
     * <p>Created on demand, "the pool exists" becomes equivalent to "some component really uses
     * it", and the thread count in a jstack reflects which features are actually enabled.
     */
    @Override
    public void afterPropertiesSet() {
        log.info("Thread pools ready, created on demand: task dispatch={}, cache={}, RPC={} "
                + "with a queue of {}", poolThreads, cacheThreads, rpcThreads, rpcQueueCapacity);
    }

    /**
     * The queue levels of every thread pool created so far.
     *
     * <h2>Why this is exposed</h2>
     * These pools are all <b>bounded</b>, and a full one either discards or rejects -- both of
     * which make messages disappear. Discarding is the more dangerous:
     * {@code DiscardPolicy}'s implementation is an empty method, so tasks vanish without a
     * sound while latency looks normal, the error rate is zero, and everything appears fine
     * except that a batch of messages is missing.
     *
     * <p>The queue level is the <b>only</b> place this can be seen coming: sitting high for a
     * long time means one traffic spike away from discarding, and a non-zero {@code dropped}
     * means it has already happened.
     *
     * <p>Only pools <b>already created</b> are reported -- they are built lazily, and a
     * component nobody uses should not occupy a line on a dashboard.
     */
    public List<BufferMetrics> bufferMetrics() {
        List<BufferMetrics> out = new ArrayList<>(pools.size());
        pools.forEach((name, executor) -> {
            int capacity = ExecutorUtils.queueCapacity(executor);
            if (capacity < 0) {
                // A scheduler and the like offer no queue capacity, so skip them -- reporting -1
            // would only make a dashboard uglier
                return;
            }
            long rejected = ExecutorUtils.rejectedCount(executor);
            long completed = executor instanceof ThreadPoolExecutor p
                    ? p.getCompletedTaskCount() : 0L;
            out.add(new BufferMetrics(name,
                    Math.max(0, ExecutorUtils.queueSize(executor)),
                    capacity,
                    Math.max(0L, rejected),
                    completed));
        });
        out.sort(Comparator.comparing(BufferMetrics::name));
        return out;
    }

    /**
     * Takes a pool on demand: created if absent, reused if present.
     *
     * <p>{@code computeIfAbsent} guarantees one key is built once, even when several components
     * ask at the same moment.
     */
    @SuppressWarnings("unchecked")
    private <T extends ExecutorService> T pool(String name, Supplier<T> factory) {
        if (destroyed) {
            throw new IllegalStateException("the thread pools are already closed, so " + name
                    + " cannot be taken. This usually means a component is still submitting "
                    + "tasks after the container was destroyed");
        }
        return (T) pools.computeIfAbsent(name, k -> factory.get());
    }

    // ------------------------------------------------------------------
    // Periodic tasks
    // ------------------------------------------------------------------

    /**
     * For renewal alone.
     *
     * <p><b>Only tasks that are sensitive to delay and quick in themselves belong here.</b>
     * Putting a slow task in leaves the leases' reliability to that task's luck.
     */
    public ScheduledExecutorService forRenewal() {
        return pool("spreader-renew",
                () -> ExecutorUtils.scheduler("spreader-renew", RENEWAL_THREADS));
    }

    /** Cleanup, maintenance, reporting -- work where being a little late does not matter. */
    public ScheduledExecutorService forMaintenance() {
        return pool("spreader-maintain",
                () -> ExecutorUtils.scheduler("spreader-maintain", MAINTENANCE_THREADS));
    }

    // ------------------------------------------------------------------
    // Execution
    // ------------------------------------------------------------------

    /**
     * Handles method calls and recursive tasks other nodes send.
     *
     * <p>A full queue discards: what feeds it is spreader's dispatch thread, and pushing back
     * would drag the whole dispatch path down -- a slow dispatch thread makes heartbeats slow,
     * and this node would be judged unreachable. A discarded request times out at the sender,
     * which recomputes it locally.
     */
    public ExecutorService forPoolInbound() {
        return pool("pool-inbound",
                () -> ExecutorUtils.discarding("pool-inbound", poolThreads, POOL_INBOUND_QUEUE));
    }

    /** Handles the cache's inbound messages. A full queue discards, for the reason given in
     *  {@link #forPoolInbound()}. */
    public ExecutorService forCacheInbound() {
        return pool("cache-inbound",
                () -> ExecutorUtils.discarding("cache-inbound", cacheThreads, CACHE_INBOUND_QUEUE));
    }

    /**
     * Handles RPC requests other nodes send.
     *
     * <p>A full queue <b>rejects</b> rather than discards: rejecting gets an overload answer to
     * the caller at once so it can take its fallback, whereas discarding leaves it waiting for
     * the timeout -- and sending new requests all the while, piling up further.
     */
    public ExecutorService forRpcInbound() {
        return pool("rpc-inbound",
                () -> ExecutorUtils.rejecting("rpc-inbound", rpcThreads, rpcQueueCapacity));
    }

    /**
     * Runs distributed aggregation's map and reduce.
     *
     * <p>A full queue <b>rejects</b>, as RPC's does and unlike the other two inbound pools,
     * because discarding here would <b>make the result wrong</b>: a dropped cache replication
     * is one replication missed and the next access makes it good, whereas a dropped
     * intermediate result leaves the aggregate <b>short by that much</b> -- no error, no
     * timeout, quietly incorrect. Rejecting gets a failure to the sender at once and fails the
     * whole job, and <b>an exception is far better than a wrong answer</b>.
     *
     * <p>Why it is not merged into {@link #forRpcInbound()}, whose rejection policy is the
     * same: what runs here is <b>user code</b> of entirely unpredictable duration -- scanning a
     * large body of text may take seconds. Merged, one slow job could occupy every RPC inbound
     * thread, and a burst of RPC traffic would queue the aggregation tasks behind it. This is
     * precisely the reason {@link #forPoolInbound()} and {@link #forCacheInbound()} remain
     * separate.
     */
    public ExecutorService forMapReduceInbound() {
        return pool("mapreduce-inbound", () -> ExecutorUtils.rejecting(
                "mapreduce-inbound", mapReduceThreads, MAPREDUCE_INBOUND_QUEUE));
    }

    /** The cache's full synchronisation. Single-threaded, to keep the order. */
    public ExecutorService forCacheSync() {
        return pool("cache-sync",
                () -> ExecutorUtils.singleThread("cache-sync", CACHE_SYNC_QUEUE));
    }

    /** Latch arrival notifications. */
    public ExecutorService forLatchNotify() {
        return pool("latch-notify",
                () -> ExecutorUtils.singleThread("latch-notify", NOTIFIER_QUEUE));
    }

    /** Barrier arrival notifications. */
    public ExecutorService forBarrierNotify() {
        return pool("barrier-notify",
                () -> ExecutorUtils.singleThread("barrier-notify", NOTIFIER_QUEUE));
    }

    /**
     * Runs DAG nodes marked as local, which is to say nodes whose work is a call to something
     * outside this application.
     *
     * <p>Its own pool rather than the task-dispatch one, because these are <b>blocking I/O</b>
     * by definition: an HTTP call to somebody else's service, holding a thread for as long as
     * they take to answer. Sharing task dispatch's pool would let one slow third party stall
     * the cluster's own work.
     *
     * <p>Sized off task dispatch's parallelism for want of a better signal, and generously:
     * threads here are waiting on the network, not computing.
     *
     * <p><b>Rejecting, not discarding.</b> A discarded node would be a node the run waits for
     * for ever, because the completion it is waiting on would simply never arrive. Rejecting
     * turns a full queue into a failed node with a cause, which is the same choice the
     * aggregation component makes and for the same reason.
     */
    public ExecutorService forDagLocal() {
        return pool("dag-local",
                () -> ExecutorUtils.rejecting("dag-local", Math.max(4, poolThreads * 2),
                        POOL_INBOUND_QUEUE));
    }

    /** Exchanger pairing notifications. */
    public ExecutorService forExchangerNotify() {
        return pool("exchanger-notify",
                () -> ExecutorUtils.singleThread("exchanger-notify", NOTIFIER_QUEUE));
    }

    /**
     * Runs recursive tasks locally.
     *
     * <p>It has to be a {@link ForkJoinPool}: a recursive task waits for its subtasks inside
     * {@code compute()}, and an ordinary thread pool meeting worker threads that wait on each
     * other exhausts itself, whereas work-stealing helps with other tasks while joining and
     * never seizes up.
     */
    public ForkJoinPool forRecursiveTasks() {
        return pool("recursive-tasks", () -> new ForkJoinPool(poolThreads));
    }

    /**
     * Closes the pools <b>that were created</b>. One no component ever asked for does not
     * exist, and so needs no closing.
     *
     * <p>The order matters: <b>stop the two schedulers first</b>. Still running, they keep
     * submitting new work to the pools below -- and closing the execution pools first would
     * have those periodic tasks throw RejectedExecutionException all the way down, filling the
     * shutdown log with noise that may hide a real problem.
     */
    @Override
    public void destroy() {
        if (destroyed) {
            return;
        }
        destroyed = true;
        shutdown("spreader-renew");
        shutdown("spreader-maintain");
        pools.keySet().forEach(this::shutdown);
        log.info("Closed {} thread pool(s)", pools.size());
        pools.clear();
    }

    private void shutdown(String name) {
        ExecutorService executor = pools.get(name);
        if (executor != null) {
            ExecutorUtils.shutdownGracefully(executor, SHUTDOWN_TIMEOUT_MS);
        }
    }

    @Override
    public String toString() {
        return "ExecutorServiceHolder{created=" + pools.size() + ", destroyed=" + destroyed + '}';
    }
}
