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
package com.chaconneai.openspreader;

import com.chaconneai.openspreader.cache.EvictionPolicy;
import com.chaconneai.openspreader.cluster.ApplicationClusterProperties;
import com.chaconneai.openspreader.scheduling.MultiProcessingScheduled;
import com.chaconneai.openspreader.serialization.SerializationType;
import com.chaconneai.spreader.transport.TransportProvider;

import java.util.concurrent.TimeUnit;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The multi-processing toolkit's configuration, under the prefix
 * {@code spring.spreader.multiprocessing}.
 *
 * <p>It covers only the tools built on top of the cluster: locks, semaphores, latches,
 * barriers, exchange points, scheduled-task exclusion, the process pool, the cluster
 * cache and the DAG engine. <b>The cluster
 * itself</b> -- its name, port, discovery, transport and timeouts -- lives under
 * {@code spring.spreader.*}; see {@link ApplicationClusterProperties}.
 *
 * <pre>
 * spring.spreader.name=my-cluster                        # the cluster
 * spring.spreader.ip-addresses=10.0.0.1,10.0.0.2         # the cluster
 * spring.spreader.multiprocessing.cache.max-keys=500000  # the toolkit
 * spring.spreader.multiprocessing.mutex.lease-ms=15000   # the toolkit
 * </pre>
 *
 * <p>Each tool switches off on its own ({@code xxx.enabled=false}), after which not even its
 * components are created.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@Data
@ConfigurationProperties(prefix = "spring.spreader.multiprocessing")
public class MultiProcessingProperties {

    /**
     * How objects become bytes; Java's native serialisation by default.
     *
     * <p>Task dispatch ({@code @MultiProcessingCall}), RPC and the cluster cache <b>share this
     * one setting</b>, since they are solving the same problem.
     *
     * <p>Switching to KRYO needs the corresponding library; see {@link SerializationType}.
     * <b>The whole cluster must be configured the same way</b>, or neither side can decode the
     * other's bytes.
     */
    private SerializationType serialization = SerializationType.JDK;

    private Mutex mutex = new Mutex();

    private Semaphore semaphore = new Semaphore();

    private Scheduling scheduling = new Scheduling();

    private Pooling pooling = new Pooling();

    private Cache cache = new Cache();

    private Latch latch = new Latch();

    private Barrier barrier = new Barrier();

    private Exchanger exchanger = new Exchanger();

    private Dag dag = new Dag();

    private Rpc rpc = new Rpc();

    private Event event = new Event();

    private Aggregation aggregation = new Aggregation();

    /**
     * The configuration for remote calls across applications ({@code @RpcClient}).
     *
     * <p>Most parameters are set per interface on the annotation -- timeouts, retries,
     * fallbacks -- and only those shared by the whole process live here. Serialisation uses the
     * global {@code spring.spreader.multiprocessing.serialization} and is not configured
     * separately: the calling and serving sides must agree, and a separate setting would only
     * add one more chance for them to disagree.
     */
    @Data
    public static class Rpc {

        /** Whether it is enabled. Switched off, {@code @RpcClient} interfaces are not
         *  registered and injecting one fails with no such bean. */
        private boolean enabled = true;

        /**
         * Which transport implementation is expected: NIO (the default), NETTY, MINA or
         * GRIZZLY.
         *
         * <p><b>One thing first</b>: RPC has no connections of its own and reuses the ones the
         * cluster <b>has already built</b> -- the same ports and connections as the gossip
         * protocol, the locks and the cache, differing only in channel name. So what really
         * decides the implementation is
         * {@code spring.spreader.advanced.transport-provider}.
         *
         * <p>What is configured here is "the implementation RPC expects", and its purpose is
         * <b>validation</b>: where it disagrees with what the cluster actually uses, startup
         * logs a warning naming the one in effect. That is better than letting someone believe
         * changing this changed the implementation -- a misunderstanding that would surface
         * only when the load-test figures failed to add up.
         *
         * <p>Blank, the default, validates nothing and follows the cluster. Giving RPC
         * connections of its own would be a change of another magnitude -- separate ports,
         * separate address discovery -- and is not supported.
         */
        private TransportProvider transportProvider;

        /**
         * How many threads handle inbound requests.
         *
         * <p>The serving side executes the method calls others send on these. Where the method
         * itself is slow, raise it, or requests queue as soon as concurrency rises -- which
         * shows up as callers timing out en masse while the serving side's CPU sits low.
         */
        private int inboundThreads = 8;

        /**
         * Which serialisation RPC uses. <b>Blank, the default, follows the global
         * {@code spring.spreader.multiprocessing.serialization}.</b>
         *
         * <p>A case for setting it separately: heavy RPC traffic with DTOs that all have
         * no-argument constructors, where KRYO for RPC buys throughput while the rest stays on
         * JDK for convenience.
         *
         * <p><b>Every node in the cluster must agree on this</b>, or neither side can decode
         * the other's bytes.
         */
        private SerializationType serialization;

        /**
         * Each attempt's timeout, in {@link #timeUnit}; 60 seconds by default.
         *
         * <p>An interface's {@code @RpcClient(timeout = ...)} overrides it. Setting it once
         * globally and adjusting the odd interface beats writing it on every one -- ten copies
         * mean ten places to change, and missing one leaves a timeout unlike the rest, an
         * inconsistency usually found only when production breaks.
         *
         * <p>0 or negative waits indefinitely.
         */
        private long timeout = 60L;

        /** How many further attempts follow a failure; 3 by default. An interface's
         *  {@code maxRetries} overrides it. */
        private int maxRetries = 3;

        /** How long to wait between attempts, in {@link #timeUnit}; one second by default. 0
         *  retries at once. */
        private int retryInterval = 1;

        /** The unit of {@link #timeout} and {@link #retryInterval}; seconds by default. */
        private TimeUnit timeUnit = TimeUnit.SECONDS;

        /**
         * How many requests a caller may have in flight at once. <b>0, the default, means no
         * limit.</b>
         *
         * <p>An interface's {@code @RpcClient(maxConcurrent = ...)} overrides it.
         *
         * <p>What is limited is <b>concurrency</b> rather than calls per second, because what
         * must be held back is "slowing downstream piling in-flight requests up": when
         * downstream goes from 10ms to 10s, holding the rate steady multiplies the in-flight
         * count a thousandfold. Capping it makes this process's call rate follow downstream
         * automatically -- that is back-pressure.
         *
         * <p>Long tasks, of seconds to minutes, most deserve it: they occupy a slot for longest
         * and pile up most readily. What to set follows downstream's capacity; start from
         * "downstream instances times concurrency per instance".
         */
        private int maxConcurrent = 0;

        /**
         * How long to wait for a slot once the in-flight limit is reached, in
         * {@link #timeUnit}. <b>0, the default, does not wait.</b>
         *
         * <p>Not waiting is failing fast, throwing {@code RpcOverloadException}, which suits a
         * call with a fallback path. A positive value queues briefly and absorbs a short
         * traffic spike.
         *
         * <p>Do not set it large: queueing is piling up too, only inside this process.
         */
        private long acquireTimeout = 0L;

        /**
         * The serving side's inbound queue capacity; 1000 by default.
         *
         * <p><b>It must not be unbounded</b>: an unbounded queue hides the overload and shows
         * it as "every caller times out", and a caller that times out usually retries, so the
         * queue grows faster still, until it reaches OOM -- with nothing anywhere having
         * reported an error.
         *
         * <p>Bounded, a full queue answers "overloaded" at once and the caller knows to
         * degrade. A refusal must be quick; a slow refusal is much the same as none.
         */
        private int inboundQueueCapacity = 1_000;
    }

    /**
     * The cross-process latch's configuration.
     *
     * <p>The leader adjudicates, as with the lock. Waiting <b>relies entirely on the leader's
     * push</b> and does not poll periodically -- with many waiters, polling is pure wasted
     * traffic. So there is no polling-interval parameter here, and the backstop for a lost push
     * is the timeout the caller passes to {@code await}.
     */
    @Data
    public static class Latch {

        /** Whether it is enabled. */
        private boolean enabled = false;

        /** How long one request waits for the leader's reply, in milliseconds. The total wait is
         *  controlled by the caller's await timeout. */
        private long requestTimeoutMs = 2_000L;

        /**
         * How long a latch record may sit untouched before it is reclaimed, in milliseconds.
         *
         * <p>A latch is single-use and nobody attends to it once it is finished. Without
         * reclamation the leader's memory only grows. It must exceed the longest interval from
         * declaration to the last countDown, or a latch would be swept away mid-use.
         */
        private long idleTimeoutMs = 300_000L;
    }

    /**
     * Carries Spring application events between processes.
     *
     * <h2>Not one line of application code changes</h2>
     * Switched on, an event from {@code publisher.publishEvent(...)} is broadcast to other
     * nodes, where {@code @EventListener} fires as though the event were local. Publishing and
     * receiving remain standard Spring on both sides.
     *
     * <p>Switched off, the bridge bean does not exist at all and events keep Spring's original
     * in-JVM semantics.
     *
     * <h2>Two things to settle before using it</h2>
     * <ul>
     *   <li><b>Events must be serialisable</b>, since they travel the network. An event with a
     *       non-serialisable field is not broadcast and leaves a warning in the log; it does not
     *       fail the application</li>
     *   <li><b>Every node receives it</b> -- logic that once handled something once per process
     *       becomes once in every process. Where something must happen once, guard the listener
     *       with an {@code applicationMutex}</li>
     * </ul>
     */
    @Data
    public static class Event {

        /** Whether it is enabled; off by default -- switching it on changes how far events
         *  travel, and that change of meaning must be explicit. */
        private boolean enabled = false;

        /**
         * How far a broadcast reaches. {@code false} by default, meaning <b>only other
         * instances of the same application</b>.
         *
         * <p>true sends to every application in the cluster, which is usually not what is
         * wanted -- an order service's domain event reaching a reporting service that most
         * likely has no such class only fills the log with decoding failures.
         */
        private boolean clusterWide = false;
    }

    /** The cross-process barrier's configuration; every entry means what {@link Latch}'s
     *  does. */
    @Data
    public static class Barrier {

        /** Whether it is enabled. */
        private boolean enabled = false;

        /** How long one request waits for the leader's reply, in milliseconds. */
        private long requestTimeoutMs = 2_000L;

        /**
         * How long a barrier record may sit untouched before it is reclaimed, in milliseconds.
         *
         * <p>A barrier is reusable, so this must exceed <b>the longest interval between
         * rounds</b>, or it would be swept away between them -- and the next round's parties
         * would find the generation out of step.
         */
        private long idleTimeoutMs = 300_000L;
    }

    /**
     * The cross-process exchanger's configuration.
     *
     * <p>There is deliberately no party count here, as there is on {@link Barrier}: an
     * exchange is always between two, and always the next two to arrive, so the two sides
     * have nothing to agree on and nothing to get out of step about.
     */
    @Data
    public static class Exchanger {

        /** Whether it is enabled. */
        private boolean enabled = false;

        /** How long one request waits for the leader's reply, in milliseconds. */
        private long requestTimeoutMs = 2_000L;

        /**
         * How long an exchange point may sit untouched before it is reclaimed, in
         * milliseconds.
         *
         * <p>It does double duty. As elsewhere it sweeps records nobody uses, and it must
         * therefore exceed <b>the longest interval between exchanges</b> under one name.
         *
         * <p>But it also bounds how long an <b>item</b> may sit unclaimed inside a record
         * still in use -- which happens when a paired process dies between handing its item
         * over and collecting the one meant for it. Set it far too high and one such death
         * pins an item in the leader's memory for that long.
         */
        private long idleTimeoutMs = 300_000L;
    }

    /**
     * The DAG engine's configuration.
     *
     * <p>There is very little, and that is on purpose. A graph carries its own shape, its own
     * channels and its own conditions, all in code where they can be read and tested. Anything
     * put here would be a second place to look.
     */
    @Data
    public static class Dag {

        /**
         * Whether the engine is enabled.
         *
         * <p><b>It has to be on for every replica</b>, not only the one that starts a run. A
         * replica with it off has no node dispatcher, so work sent to it comes back refused.
         * Half a cluster configured is worse than none, because it works until a dispatch
         * happens to land on the wrong instance.
         */
        private boolean enabled = false;

        /**
         * How long a whole run may take, in milliseconds. 0 waits indefinitely.
         *
         * <p>It is the ceiling on the run, not on a node: a graph of ten slow nodes needs the
         * sum of them. A run that overruns comes back with whatever finished and a failure
         * saying so, rather than throwing away the part that worked.
         *
         * <p>Waiting for ever is the default because a graph's natural duration is the
         * application's business, and a number invented here would be wrong for somebody.
         * {@code CompiledGraph.invoke(state, timeout, unit)} overrides it per run.
         */
        private long defaultTimeoutMs = 0L;
    }

    /** The distributed lock's configuration. */
    @Data
    public static class Mutex {

        /** Whether the distributed lock is enabled. */
        private boolean enabled = false;

        /**
         * The lease, in milliseconds.
         *
         * <p>A holder renews before the lease expires, and a failed renewal releases the lock.
         * Too short, and one network wobble may lose it; too long, and others wait longer after
         * a holder freezes. An ordinary crash or departure is unaffected by this value -- the
         * leader releases the lock the moment it receives the departure event.
         */
        private long leaseMs = 15_000L;

        /** How long to wait for the leader's reply, in milliseconds. A timeout counts as
         *  failure, and the layer above decides whether to retry. */
        private long requestTimeoutMs = 2_000L;

        /**
         * How long to wait before retrying a lock that was not acquired, in milliseconds.
         *
         * <p>It keeps to the same rhythm while leadership is vacant, and resumes of its own
         * accord once a new leader takes the cluster port.
         */
        private long retryIntervalMs = 100L;
    }

    /** The distributed semaphore's configuration; every entry means what {@link Mutex}'s
     *  does. */
    @Data
    public static class Semaphore {

        /** Whether the distributed semaphore is enabled. */
        private boolean enabled = false;

        /** The lease, in milliseconds. A holder renews before it expires, and a failed renewal
         *  releases the permit. */
        private long leaseMs = 15_000L;

        /** How long to wait for the leader's reply, in milliseconds. */
        private long requestTimeoutMs = 2_000L;

        /** How long to wait before retrying a permit that was not acquired, in
         *  milliseconds. */
        private long retryIntervalMs = 100L;
    }

    /**
     * The configuration for cluster-wide exclusion of scheduled tasks.
     *
     * <p>The tasks themselves are still defined with Spring's own {@code @Scheduled} -- the
     * period, the cron expression and the initial delay all unchanged. This solves only the
     * problem of several instances running the same task at once.
     */
    @Data
    public static class Scheduling {

        /** Whether it is enabled. Switched off, not even the components are created, and
         *  scheduled tasks behave exactly as they did before this package. */
        private boolean enabled = false;

        /**
         * Whether it applies to <b>every</b> {@code @Scheduled} task.
         *
         * <p>false by default, so only tasks additionally carrying
         * {@link MultiProcessingScheduled} are made exclusive and the rest execute exactly as
         * before -- the pluggable default posture.
         *
         * <p>true makes everything exclusive with one line of configuration and no annotation
         * in the code. But note that <b>every scheduled task in this process becomes serialised
         * across the cluster</b>, including those that should run once per instance, such as
         * each refreshing its own local cache.
         */
        private boolean applyToAll = false;

        /**
         * The default granularity when {@code applyToAll} is in effect.
         *
         * <p>APPLICATION by default -- exclusive only against other instances of the same
         * application, leaving other applications in the cluster unaffected.
         */
        private Scope defaultScope = Scope.APPLICATION;
    }

    /**
     * The cross-process task dispatch configuration.
     *
     * <p>Tasks are dispatched only <b>among replicas of the same application</b>, and there is
     * no cluster-level option: dispatch rests on the peer having the same class and method,
     * which does not hold across applications.
     */
    @Data
    public static class Pooling {

        /** Whether it is enabled. */
        private boolean enabled = false;

        /** How long to wait for a remote result, in milliseconds. On a timeout a recursive task
         *  is recomputed locally and a method call throws. */
        private long requestTimeoutMs = 10_000L;

        /**
         * How many levels a recursive task is dispatched outward at most; past that it is
         * computed locally.
         *
         * <p>Without the limit, a recursion a few levels deep would inflate the message volume
         * exponentially while the subtasks grew ever smaller, until all the time went on the
         * network.
         */
        private int maxDepth = 3;

        /** The parallelism for running tasks locally, which is also how many threads handle
         *  inbound requests. */
        private int parallelism = Runtime.getRuntime().availableProcessors();
    }

    /**
     * The distributed aggregation (MapReduce) configuration.
     *
     * <p>It spreads one aggregation across the whole cluster: shards go out, intermediate
     * results gather by key on the several nodes, and the partial reductions return to the
     * submitter to be joined into one.
     */
    @Data
    public static class Aggregation {

        /** Whether it is enabled. */
        private boolean enabled = false;

        /**
         * A job's default timeout, in milliseconds.
         *
         * <p>Far larger than the other components', because what it waits for is <b>user code
         * to finish</b> -- what gets spread across a cluster is work that takes a long time on
         * one machine to begin with.
         *
         * <p>No node is told to stop after a timeout: interrupting a distributed computation
         * halfway through is far harder than letting it finish, and the gain is a few seconds
         * of CPU.
         */
        private long requestTimeoutMs = 60_000L;

        /**
         * How many threads run map and reduce, that is, how many shards one node computes at
         * once.
         *
         * <p>Unset, it follows task dispatch's parallelism. What runs here is user code, and
         * raising it is not necessarily faster -- past the core count it is only contention for
         * the CPU.
         */
        private int parallelism = Runtime.getRuntime().availableProcessors();
    }

    /**
     * The cluster cache's configuration.
     *
     * <p>The leader writes and broadcasts incrementally to the others, and every process holds
     * a complete copy.
     */
    @Data
    public static class Cache {

        /** Whether it is enabled. */
        private boolean enabled = false;

        /**
         * The replication scope: data is synchronised only to instances under this application
         * name. Blank, the default, means the whole cluster.
         *
         * <p>The typical setting is your own {@code spring.application.name}, so that the cache
         * is shared only among your application's instances and other applications in the
         * cluster neither see it nor spend memory and bandwidth on it.
         *
         * <p><b>Note that the leader is cluster-level</b>: the write path is fixed on whichever
         * node holds the cluster port, which need not belong to this application. That is no
         * matter -- the leader holds a full copy regardless, being the executor of every write,
         * and on taking over without data in hand it first pulls a copy from one of the group's
         * instances before it begins numbering.
         *
         * <p>Once set, instances that are <b>neither in the group nor the leader</b> hold no
         * copy. They can write, forwarding to the leader, but a local read means nothing.
         */
        private String applicationName = "";

        /**
         * The total time one write may take, in milliseconds, retries included.
         *
         * <p>While leadership is vacant a write retries within this window and resumes of its
         * own accord once a new leader takes the cluster port; only failing past the window
         * throws.
         *
         * <p><b>This value must exceed the cluster's takeover time</b>, or a write landing in
         * the changeover window is bound to fail. Measured with five nodes,
         * {@code takeover-delay-ms=300} and {@code leader-quiet-period-ms=2000}: takeover takes
         * 3.3 to 4.4 seconds, and at 3 seconds every failover lost a write while at 8 seconds
         * none was lost -- that write blocked for 4 seconds and then succeeded on the new
         * leader.
         *
         * <p>The cost is that a caller's thread may hang for that long. Where that is
         * unacceptable, lower it and be ready to handle {@code ProcessingCacheException} in the
         * application.
         */
        private long requestTimeoutMs = 8_000L;

        /** How long to wait before retrying a failed write, in milliseconds. */
        private long retryIntervalMs = 50L;

        /**
         * How long a version gap may be waited on, in milliseconds, before a full snapshot is
         * pulled.
         *
         * <p>Updates are applied in strict version order, and one missing in between has to be
         * waited for. During that wait this node's data <b>stands where the gap began</b> --
         * not wrong, only old. Too short, and slight network reordering triggers full pulls
         * often; too long, and the data stays old for longer.
         */
        private long gapTimeoutMs = 1_000L;

        /** How often the leader sweeps for expired keys, in milliseconds. */
        private long sweepIntervalMs = 1_000L;

        /**
         * The byte cap on one chunk of a full snapshot.
         *
         * <p>The transport caps a single frame at 8MB by default, and this leaves ample
         * headroom. With many keys a snapshot is split into several chunks sent in turn.
         */
        private int snapshotChunkBytes = 1024 * 1024;

        /**
         * How many updates one frame packs at most.
         *
         * <p>Under heavy writing, the broadcast thread packs the updates backed up in the queue
         * into one frame, and throughput is no longer bounded by frames per second. Raising it
         * lifts peak throughput at the cost of larger frames and more for the receiver to handle
         * at once.
         *
         * <p>Note that this is <b>not</b> batching by delay: with an empty queue, one update
         * arrives and one is sent, and latency is unaffected.
         */
        private int maxBatchSize = 512;

        /**
         * How many bytes one frame holds at most. <b>0, the default, derives it from the
         * transport protocol.</b>
         *
         * <p>Capping by count alone is not enough: 512 updates each carrying a few kilobytes of
         * value make a frame of several megabytes. <b>A UDP datagram holds at most 65507 bytes,
         * and a frame beyond that cannot be sent at all</b> -- while the replication stream is
         * strictly ordered by version, so one lost frame blocks everything after it, which
         * shows up as "the cache suddenly stopped synchronising" with nothing reported.
         *
         * <p>The derived values: 60000 for UDP, leaving headroom below 65507 for the protocol
         * headers, and a quarter of the frame cap for TCP. <b>And it is a hard guarantee</b>:
         * the encoded size is measured afterwards, and a batch that really exceeds it is split
         * in half and sent as several frames, so "it cannot be sent" never arises.
         */
        private int maxBatchBytes = 0;

        /**
         * The pending broadcast queue's capacity.
         *
         * <p>A full queue discards updates without blocking the write path, and the receiver
         * then pulls a full snapshot on noticing the version gap. Reaching that point means the
         * replicas can no longer keep up with the leader's write rate.
         */
        private int outboxCapacity = 100_000;

        /**
         * How many threads handle inbound requests.
         *
         * <p>The leader handles the write and snapshot requests other nodes forward on these.
         * Writes queue for the same state lock, so raising it does not lift write throughput --
         * what it guards against is a few slow requests blocking everything behind them.
         */
        private int inboundThreads = 4;

        /**
         * How long to wait for a full snapshot, in milliseconds.
         *
         * <p>It must cover the peer serialising the whole body of data and sending every chunk,
         * which is proportional to the key count. With a few hundred thousand keys, raise it, or
         * every pull fails and retries endlessly.
         */
        private long snapshotTimeoutMs = 30_000L;

        /**
         * The background maintenance interval, in milliseconds: checking for gaps and sweeping
         * expiries.
         *
         * <p>0 derives it, taking half the smaller of {@code gapTimeoutMs} and
         * {@code sweepIntervalMs}, so that neither is ever left late. A non-zero value is used
         * as given.
         */
        private long maintenanceIntervalMs = 0L;

        // --------------------------------------------------------------
        // Eviction
        // --------------------------------------------------------------

        /**
         * The cap on the key count. 0 means no limit.
         *
         * <p>It stands in an "or" relation with {@code maxBytes}: exceeding either begins
         * eviction. With both at 0 the cache grows until it eats the heap -- configure that only
         * where the key count is bounded to begin with.
         */
        private long maxKeys = 1_000_000L;

        /**
         * The cap on content bytes. <b>-1, the default, derives it as 25% of the maximum
         * heap</b>; 0 means no limit.
         *
         * <p>No absolute default is fixed, because the same number would OOM in a 512MB
         * container and waste most of a 32GB machine -- only a proportion of the heap makes one
         * default fit everywhere.
         *
         * <p>The measurement is <b>approximate</b>: byte[] lengths plus a fixed per-entry
         * overhead, without the JVM's real object header layout and without the map's own bucket
         * array. Real usage is usually 20% to 50% above this figure, and a quarter leaves room
         * for that error and for the application's own memory.
         */
        private long maxBytes = -1L;

        /**
         * Whether the cache is written to disk at shutdown and read back at startup. <b>Off</b>
         * by default.
         *
         * <h2>It is not a dual write</h2>
         * The disk is touched exactly twice: read once at startup, written once at shutdown. Not
         * one byte is written while running, so write-path latency is entirely unaffected --
         * quite unlike "every write synchronises to external storage", which turns a 0.0005ms
         * local write into a network round trip.
         *
         * <p>The cost, stated plainly: <b>kill -9 or a power cut loses this run's data</b>.
         * That is the right trade-off for a cache -- what is wanted is "no need to send every
         * request to the database after a restart", not "nothing may ever be lost". Anything
         * that genuinely needs the latter should not live in a cache alone.
         *
         * <h2>Only the leader takes part</h2>
         * The leader opens a new epoch as soon as it has loaded, and the followers seeing the
         * epoch change pull the data through an ordinary full synchronisation -- so the data
         * still spreads across the whole cluster, with a single entry point. Having each
         * follower load a file of its own would only manufacture divergence.
         */
        private boolean persistent = false;

        /**
         * The cache file's path; {@code ~/.spreader/cache} by default.
         *
         * <p>Several applications on one machine must each set their own, or they overwrite each
         * other -- the file name carries no application name, because several instances of one
         * application should point at the same file anyway, and only the leader writes it.
         */
        private String persistentFile;

        /** The eviction policy; LRU by default. */
        private EvictionPolicy evictionPolicy = EvictionPolicy.LRU;

        /**
         * How many candidates each eviction samples at random before picking the one most
         * deserving removal.
         *
         * <p>Exact LRU is avoided because it would touch a global list on every read, needing a
         * global lock and cutting read throughput by an order of magnitude. More samples come
         * closer to exact, at the cost of scanning a few more each time. Redis's default is 5
         * as well.
         */
        private int evictionSamples = 5;

        /**
         * How many keys one maintenance cycle evicts at most.
         *
         * <p>The limit keeps a large overshoot from blocking the leader, and keeps the DELs from
         * tens of thousands of evictions from flooding the broadcast queue at once. Finishing
         * over a few cycles is fine.
         */
        private int evictionBatch = 1_000;

        /**
         * How often a follower reports which keys it has read to the leader, in milliseconds. 0
         * reports nothing.
         *
         * <p><b>Without the report, LRU barely works</b>: this cache's reads all happen locally
         * on each node and the leader cannot see them, so with only the writes to go on it
         * deletes as cold a hot key that is read and never written.
         *
         * <p>The report is sampled and capped in size, and is not complete -- eviction is a
         * heuristic, and an approximation is enough.
         */
        private long accessReportIntervalMs = 5_000L;

        /** The reporting sample rate: one key is recorded per this many reads on average. Reads
         *  run into the tens of millions a second, and recording them all would crush it. */
        private int accessReportSampleRate = 16;

        /** How many keys one round gathers to report at most, so that a scanning access pattern
         *  does not eat the memory. */
        private int accessReportMaxKeys = 10_000;
    }
}
