package com.chaconneai.openspreader.aggregation;

import com.chaconneai.openspreader.MultiProcessingService;
import com.chaconneai.openspreader.concurrent.ExecutorServiceHolder;
import com.chaconneai.openspreader.serialization.ObjectCodec;
import com.chaconneai.spreader.GossipCluster;
import com.chaconneai.spreader.Node;
import com.chaconneai.spreader.loadbalance.LoadBalancer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Distributed aggregation's execution engine: one node plays three roles at once.
 *
 * <table border="1">
 *   <caption>The three roles and their state</caption>
 *   <tr><th>Role</th><th>When it applies</th><th>What it waits for</th></tr>
 *   <tr><td><b>Submitter</b></td><td>{@code submit} was called</td>
 *       <td>Each reducer's partial result ({@link Coordination})</td></tr>
 *   <tr><td><b>Mapper</b></td><td>A {@code MAP_TASK} arrived</td>
 *       <td>Nothing; it computes, sends, and is done</td></tr>
 *   <tr><td><b>Reducer</b></td><td>A {@code SHUFFLE} arrived</td>
 *       <td>{@code shardCount} shard messages ({@link ShuffleCollector})</td></tr>
 * </table>
 *
 * <p>One process may be all three at once, and on a single-node cluster certainly is.
 *
 * <h2>Four invariants</h2>
 * <ol>
 *   <li><b>No work on the message thread.</b> An arriving message is handed straight to
 *       {@link ExecutorServiceHolder#forMapReduceInbound()}. Running {@code map} on the
 *       dispatch thread would occupy it -- and a full inbound buffer <b>discards</b>, dropping
 *       exactly the intermediate results this node is waiting for: a shape that starves
 *       itself.</li>
 *   <li><b>The reducer list is fixed by the submitter.</b> Were each mapper to read the member
 *       list itself, gossip's eventual consistency would have them see different lists while
 *       membership wobbles, and one key would land on two nodes -- <b>a wrong answer rather
 *       than an error</b>.</li>
 *   <li><b>One shard sends one message per reducer.</b> That message is itself "this shard is
 *       finished for you", so there is no reordering problem where an end marker arrives before
 *       the data.</li>
 *   <li><b>Every count de-duplicates by identity.</b> The message layer resends, so the same
 *       message may arrive twice. A counter would reach its total early and trigger the
 *       reduction before the data was complete.</li>
 * </ol>
 *
 * <h2>Sending to oneself does not touch the network</h2>
 * A shard or an intermediate result may land on this node. The message then goes straight to
 * the local handling logic, <b>bypassing the cluster message layer</b> --
 * {@code unicastOn(channel, self, ...)} returns false silently for oneself, and even if it
 * could send, it would serialise once for nothing and come back round through the inbound
 * pool.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
public class MapReduceService implements MultiProcessingService {

    private static final Logger log = LoggerFactory.getLogger(MapReduceService.class);

    /** Its own channel. Other components' listeners never see these messages, nor this one
     *  theirs. */
    public static final String CHANNEL = "spreader.mapreduce";

    private final GossipCluster cluster;
    private final Map<String, MapReduceJob<?, ?, ?, ?>> jobs;
    private final ObjectCodec codec;
    private final ExecutorService inbound;
    private final long defaultTimeoutMs;

    private final AtomicLong jobIdGen = new AtomicLong();

    /** Jobs this node is awaiting results for <b>as the submitter</b>, keyed by local
     *  jobId. */
    private final Map<Long, Coordination> coordinating = new ConcurrentHashMap<>();

    /**
     * Jobs this node is collecting for <b>as a reducer</b>.
     *
     * <p>The key is {@code coordinatorId + "#" + jobId} and <b>cannot be the jobId alone</b>:
     * two nodes number their own jobs independently, and the numbers will certainly
     * collide.
     */
    private final Map<String, ShuffleCollector> collecting = new ConcurrentHashMap<>();

    /**
     * The shuffle strategy this node uses <b>as a mapper</b>, cached per job.
     *
     * <p>One per shard will not do: consistent hashing caches a virtual node ring internally,
     * and each construction recomputes several hundred MD5 hashes. Several shards of one job
     * landing in the same JVM share one instance, and the ring is built once.
     *
     * <p>It carries a timestamp because <b>no event tells this node that a job has
     * finished</b>: a mapper sends its intermediate results and leaves, and nobody comes back
     * to tell it. Only a periodic sweep can clear them.
     */
    private final Map<String, Stamped<LoadBalancer>> shuffleBalancers = new ConcurrentHashMap<>();

    /** The thread pools' owner; the sweep takes its scheduler from here too. */
    private final ExecutorServiceHolder executors;

    /** Sweeps periodically for state that will never be completed. */
    private ScheduledFuture<?> sweeper;

    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong mapsRun = new AtomicLong();
    private final AtomicLong reducesRun = new AtomicLong();

    public MapReduceService(GossipCluster cluster, Map<String, MapReduceJob<?, ?, ?, ?>> jobs,
                            ObjectCodec codec, long defaultTimeoutMs,
                            ExecutorServiceHolder executors) {
        this.cluster = cluster;
        this.jobs = jobs == null ? Map.of() : Map.copyOf(jobs);
        this.codec = codec;
        this.defaultTimeoutMs = defaultTimeoutMs;
        this.executors = executors;
        this.inbound = executors.forMapReduceInbound();
    }

    @Override
    public void start() {
        cluster.addListener(CHANNEL, this);
        // The sweep runs at half the timeout, so leftovers linger for at most one and a half
        // timeout periods rather than piling up
        long period = Math.max(1_000L, defaultTimeoutMs / 2);
        sweeper = executors.forMaintenance().scheduleWithFixedDelay(
                this::sweepStale, period, period, TimeUnit.MILLISECONDS);
        log.info("Distributed aggregation enabled on channel {} with {} job(s) registered {}",
                CHANNEL, jobs.size(), jobs.keySet());
    }

    @Override
    public void close() {
        if (sweeper != null) {
            sweeper.cancel(false);
        }
        // The thread pools are not closed: they belong to ExecutorServiceHolder
        coordinating.values().forEach(c -> c.future.completeExceptionally(
                new MapReduceException("the node is shutting down and the job was cancelled")));
        coordinating.clear();
        collecting.clear();
        shuffleBalancers.clear();
    }

    /**
     * Clears state that will never be completed.
     *
     * <h2>Why this is necessary</h2>
     * Neither the reducer nor the mapper role <b>knows when a job ends</b>:
     *
     * <ul>
     *   <li>An entry in {@link #collecting} is removed only when <b>all {@code shardCount}
     *       shards have arrived</b>. A submitter that died partway, or a mapper that failed to
     *       send its intermediate results, leaves that entry for ever -- holding <b>every
     *       intermediate result</b>, possibly hundreds of megabytes</li>
     *   <li>{@link #shuffleBalancers} holds one entry per job with no moment at which to
     *       remove it. Ten thousand jobs are ten thousand consistent-hash instances, each with
     *       a TreeMap of several hundred nodes</li>
     * </ul>
     *
     * <p>The submitter's side ({@code coordinating}) does not have this problem: the future
     * carries a timeout, and success, failure and timeout all reach the {@code handle} callback
     * that removes it.
     *
     * <p>The threshold is three times the timeout: a normal job should long since have
     * finished, and anything still there has certainly been abandoned.
     */
    private void sweepStale() {
        try {
            long deadline = System.nanoTime()
                    - TimeUnit.MILLISECONDS.toNanos(defaultTimeoutMs * 3);
            int dropped = 0;
            for (Map.Entry<String, ShuffleCollector> e : collecting.entrySet()) {
                if (e.getValue().createdNanos() < deadline) {
                    collecting.remove(e.getKey());
                    dropped++;
                }
            }
            int balancers = 0;
            for (Map.Entry<String, Stamped<LoadBalancer>> e : shuffleBalancers.entrySet()) {
                if (e.getValue().createdNanos() < deadline) {
                    shuffleBalancers.remove(e.getKey());
                    balancers++;
                }
            }
            if (dropped > 0 || balancers > 0) {
                // info rather than debug: intermediate results being swept means some job did
                // not finish, and that deserves to be seen rather than to happen quietly
                log.info("Swept {} incomplete set(s) of intermediate results and {} idle shuffle "
                        + "strateg(ies). Some job did not finish -- the submitter went away, or "
                        + "a shard's intermediate results were never sent",
                        dropped, balancers);
            }
        } catch (RuntimeException e) {
            // The sweep dying would quietly bring the leak back, so it must be caught here
            log.warn("Sweeping aggregation leftovers errored: {}", e.toString());
        }
    }

    /** A cache entry carrying its creation time -- something with no "finished" event can
     *  only be cleared by time. */
    private record Stamped<T>(T value, long createdNanos) {

        static <T> Stamped<T> of(T value) {
            return new Stamped<>(value, System.nanoTime());
        }
    }

    // ==================================================================
    // The submitter
    // ==================================================================

    <K, R> CompletableFuture<MapReduceResult<K, R>> submit(String jobBeanName, Object input,
                                                           long timeoutMs) {
        MapReduceJob<Object, Object, Object, Object> job = lookup(jobBeanName);
        if (job == null) {
            return failedFuture("no MapReduceJob named [" + jobBeanName + "] was found. The "
                    + "registered ones are " + jobs.keySet() + ". Note that every node in the "
                    + "cluster needs this bean, under the same name and with the same "
                    + "implementation");
        }

        // The list is fixed here and travels with the task to every mapper. Whatever the
        // cluster does afterwards leaves this round unaffected -- otherwise mappers would see
        // different lists and one key would land on two nodes, giving a wrong answer rather
        // than an error
        List<Node> snapshot = cluster.members();
        if (snapshot.isEmpty()) {
            return failedFuture("the cluster has no members, so no job can be submitted");
        }

        List<?> shards;
        try {
            shards = job.split(input, snapshot.size());
        } catch (RuntimeException e) {
            return failedFuture("split failed: " + e, e);
        }
        if (shards == null || shards.isEmpty()) {
            return failedFuture("split produced 0 shards. With no mapper there are no "
                    + "intermediate results, and a reducer never receives the signal that "
                    + "everything has arrived");
        }

        long jobId = jobIdGen.incrementAndGet();
        List<String> reducerIds = snapshot.stream().map(Node::id).toList();
        Coordination c = new Coordination(jobId, jobBeanName, reducerIds, shards.size());
        coordinating.put(jobId, c);
        submitted.incrementAndGet();

        try {
            dispatchShards(jobId, jobBeanName, shards, snapshot, reducerIds);
        } catch (RuntimeException e) {
            coordinating.remove(jobId);
            failed.incrementAndGet();
            return e instanceof MapReduceException m
                    ? CompletableFuture.failedFuture(m)
                    : failedFuture("dispatching the shards failed: " + e, e);
        }

        // No node is told to stop after a timeout: interrupting a distributed computation
        // halfway through is far harder than letting it finish, and the gain is a few seconds
        // of CPU
        return c.future
                .orTimeout(timeoutMs <= 0 ? defaultTimeoutMs : timeoutMs, TimeUnit.MILLISECONDS)
                .handle((result, error) -> {
                    coordinating.remove(jobId);
                    if (error != null) {
                        failed.incrementAndGet();
                        throw new CompletionException(explain(error, c));
                    }
                    completed.incrementAndGet();
                    // The figures are fixed at this point: a late partial result no longer
                    // changes them, and elapsedMs measures exactly the stretch from submission
                    // to the complete result
                    return new MapReduceResult<>(this.<K, R>castResult(result), c.stats());
                });
    }

    /**
     * Shard i goes to node i.
     *
     * <p><b>No load balancing is used here</b>; a modulo is enough: deterministic, stateless,
     * and identical however many times it runs. Round-robin for the same job would only add a
     * piece of instance state. With more shards than nodes, some nodes naturally take several;
     * to have a stronger machine do more, {@code split} should produce shards of different
     * sizes, which is the more direct knob.
     */
    private void dispatchShards(long jobId, String jobBeanName, List<?> shards,
                                List<Node> snapshot, List<String> reducerIds) {
        String selfId = cluster.self().id();
        for (int i = 0; i < shards.size(); i++) {
            Node target = snapshot.get(i % snapshot.size());
            byte[] payload = codec.encode(shards.get(i));
            MapReduceMessage msg = MapReduceMessage.mapTask(jobId, jobBeanName, i,
                    shards.size(), reducerIds, selfId, payload);
            if (!dispatch(target, msg)) {
                throw new MapReduceException("shard " + i + " failed to send to "
                        + target.label() + ", so the job stops. Shards are not resent -- there "
                        + "is no telling whether the peer never received it or received it and "
                        + "lost the acknowledgement, and resending could have the shard "
                        + "computed twice");
            }
        }
    }

    /** A partial result arrived from some reducer. */
    private void onReduceResult(Node sender, MapReduceMessage msg) {
        Coordination c = coordinating.get(msg.jobId());
        if (c == null) {
            // The job has already timed out or failed. The peer computed for nothing, but that
            // is not an error -- a normal timeout path should not fill the log with warnings
            log.debug("A partial result arrived that nobody is waiting for: job={}, from {}",
                    msg.jobId(), sender.label());
            return;
        }
        if (!msg.success()) {
            c.future.completeExceptionally(new MapReduceException(
                    "node " + sender.label() + " failed to reduce: " + msg.message()));
            return;
        }
        try {
            Object decoded = codec.decode(msg.payload());
            if (decoded instanceof ReducePayload p) {
                c.absorb(sender.id(), p);
            } else {
                c.future.completeExceptionally(new MapReduceException(
                        "the partial result is in an unrecognised format: "
                                + (decoded == null ? "null"
                                : decoded.getClass().getName())
                                + ". Is some node in the cluster still running an older "
                                + "version?"));
            }
        } catch (RuntimeException e) {
            c.future.completeExceptionally(
                    new MapReduceException("the partial result failed to deserialise", e));
        }
    }

    // ==================================================================
    // mapper
    // ==================================================================

    private void onMapTask(MapReduceMessage msg) {
        MapReduceJob<Object, Object, Object, Object> job = lookup(msg.jobBeanName());
        if (job == null) {
            replyFailure(msg, "this node has no MapReduceJob named [" + msg.jobBeanName()
                    + "]. Every node in the cluster needs this bean, under the same name and "
                    + "with the same implementation");
            return;
        }

        List<Node> reducers = resolve(msg.reducerIds());
        if (reducers.size() != msg.reducerIds().size()) {
            replyFailure(msg, "the reducer list names nodes no longer in the cluster: "
                    + msg.reducerIds().size() + " were expected and only " + reducers.size()
                    + " were found. The list is fixed by the submitter, and losing one partway "
                    + "makes \"one key is reduced once\" impossible to guarantee, so the whole "
                    + "job fails");
            return;
        }

        // Several shards of one job share one strategy instance, so the consistent-hash ring
        // is built once
        LoadBalancer balancer = shuffleBalancers
                .computeIfAbsent(keyOf(msg), k -> Stamped.of(job.shuffleBalancer())).value();

        // Gathered by reducer and sent once map has finished. The whole method runs as one
        // task on the inbound pool, so only one thread touches these maps and a plain HashMap
        // is enough
        Map<String, Map<Object, List<Object>>> grouped = new HashMap<>();

        // Key to reducing node, remembered once computed.
        //
        // This cache is not an optional optimisation: choose() lands on every intermediate
        // record, and each consistent-hash call first computes a signature of the candidate set
        // -- building a list, joining strings, sorting -- and then an MD5 of the key, creating
        // a MessageDigest as it goes. "the" appearing ten thousand times in a word count is ten
        // thousand identical computations giving identical answers.
        //
        // The keys are already held in grouped, so this stores one more reference and the
        // memory cost is negligible
        Map<Object, Node> owners = new HashMap<>();

        // The emit count. This whole stretch runs on one thread, so an array as a mutable slot
        // saves a CAS against an AtomicLong
        long[] emitted = new long[1];

        Emitter<Object, Object> emitter = (key, value) -> {
            if (key == null) {
                throw new MapReduceException(
                        "an emitted key must not be null -- it decides which node reduces the "
                                + "record");
            }
            Node owner = owners.computeIfAbsent(key, k -> balancer.choose(reducers, k));
            if (owner == null) {
                throw new MapReduceException("shuffleBalancer returned null for key [" + key
                        + "]. It must choose a reducing node for every key");
            }
            grouped.computeIfAbsent(owner.id(), x -> new HashMap<>())
                    .computeIfAbsent(key, x -> new ArrayList<>())
                    .add(value);
            emitted[0]++;
        };

        long mapStarted = System.nanoTime();
        try {
            job.map(codec.decode(msg.payload()), emitter);
        } catch (RuntimeException e) {
            log.warn("map failed for shard {}: {}", msg.shardIndex(), e.toString());
            replyFailure(msg, "map failed for shard " + msg.shardIndex() + ": " + e);
            return;
        }
        mapsRun.incrementAndGet();

        long combineCalls = 0;
        if (job.combinable()) {
            long[] calls = new long[1];
            if (!combineLocally(job, grouped, msg, calls)) {
                return;
            }
            combineCalls = calls[0];
        }
        // How many records remain after the combine -- this is what really goes on the network
        long shuffled = countRecords(grouped);
        long mapNanos = System.nanoTime() - mapStarted;

        // Every reducer receives a message, even where no key of this shard landed on it --
        // that empty message is "this shard is finished for you". One message short and that
        // reducer never gathers shardCount of them, never reduces, and the submitter waits out
        // its timeout alongside
        for (Node reducer : reducers) {
            Map<Object, List<Object>> batch = grouped.get(reducer.id());
            // Every message carries this shard's figures. The reducer de-duplicates by
            // shardIndex, so M copies are not counted M times -- while one copy short could
            // lose the shard's figures entirely
            MapReduceMessage out = MapReduceMessage.shuffle(msg.jobId(), msg.jobBeanName(),
                    msg.shardIndex(), msg.shardCount(), msg.coordinatorId(),
                    codec.encode(batch == null ? new HashMap<>() : batch),
                    emitted[0], shuffled, combineCalls, mapNanos);
            if (!dispatch(reducer, out)) {
                replyFailure(msg, "shard " + msg.shardIndex() + "'s intermediate results failed "
                        + "to send to " + reducer.label());
                return;
            }
        }
    }

    /**
     * Reduces locally before sending -- Hadoop calls this a combiner.
     *
     * <p>{@code emit(word, 1)} produces tens of thousands of {@code 1}s for "the", each
     * serialised, sent over the network, and added to a list on the reducer. Reducing locally
     * first leaves <b>one record</b> per key -- the traffic falls in inverse proportion to how
     * repetitive the keys are, and word counting drops by an order of magnitude or two.
     *
     * <p>It happens only where {@link MapReduceJob#combinable()} declares true, since it
     * requires reduce to be associative. See that method for why.
     *
     * @return true on success; where combine throws, the submitter has already been notified
     *         and it returns false
     */
    private boolean combineLocally(MapReduceJob<Object, Object, Object, Object> job,
                                   Map<String, Map<Object, List<Object>>> grouped,
                                   MapReduceMessage msg, long[] calls) {
        try {
            for (Map<Object, List<Object>> batch : grouped.values()) {
                // Replaced in place: what the combine produced is still "this key's
                // intermediate value", and the reducer combines what the mappers send and
                // reduces once more as usual
                batch.replaceAll((key, values) -> {
                    if (values.size() <= 1) {
                        // With one record there is nothing to combine, and calling reduce
                        // would achieve nothing
                        return values;
                    }
                    calls[0]++;
                    return List.of(job.reduce(key, values));
                });
            }
            return true;
        } catch (RuntimeException e) {
            log.warn("the local combine failed for shard {}: {}", msg.shardIndex(),
                    e.toString());
            replyFailure(msg, "the local combine failed for shard " + msg.shardIndex() + ": " + e
                    + ". This job declares combinable()=true, meaning its reduce should be able "
                    + "to run over part of the data first -- check whether that premise really "
                    + "holds");
            return false;
        }
    }

    // ==================================================================
    // reducer
    // ==================================================================

    private void onShuffle(MapReduceMessage msg) {
        String key = keyOf(msg);
        ShuffleCollector collector = collecting.computeIfAbsent(key, k ->
                new ShuffleCollector(msg.jobBeanName(), msg.shardCount()));

        boolean ready;
        try {
            ready = collector.accept(msg.shardIndex(), asBuckets(codec.decode(msg.payload())),
                    new long[] {msg.emitted(), msg.shuffled(),
                            msg.combineCalls(), msg.phaseNanos()});
        } catch (RuntimeException e) {
            collecting.remove(key);
            sendTo(msg.coordinatorId(),
                    MapReduceMessage.reduceFailed(msg.jobId(),
                            "the intermediate results failed to deserialise: " + e));
            return;
        }
        if (ready) {
            collecting.remove(key);
            reduceAndReply(msg, collector);
        }
    }

    private void reduceAndReply(MapReduceMessage msg, ShuffleCollector collector) {
        MapReduceJob<Object, Object, Object, Object> job = lookup(collector.jobBeanName);
        if (job == null) {
            sendTo(msg.coordinatorId(), MapReduceMessage.reduceFailed(msg.jobId(),
                    "this node has no MapReduceJob named [" + collector.jobBeanName + "]"));
            return;
        }
        Map<Object, Object> result = new LinkedHashMap<>();
        long reduceStarted = System.nanoTime();
        try {
            for (Map.Entry<Object, List<Object>> e : collector.buckets().entrySet()) {
                result.put(e.getKey(), job.reduce(e.getKey(), e.getValue()));
            }
        } catch (RuntimeException e) {
            sendTo(msg.coordinatorId(),
                    MapReduceMessage.reduceFailed(msg.jobId(), "reduce failed: " + e));
            return;
        }
        long reduceNanos = System.nanoTime() - reduceStarted;
        reducesRun.incrementAndGet();

        // The result and the figures go back as one package. The shard figures are what this
        // reducer received from the mappers, and the submitter de-duplicates by shard index --
        // every reducer carries an identical copy
        ReducePayload payload = new ReducePayload(result, collector.shardStats(),
                result.size(), reduceNanos);
        sendTo(msg.coordinatorId(),
                MapReduceMessage.reduceResult(msg.jobId(), codec.encode(payload)));
    }

    /** Tells the submitter directly when the map phase goes wrong, rather than leaving it to
     *  wait out its timeout. */
    private void replyFailure(MapReduceMessage origin, String reason) {
        sendTo(origin.coordinatorId(), MapReduceMessage.reduceFailed(origin.jobId(), reason));
    }

    private void sendTo(String nodeId, MapReduceMessage msg) {
        Node target = find(nodeId);
        if (target == null) {
            log.warn("The submitter {} is no longer in the cluster, so there is nowhere to send "
                    + "the result: job={}", nodeId, msg.jobId());
            return;
        }
        if (!dispatch(target, msg)) {
            log.warn("Failed to send the result back to {}: job={}", target.label(),
                    msg.jobId());
        }
    }

    // ==================================================================
    // Sending and receiving
    // ==================================================================

    @Override
    public void onPayload(Node sender, byte[] content) {
        MapReduceMessage msg = MapReduceMessage.decode(content);
        if (msg == null) {
            // Not one of this protocol's messages. The channel is exclusive, so this should
            // not happen, but it must not blow up the dispatch thread
            return;
        }
        // Handed on at once: this is still gossip's dispatch thread, and running map on it
        // would occupy it entirely
        inbound.execute(() -> handle(sender, msg));
    }

    private void handle(Node sender, MapReduceMessage msg) {
        try {
            switch (msg.type()) {
                case MAP_TASK -> onMapTask(msg);
                case SHUFFLE -> onShuffle(msg);
                case REDUCE_RESULT -> onReduceResult(sender, msg);
            }
        } catch (RuntimeException e) {
            log.warn("Handling an aggregation message errored: {} - {}", msg, e.toString(), e);
        }
    }

    /**
     * Sends one message; <b>this is the only way out</b>.
     *
     * <p>"Is the target myself?" is decided in this one place. Missing that test once has a
     * well-hidden consequence: {@code unicastOn(channel, self, ...)} <b>returns false
     * silently</b> for oneself, and that shard's data vanishes without a sound. And consistent
     * hashing is stable -- the same keys are lost every time, so the result is not "all wrong"
     * but "missing a part", and nobody checks an aggregate by eye.
     */
    private boolean dispatch(Node target, MapReduceMessage msg) {
        if (target.id().equals(cluster.self().id())) {
            // Run locally: no serialisation, no network, no inbound buffer. It still goes
            // through the inbound pool, so that map does not run synchronously on the calling
            // thread
            inbound.execute(() -> handle(cluster.self(), msg));
            return true;
        }
        return cluster.unicastOn(CHANNEL, target, msg.encode());
    }

    // ==================================================================
    // State
    // ==================================================================

    /** The submitter's view: awaiting each reducer's partial result, and joining them into
     *  one once they are all in. */
    private static final class Coordination {

        private final long jobId;
        private final String jobBeanName;
        private final int expected;
        /** Reducers that have not yet delivered. <b>It de-duplicates too</b>: a resent result
         *  removes nothing and is ignored of its own accord. */
        private final Set<String> awaiting = ConcurrentHashMap.newKeySet();
        private final Map<Object, Object> merged = new LinkedHashMap<>();
        private final CompletableFuture<Map<Object, Object>> future = new CompletableFuture<>();

        /** Shard index to that shard's figures. Reducers bring duplicate copies, and the shard
         *  index de-duplicates them naturally. */
        private final Map<Integer, long[]> shardStats = new LinkedHashMap<>();
        private long reduceCalls;
        private long reduceNanos;
        private final int shards;
        private final long startedNanos = System.nanoTime();

        Coordination(long jobId, String jobBeanName, List<String> reducerIds, int shards) {
            this.jobId = jobId;
            this.jobBeanName = jobBeanName;
            this.expected = reducerIds.size();
            this.shards = shards;
            this.awaiting.addAll(reducerIds);
        }

        /** Sums the figures gathered from everywhere into one. */
        synchronized MapReduceStats stats() {
            long emitted = 0;
            long shuffled = 0;
            long combineCalls = 0;
            long mapNanos = 0;
            for (long[] s : shardStats.values()) {
                emitted += s[0];
                shuffled += s[1];
                combineCalls += s[2];
                mapNanos += s[3];
            }
            return new MapReduceStats(shards, expected, shardStats.size(), emitted, shuffled,
                    combineCalls, reduceCalls, merged.size(), mapNanos, reduceNanos,
                    (System.nanoTime() - startedNanos) / 1_000_000);
        }

        /**
         * Absorbs one partial result.
         *
         * <p><b>A key collision means the partitioning is broken.</b> Normally the reducers'
         * key sets are disjoint -- one key lands on one node. A genuine duplicate means some
         * key was half reduced on each of two nodes, and <b>both halves are incomplete</b>. It
         * must throw rather than overwrite: an exception is far better than a wrong answer.
         *
         * <p>And this error <b>cannot be found by checking the result</b> -- a summing
         * aggregate such as a word count is associative, adding separately and adding again
         * still gives the right answer, and the algorithm itself hides the bug. This check is
         * the only place that can catch it.
         */
        synchronized void absorb(String reducerId, ReducePayload payload) {
            if (!awaiting.remove(reducerId)) {
                // A resend, or a node not on the list at all. It has been merged once, and
                // merging again would double-count
                return;
            }
            // Shard figures de-duplicate by shard index: every reducer carried a complete copy
            payload.shardStats().forEach(shardStats::putIfAbsent);
            reduceCalls += payload.reduceCalls();
            reduceNanos += payload.reduceNanos();

            for (Map.Entry<Object, Object> e : payload.result().entrySet()) {
                Object previous = merged.put(e.getKey(), e.getValue());
                if (previous != null) {
                    future.completeExceptionally(new MapReduceException(
                            "key [" + e.getKey() + "] appears in more than one node's reduced "
                                    + "result, which means the shuffle's node lists disagreed -- "
                                    + "one key was half computed on each of two machines, and "
                                    + "both halves are incomplete. Check whether shuffleBalancer "
                                    + "has been replaced with a non-deterministic strategy"));
                    return;
                }
            }
            if (awaiting.isEmpty()) {
                future.complete(new LinkedHashMap<>(merged));
            }
        }

        String describe() {
            return "job=" + jobId + "(" + jobBeanName + "), received "
                    + (expected - awaiting.size()) + "/" + expected
                    + " partial result(s), still awaiting " + awaiting;
        }
    }

    /** The reducer's view: gathering the intermediate results the shards send, and reducing
     *  once they are all in. */
    private static final class ShuffleCollector {

        private final String jobBeanName;
        private final int shardCount;
        private final long createdNanos = System.nanoTime();
        private final Map<Object, List<Object>> buckets = new LinkedHashMap<>();
        /** Which shards have arrived. <b>A set rather than a counter</b> -- the message layer
         *  resends, and one shard may arrive twice. */
        private final Set<Integer> shardsSeen = ConcurrentHashMap.newKeySet();
        /** Shard index to {@code [emitted, shuffled, combineCalls, mapNanos]}, de-duplicated by
         *  shard of its own accord. */
        private final Map<Integer, long[]> shardStats = new LinkedHashMap<>();
        private boolean reduced;

        ShuffleCollector(String jobBeanName, int shardCount) {
            this.jobBeanName = jobBeanName;
            this.shardCount = shardCount;
        }

        long createdNanos() {
            return createdNanos;
        }

        /** @return true once everything has arrived, and <b>exactly one thread receives
         *          true</b> */
        synchronized boolean accept(int shardIndex, Map<Object, List<Object>> batch,
                                    long[] stats) {
            if (reduced || !shardsSeen.add(shardIndex)) {
                // The same shard resent. It has been merged, and merging again would
                // double-count
                return false;
            }
            batch.forEach((k, vs) ->
                    buckets.computeIfAbsent(k, x -> new ArrayList<>()).addAll(vs));
            shardStats.put(shardIndex, stats);
            if (shardCount > 0 && shardsSeen.size() >= shardCount) {
                reduced = true;
                return true;
            }
            return false;
        }

        synchronized Map<Object, List<Object>> buckets() {
            return buckets;
        }

        synchronized Map<Integer, long[]> shardStats() {
            return new LinkedHashMap<>(shardStats);
        }
    }

    /**
     * The package a reducer returns to the submitter: results plus figures.
     *
     * @param shardStats the shard figures this reducer received from the mappers. <b>Every
     *                   reducer carries an identical copy</b>, because every mapper sent a
     *                   message to every reducer. The submitter de-duplicates by shard index
     */
    record ReducePayload(
            Map<Object, Object> result,
            Map<Integer, long[]> shardStats,
            long reduceCalls,
            long reduceNanos) implements java.io.Serializable {
    }

    // ==================================================================
    // Miscellany
    // ==================================================================

    @Override
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("channel", CHANNEL);
        m.put("jobs", new ArrayList<>(jobs.keySet()));
        m.put("submitted", submitted.get());
        m.put("completed", completed.get());
        m.put("failed", failed.get());
        m.put("mapsRun", mapsRun.get());
        m.put("reducesRun", reducesRun.get());
        m.put("coordinating", coordinating.size());
        m.put("collecting", collecting.size());
        return m;
    }

    int runningJobs() {
        return coordinating.size();
    }

    /** How many intermediate records the grouping holds -- after a combine, this is what
     *  really goes on the network. */
    private static long countRecords(Map<String, Map<Object, List<Object>>> grouped) {
        long total = 0;
        for (Map<Object, List<Object>> batch : grouped.values()) {
            for (List<Object> values : batch.values()) {
                total += values.size();
            }
        }
        return total;
    }

    /** A job's identity on the reducer side: submitter plus number. The number alone would
     *  collide between two nodes' jobs. */
    private static String keyOf(MapReduceMessage msg) {
        return msg.coordinatorId() + "#" + msg.jobId();
    }

    @SuppressWarnings("unchecked")
    private MapReduceJob<Object, Object, Object, Object> lookup(String beanName) {
        return (MapReduceJob<Object, Object, Object, Object>) jobs.get(beanName);
    }

    /** Resolves a list of ids back into Nodes. A departed node does not appear in the result,
     *  and the caller compares the counts. */
    private List<Node> resolve(List<String> ids) {
        Map<String, Node> byId = new HashMap<>();
        for (Node n : cluster.members()) {
            byId.put(n.id(), n);
        }
        List<Node> out = new ArrayList<>(ids.size());
        for (String id : ids) {
            Node n = byId.get(id);
            if (n != null) {
                out.add(n);
            }
        }
        return out;
    }

    private Node find(String id) {
        for (Node n : cluster.members()) {
            if (n.id().equals(id)) {
                return n;
            }
        }
        return null;
    }

    private MapReduceException explain(Throwable error, Coordination c) {
        Throwable cause = error instanceof CompletionException ? error.getCause() : error;
        if (cause instanceof MapReduceException e) {
            return e;
        }
        if (cause instanceof TimeoutException) {
            // "It timed out" helps nobody investigate; it has to say who is still missing
            return new MapReduceException("the aggregation timed out. " + c.describe()
                    + ". The slow nodes are still computing, and their results will be discarded "
                    + "when they arrive");
        }
        return new MapReduceException("the aggregation failed: " + cause, cause);
    }

    private <T> CompletableFuture<T> failedFuture(String message) {
        return CompletableFuture.failedFuture(new MapReduceException(message));
    }

    private <T> CompletableFuture<T> failedFuture(String message, Throwable cause) {
        return CompletableFuture.failedFuture(new MapReduceException(message, cause));
    }

    @SuppressWarnings("unchecked")
    private <K, R> Map<K, R> castResult(Map<Object, Object> result) {
        return (Map<K, R>) result;
    }

    @SuppressWarnings("unchecked")
    private static Map<Object, List<Object>> asBuckets(Object decoded) {
        return decoded == null ? Map.of() : (Map<Object, List<Object>>) decoded;
    }
}
