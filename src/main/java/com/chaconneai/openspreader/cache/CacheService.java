package com.chaconneai.openspreader.cache;

import com.chaconneai.openspreader.MultiProcessingService;
import com.chaconneai.spreader.GossipCluster;
import java.io.IOException;
import com.chaconneai.spreader.Node;
import com.chaconneai.openspreader.concurrent.ExecutorServiceHolder;
import com.chaconneai.openspreader.cluster.SelfRegisteringListener;
import com.chaconneai.spreader.transport.TransportType;
import com.chaconneai.openspreader.idempotence.IdempotentRequestCache;
import com.chaconneai.spreader.transport.Frames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The cluster cache's core: the leader executes writes serially and broadcasts them, and
 * every other node replays them in version order.
 *
 * <h2>In one sentence</h2>
 * This is a <b>single-leader replicated state machine</b>. The leader is the only way in for a
 * write; it numbers each one with an increasing version and broadcasts "the operation plus its
 * version", and every other node replays them in strict version order. The same starting
 * point, the same sequence of operations and the same executing code ({@link CacheStore#apply}
 * throughout) leave the copies identical.
 *
 * <h2>Why operations are replicated rather than data</h2>
 * Replicating data -- sending a key's whole new content after each change -- is no different
 * for a small value under {@code set}, but one {@code lpush} on a list of a hundred thousand
 * elements would send all hundred thousand. Replicating the operation sends only that one
 * element, which is what "incremental synchronisation" actually means.
 *
 * <p>The cost is that <b>operations must be deterministic</b>: the same operation replayed on
 * any node must give the same result. So a TTL travels as milliseconds remaining, which each
 * node converts to a local absolute instant, sidestepping clock skew; and expiry deletion is
 * started by the leader rather than decided by each node.
 *
 * <h2>Performance: three things decide how fast it is</h2>
 * <ol>
 *   <li><b>Reads never touch the network</b> -- every process holds the whole body of data, a
 *       read is a local memory operation, and {@link CacheStore} has no global lock, so reads
 *       of different keys never block each other.</li>
 *   <li><b>Broadcasting batches naturally</b> -- one frame per update would pin throughput to
 *       frames per second, measured at about 2600 for an acknowledged multicast. The broadcast
 *       thread sends one when it is idle and packs the queue together when it backs up. <b>No
 *       timer batches them</b>: latency is unchanged under light load, and throughput comes
 *       free of the frame rate under heavy load.</li>
 *   <li><b>The leader's local writes touch no network</b> -- being the leader, it executes
 *       directly, and only a non-leader's write costs a round trip.</li>
 * </ol>
 *
 * <h2>What happens on a version gap</h2>
 * A version that is not "applied + 1" means something was missed in between. It must not be
 * applied over the gap, which would fork permanently. Instead it is <b>buffered</b> until the
 * missing one arrives; and where {@code gapTimeoutMs} passes without it -- the message really
 * was lost, or this node was briefly isolated -- a full snapshot is pulled from the leader to
 * realign.
 *
 * <h2>Availability</h2>
 * <ul>
 *   <li><b>A dead leader leaves reads unaffected</b> -- every process holds a complete copy and
 *       a read depends on nothing remote. Only writes fail until a new leader is chosen, and
 *       they retry within the timeout window, usually recovering within a second.</li>
 *   <li><b>A change of leader loses no data</b> -- the new leader was already a member holding
 *       a complete copy, and it takes its own as the new baseline while the others
 *       realign.</li>
 *   <li><b>A newly joined node</b> pulls a full snapshot at its own {@code onClusterJoined} and
 *       follows the increments afterwards.</li>
 *   <li><b>A node that dropped out and returned</b> pulls a full snapshot when the version gap
 *       times out.</li>
 * </ul>
 * Nowhere caches the member list at startup; the leader is looked up afresh every time through
 * {@link GossipCluster#leader()}.
 *
 * <h2>What it cannot do</h2>
 * Under a network partition each side may have a leader writing its own, and after recovery
 * they <b>cannot be merged</b> -- whichever side becomes leader last pushes its own data to
 * everyone as the baseline, and the other side's writes are simply lost. So this is a cache,
 * not a database.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class CacheService
        implements MultiProcessingService, SelfRegisteringListener {

    private static final Logger log = LoggerFactory.getLogger(CacheService.class);

    /** The cache protocol's own channel. The application's listeners subscribe to the default
     *  channel and never see these messages. */
    public static final String CHANNEL = "spreader.cache";

    /** The epoch is unknown -- this node has not aligned with any leader yet. */
    private static final long EPOCH_UNKNOWN = -1L;

    /**
     * While waiting for the epoch to catch up, a full synchronisation is rescheduled this
     * often. See {@link #awaitAppliedLocked}.
     *
     * <p>The value need only be <b>markedly smaller</b> than {@code gapTimeoutMs}: too large
     * and the wait times out first, too small and it spins while a synchronisation is genuinely
     * under way.
     */
    private static final long RESYNC_NUDGE_INTERVAL_MS = 200L;

    private final GossipCluster cluster;
    private final CacheStore store = new CacheStore();

    /**
     * The replication scope: updates are multicast only to instances under this application
     * name. Empty means the whole cluster.
     *
     * <p>The way in for a write is still the cluster leader, which need not belong to this
     * application, but the leader holds a full copy regardless -- it executes every write. See
     * {@link #holdsReplica()}.
     */
    private final String replicationName;

    private final CacheOptions options;
    private final long requestTimeoutMs;
    private final long retryIntervalMs;
    private final long gapTimeoutMs;
    private final int snapshotChunkBytes;
    private final int maxBatchSize;

    /**
     * How many bytes one frame holds at most.
     *
     * <p>Capping by count alone is not enough: 512 updates each carrying a few hundred bytes of
     * value make a frame of several hundred kilobytes. <b>A UDP datagram is capped at 65507
     * bytes and anything beyond it cannot be sent at all</b> -- while the replication stream is
     * strictly ordered by version, so one lost frame blocks everything after it, which shows up
     * as "the cache suddenly stopped synchronising" with nothing reported.
     *
     * <p>So the cap is derived from the transport protocol and needs no configuration: 32KB for
     * UDP, leaving ample headroom, and a quarter of the frame cap for TCP.
     */
    private final int maxBatchBytes;

    /**
     * The leader epoch. It takes a new value with each change of leader, so that "whoever
     * issues the version numbers is no longer the same node" can be detected. Without it, a new
     * leader numbering from 1 while the others had applied up to 500 would have its updates
     * discarded as duplicates.
     */
    private volatile long epoch = EPOCH_UNKNOWN;
    /**
     * Who the last known leader was.
     *
     * <p>Deciding whether the leader <b>really</b> changed <b>must go by identity, not by the
     * event</b>. At cluster startup a node that takes the cluster port is already the leader
     * and adjudicates normally, but the "leader changed" event is only emitted after the quiet
     * period ({@code leaderQuietPeriodMs}, two seconds by default), and <b>every node receives
     * it</b>.
     *
     * <p>Treating that emission as a change of leader means every lock, permit, latch and
     * barrier handed out in those two seconds is <b>silently invalidated</b> -- the leader
     * clears its register and followers tell waiters "invalid". A usage like "take a lock at
     * startup to initialise" would believe it had the lock and quietly lose it, with nothing
     * reported.
     *
     * <p>So the leader's id is remembered here, and only a genuine change of id counts.
     */
    private volatile String lastLeaderId;

    /** The version this node has applied up to. On the leader it is also "the next number to
     *  issue, minus one". */
    private long applied;

    /** Updates received but not yet applicable, there being a gap before them, ordered by
     *  version. */
    private final TreeMap<Long, Pending> buffer = new TreeMap<>();

    /** When the gap appeared, used to decide whether to give up waiting and pull a full
     *  snapshot. 0 means there is no gap. */
    private long gapSince;

    /**
     * Guards the epoch, applied, buffer, version allocation and {@link CacheStore#apply}.
     *
     * <p>The critical section does memory operations only -- <b>nothing is ever sent over the
     * network inside the lock</b>; an update goes into {@link #outbox} for the broadcast thread
     * to send. That is the key to write throughput: the critical section is a few microseconds,
     * while one acknowledged multicast is several hundred.
     */
    private final Object stateLock = new Object();

    /** Updates awaiting broadcast. Bounded: a full queue means the replicas can no longer keep
     *  up, and having them pull a full snapshot beats gathering memory without bound. */
    private final BlockingQueue<CacheMessage.Entry> outbox;

    private final Map<Long, CompletableFuture<CacheMessage>> pendingWrites = new ConcurrentHashMap<>();
    private final Map<Long, Assembly> pendingSnapshots = new ConcurrentHashMap<>();
    private final AtomicLong requestIdGen = new AtomicLong();

    /**
     * The reply cache for write requests, which stops "a resend caused by a lost reply".
     *
     * <p>The TTL is twice the request timeout: resends all happen within that window, and
     * anything later cannot be the same call.
     */
    private final IdempotentRequestCache<CacheMessage> writeIdempotence;

    /** Only one full pull may be under way at a time. */
    private final AtomicBoolean syncing = new AtomicBoolean();

    /**
     * Whether the current full synchronisation <b>must be followed by another</b>.
     *
     * <p>Without it, a request arriving during a synchronisation is discarded by
     * {@link #syncing}'s guard. Discarding is normally right, there being no point pulling the
     * same data twice, but one case must be redone: the snapshot being pulled comes from <b>a
     * leader that has since been replaced</b>, and what comes back is either stale or never
     * arrives at all.
     */
    private final AtomicBoolean resyncAgain = new AtomicBoolean();

    /** Just taken over and fetching baseline data from the group. Writes are held back
     *  meanwhile, or the numbers issued would fall out of step with the baseline. */
    private volatile boolean adopting;

    /** The broadcast thread. A single thread makes the order sent the version order, which
     *  greatly reduces how long receivers wait on gaps. */
    private Thread broadcaster;

    /** Handles inbound write and snapshot requests. It must not occupy spreader's event
     *  dispatch thread. */
    private final ExecutorService inbound;

    /**
     * Devoted to pulling full snapshots.
     *
     * <p>A thread of its own, not shared with {@link #inbound}: a pull blocks waiting for the
     * peer's snapshot, for up to nine seconds, while that peer may itself be waiting for this
     * node's. Sharing a pool, a few mutually waiting pulls would fill it and nobody could
     * answer anybody.
     */
    private final ExecutorService syncExecutor;

    /**
     * The two scheduled-task pools; see {@link ExecutorServiceHolder}.
     *
     * <p>Renewal and cleanup run apart: cleanup may take hundreds of milliseconds, and one late
     * renewal releases a lock wrongly.
     */
    private final ExecutorServiceHolder executors;

    // Observability
    private final AtomicLong framesSent = new AtomicLong();
    private final AtomicLong opsSent = new AtomicLong();
    private final AtomicLong opsApplied = new AtomicLong();
    private final AtomicLong resyncCount = new AtomicLong();
    private final AtomicLong outboxOverflow = new AtomicLong();
    private final AtomicLong evicted = new AtomicLong();

    private final AtomicLong accessReported = new AtomicLong();

    /**
     * Disk persistence; null when it is not configured.
     *
     * <p>The disk is touched at two moments only: loaded once at startup, written once at
     * shutdown. Not one byte is written while running -- quite unlike "every write synchronises
     * to external storage", which turns a 0.0005ms local write into a network round trip.
     */
    private final CachePersistence persistence;

    private volatile boolean closed;

    /**
     * @param executors the scheduled-task pools. Spring's configuration passes the shared one
     *                  in, so the container manages the lifecycle and Actuator can see it
     */
    public CacheService(GossipCluster cluster, CacheOptions options, ExecutorServiceHolder executors) {
        this(cluster, options, executors, null);
    }

    /**
     * @param persistence disk persistence; {@code null} disables it
     */
    public CacheService(GossipCluster cluster, CacheOptions options, ExecutorServiceHolder executors,
                        CachePersistence persistence) {
        this.persistence = persistence;
        this.cluster = cluster;
        this.options = options;
        this.replicationName = options.replicationName();
        this.requestTimeoutMs = options.requestTimeoutMs();
        // The reply cache's window is twice the request timeout: resends all happen within it
        this.writeIdempotence = new IdempotentRequestCache<>(
                Math.max(2_000L, options.requestTimeoutMs() * 2), 100_000);
        this.retryIntervalMs = options.retryIntervalMs();
        this.gapTimeoutMs = options.gapTimeoutMs();
        this.maxBatchSize = options.maxBatchSize();
        this.maxBatchBytes = resolveMaxBatchBytes(cluster, options.maxBatchBytes());
        // The configured value is only a ceiling, and the transport caps it again: a UDP
        // datagram holds at most 65507 bytes while this setting defaults to 1MB -- without
        // narrowing it, a full synchronisation could not send a single chunk over UDP and a new
        // node would never catch up
        this.snapshotChunkBytes = Math.min(options.snapshotChunkBytes(), this.maxBatchBytes);
        this.outbox = new ArrayBlockingQueue<>(options.outboxCapacity());
        // What feeds it is spreader's dispatch thread, and a full queue can only discard --
        // pushing back would drag the whole dispatch path down. A discarded write request times
        // out at the sender and is retried, whereas blocking the dispatch thread is global
        this.inbound = executors.forCacheInbound();
        this.syncExecutor = executors.forCacheSync();
        this.executors = executors;
    }

    /**
     * A frame's byte cap.
     *
     * <p>A configured value is used as given; without one (0) it is derived from the transport
     * protocol:
     * <ul>
     *   <li><b>UDP</b> -- {@link Frames#MAX_DATAGRAM_BYTES} is the operating system's hard
     *       ceiling, less headroom for protocol headers and batch metadata</li>
     *   <li><b>TCP</b> -- a stream, capped by {@code maxMessageBytes} (8MB by default), of which
     *       a quarter is taken</li>
     * </ul>
     */
    private static int resolveMaxBatchBytes(GossipCluster cluster, int configured) {
        if (configured > 0) {
            return configured;
        }
        if (cluster.config().transportType() == TransportType.UDP) {
            // 5507 bytes are left for the UDP header, spreader's frame header and the batch
            // metadata, which is ample
            return Frames.MAX_DATAGRAM_BYTES - 5_507;
        }
        return Math.max(64 * 1024, cluster.config().maxMessageBytes() / 4);
    }

    public void start() {
        cluster.addListener(CHANNEL, this);
        lastLeaderId = cluster.leader() == null ? null : cluster.leader().id();
        if (cluster.isLeader()) {
            // The order matters: load the disk data first, then open the epoch.
            //
            // openNewEpoch() ends with announceEpoch(), broadcasting the new epoch, and a
            // follower seeing the change pulls a full synchronisation -- by which time the
            // leader already holds the disk data, so it flows to every replica along the
            // existing synchronisation path, without a line of extra distribution code.
            //
            // The other way round, opening the epoch before loading, a follower pulls an empty
            // snapshot and the disk data thereafter exists only on the leader, with the replicas
            // never agreeing
            loadFromDisk();
            // A cold start does not fetch a baseline -- the cluster has no data to begin with,
            // and fetching one would only wait out a snapshot timeout while every write is held
            // back
            openNewEpoch();
        }
        // Only LRU and LFU need access information; RANDOM and NONE switch it off, saving two
        // field writes on the read path
        store.trackAccess(options.needsAccessTracking());
        syncReadReporting();
        broadcaster = daemon("cache-broadcast").newThread(this::broadcastLoop);
        broadcaster.start();
        long tick = options.effectiveMaintenanceIntervalMs();
        executors.forMaintenance().scheduleWithFixedDelay(this::maintain, tick, tick, TimeUnit.MILLISECONDS);
        long reportInterval = options.accessReportIntervalMs();
        if (reportInterval > 0 && options.needsAccessTracking()) {
            executors.forMaintenance().scheduleWithFixedDelay(this::reportAccess,
                    reportInterval, reportInterval, TimeUnit.MILLISECONDS);
        }
        log.info("Cluster cache started: replication scope={}, write timeout={}ms, gap "
                        + "wait={}ms, expiry sweep={}ms, snapshot timeout={}ms, maintenance "
                        + "interval={}ms, batch cap={} updates / {} bytes, snapshot chunk={} "
                        + "bytes, queue capacity={}, inbound threads={}",
                replicationName == null ? "the whole cluster" : replicationName,
                requestTimeoutMs, gapTimeoutMs, options.sweepIntervalMs(),
                options.snapshotTimeoutMs(), tick, maxBatchSize, maxBatchBytes,
                snapshotChunkBytes, options.outboxCapacity(), options.inboundThreads());
        log.info("Cache eviction: policy={}, key cap={}, byte cap={}, samples={}, per-cycle "
                + "cap={}, access reporting={}ms",
                options.evictionPolicy(),
                options.maxKeys() == 0 ? "unlimited" : options.maxKeys(),
                options.maxBytes() == 0 ? "unlimited" : options.maxBytes(),
                options.evictionSamples(), options.evictionBatch(),
                options.accessReportIntervalMs());
        if (!holdsReplica()) {
            log.warn("This node (application {}) is outside the cache replication scope {}: it "
                    + "can write, but a local read finds nothing",
                    cluster.self().name(), replicationName);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        // Writing to disk comes first: once closed is set the write path starts throwing, and
        // after cluster.removeListener isLeader() may change too. Either would leave the dump
        // without the right data, or never reached at all
        dumpOnClose();

        closed = true;
        cluster.removeListener(this);
        // Neither executors nor the pools it hands out are closed: that is
        // ExecutorServiceHolder's business, and one service shutting down should not take
        // another's thread pools with it
        if (broadcaster != null) {
            broadcaster.interrupt();
        }
        pendingWrites.values().forEach(f -> f.completeExceptionally(
                new ProcessingCacheException("the cache service is closed")));
        pendingWrites.clear();
        pendingSnapshots.values().forEach(a -> a.done.completeExceptionally(
                new ProcessingCacheException("the cache service is closed")));
        pendingSnapshots.clear();
    }

    /**
     * Saves the data before shutting down. It comes first in close(), since the cluster
     * channel closes afterwards.
     *
     * <p>The decision about who writes lives in {@link #dumpToDisk()} and applies whoever
     * triggers it -- several instances on one machine point at the same file, and one extra
     * write is one more overwrite, regardless of whether the shutdown sequence or something
     * else called it.
     */
    private void dumpOnClose() {
        if (persistence != null) {
            dumpToDisk();
        }
    }

    public CacheStore store() {
        return store;
    }

    // ------------------------------------------------------------------
    // Writes: executed locally when this node is the leader, sent to it otherwise
    // ------------------------------------------------------------------

    /**
     * Executes one write.
     *
     * @throws ProcessingCacheException on misuse -- a type mismatch and the like, where
     *                                  retrying does not help -- or when the leader is
     *                                  temporarily unavailable, where it gives up after the
     *                                  timeout and retrying may help
     */
    public CacheStore.Result write(CacheOp op, String key, String field, byte[] value, long arg) {
        if (closed) {
            throw new ProcessingCacheException("the cache service is closed");
        }
        long deadline = System.currentTimeMillis() + requestTimeoutMs;
        String lastReason = "leadership is vacant";

        // The requestId is generated outside the loop: every retry below reuses it, so that the
        // leader recognises "a resend of the same request" rather than a new write.
        //
        // For operations such as INCR and ZINCRBY this is a matter of correctness: the request
        // arrived and the leader executed it, and only the reply was lost on the way back -- a
        // retry carrying a new id would increment a second time, with nothing reported.
        long requestId = requestIdGen.incrementAndGet();

        while (true) {
            Node leader = cluster.leader();
            if (leader != null) {
                try {
                    if (leader.id().equals(cluster.self().id())) {
                        if (adopting) {
                            // Just taken over and fetching baseline data from an instance of
                            // the group; numbering now would fall out of step with the old data
                            throw new RetryableException(
                                    "the leader is taking over the cache data");
                        }
                        // This node is the leader: the round trip is saved, by exactly the same
                        // logic as remotely
                        return applyAsLeader(op, key, field, value, arg);
                    }
                    return sendToLeader(requestId, leader, op, key, field, value, arg,
                            deadline - System.currentTimeMillis());
                } catch (RetryableException e) {
                    lastReason = e.getMessage();
                }
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new ProcessingCacheException("the cache write failed: " + lastReason);
            }
            try {
                // Leadership is vacant, or the peer answered "I am no longer the leader" --
                // wait, then read leader() again
                Thread.sleep(Math.min(retryIntervalMs, remaining));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProcessingCacheException("the cache write was interrupted");
            }
        }
    }

    /**
     * Executed on the leader: a version is allocated, the change lands locally, and it goes
     * into the broadcast queue.
     *
     * <p>The whole stretch is inside the lock, but <b>the lock holds memory operations only</b>
     * -- sending is the broadcast thread's business. A multicast inside the critical section
     * would press write throughput down to frames per second, three orders of magnitude below
     * memory operations.
     */
    private CacheStore.Result applyAsLeader(CacheOp op, String key, String field,
                                            byte[] value, long arg) {
        synchronized (stateLock) {
            if (!cluster.isLeader()) {
                // Leadership was lost at just this instant; retrying against the new leader is
                // all that is needed
                throw new RetryableException("this node is no longer the leader");
            }
            return executeLocked(op, key, field, value, arg).result();
        }
    }

    /**
     * One write's result.
     *
     * <p>The {@code version} must be carried out separately and <b>cannot be read from
     * {@code applied} afterwards</b>: the execution also triggers eviction, whose DELs push
     * {@code applied} further forward, and reading it then would give a number beyond this
     * operation's -- after which the requester, answered with it, would remain permanently
     * behind.
     */
    private record Executed(CacheStore.Result result, long version) {
    }

    /**
     * Executes one write on the leader: a version is allocated, the change lands locally, and
     * it goes into the broadcast queue.
     *
     * <p>The caller must hold {@link #stateLock} and have confirmed that this node is the
     * leader. Local writes and forwarded writes share this one stretch and <b>must not be split
     * into two</b> -- the slightest difference between two implementations would let "who
     * started it" affect the result.
     */
    private Executed executeLocked(CacheOp op, String key, String field,
                                   byte[] value, long arg) {
        evictIfExpiredLocked(op, key);

        // A pop must be resolved to a specific member on the leader first. Having each node pop
        // for itself would let the slightest difference in ordering pop different members,
        // forking the replicas on the spot with nothing reported
        if (op == CacheOp.ZPOPMIN || op == CacheOp.ZPOPMAX) {
            ScoredMember victim = store.zpeek(key, op == CacheOp.ZPOPMIN);
            if (victim == null) {
                // An empty set or an absent key: nothing happened, and no version is consumed
                return new Executed(CacheStore.Result.NONE, applied);
            }
            // What is really replicated is "remove this specific member", independent of who
            // computed it on which node
            long version = applied + 1;
            store.apply(CacheOp.ZREM, key, "", victim.member(), 0L);
            applied = version;
            opsApplied.incrementAndGet();
            enqueueLocked(version, CacheOp.ZREM, key, "", victim.member(), 0L);
            evictIfOverLimitLocked();
            return new Executed(new CacheStore.Result(victim.member(),
                    Double.doubleToRawLongBits(victim.score()), true), version);
        }

        long version = applied + 1;
        // apply may throw a type error. It comes before the increment, so a failure consumes no
        // version and broadcasts nothing
        CacheStore.Result result = store.apply(op, key, field, value, arg);
        applied = version;
        opsApplied.incrementAndGet();
        enqueueLocked(version, op, key, field, value, arg);
        evictIfOverLimitLocked();
        return new Executed(result, version);
    }

    /**
     * Issues a deletion first where a key has expired and its deletion has not been broadcast
     * yet.
     *
     * <p>Without this step, {@code setIfAbsent} would meet a key that is logically absent and
     * physically present and judge it taken, and {@code lpush} would append to an old list that
     * should have gone.
     */
    private void evictIfExpiredLocked(CacheOp op, String key) {
        if (op == CacheOp.CLEAR || key == null || !store.isExpired(key)) {
            return;
        }
        long version = applied + 1;
        store.apply(CacheOp.DEL, key, "", null, 0L);
        applied = version;
        enqueueLocked(version, CacheOp.DEL, key, "", null, 0L);
    }

    /**
     * Queues an update for broadcast.
     *
     * <p>A full queue does not block -- blocking would have the slowest replica drag the whole
     * write path down. It is discarded, and the receiver pulls a full snapshot on noticing the
     * version gap: the cost is one full transfer, which is far better than blocking the
     * leader.
     */
    private void enqueueLocked(long version, CacheOp op, String key, String field,
                               byte[] value, long arg) {
        CacheMessage.Entry entry =
                new CacheMessage.Entry(epoch, version, op, key, field, value, arg);
        if (!outbox.offer(entry)) {
            long n = outboxOverflow.incrementAndGet();
            if (n == 1 || n % 1000 == 0) {
                log.warn("The broadcast queue is full (occurrence {}); update v{} was discarded "
                        + "and receivers will catch up by full synchronisation", n, version);
            }
        }
    }

    /**
     * Whether this operation's result depends on the ordering, so that the leader must compute
     * the outcome and broadcast that.
     *
     * <p>For such an operation the request sent and the operation finally replicated are
     * <b>not the same one</b>, so the requester must not apply what it sent locally.
     */
    private static boolean isLeaderResolved(CacheOp op) {
        return op == CacheOp.ZPOPMIN || op == CacheOp.ZPOPMAX;
    }

    /**
     * Whether executing this operation a second time changes the result.
     *
     * <p>It serves only the fallback path where synchronisation does not arrive and the write
     * lands locally, and that path <b>must</b> ask the question: a fallback application does not
     * advance {@code applied}, so the broadcast arriving afterwards <b>executes the same
     * operation again</b>.
     *
     * <ul>
     *   <li>{@code SET} executed twice leaves the value unchanged -- safe</li>
     *   <li>{@code INCR} executed twice <b>increments once too often</b> -- with nothing
     *       reported, and by the time it is noticed there is no trail to follow</li>
     * </ul>
     *
     * <p>So for a non-idempotent operation it is better to leave the caller unable to read it
     * for a moment, until the broadcast arrives of its own accord, than to risk executing it
     * twice: reading a stale value is recoverable, a wrong count is not.
     */
    private static boolean isIdempotent(CacheOp op) {
        return switch (op) {
            // A wholesale overwrite or deletion: the result is the same however many times it
            // runs
            case SET, SET_IF_ABSENT, DEL, CLEAR, EXPIRE, PERSIST, SETBIT, HSET, HDEL, LTRIM,
                 ZADD, ZREM -> true;
            // Accumulating, pushing, popping, aggregating -- each execution changes the result
            default -> false;
        };
    }

    /**
     * Forwards a write request to the leader.
     *
     * @param requestId generated by the caller and <b>reused across retries</b>, so that the
     *                  leader recognises a resend. See
     *                  {@link com.chaconneai.openspreader.idempotence.IdempotentRequestCache}
     */
    private CacheStore.Result sendToLeader(long requestId, Node leader, CacheOp op, String key,
                                           String field, byte[] value, long arg, long waitMs) {
        if (waitMs <= 0) {
            throw new RetryableException("timed out waiting for the leader's reply");
        }
        CompletableFuture<CacheMessage> future = new CompletableFuture<>();
        pendingWrites.put(requestId, future);
        try {
            CacheMessage request = CacheMessage.write(requestId, op, key, field, value, arg);
            if (!cluster.unicastOn(CHANNEL, leader, request.encode())) {
                throw new RetryableException("the message could not reach the leader "
                        + leader.label());
            }
            CacheMessage response = future.get(waitMs, TimeUnit.MILLISECONDS);
            if (!response.success()) {
                // The leader refused explicitly: either "I am no longer the leader", where
                // retrying helps, or a type error, where it does not. The latter should not be
                // retried for nothing and is thrown straight out
                if (response.message().contains("not the leader")) {
                    throw new RetryableException(response.message());
                }
                throw new ProcessingCacheException(response.message());
            }
            // The reply carries this operation's version, which is applied locally at once, so
            // that "write, then read" is accurate. Note that what is written is the local value;
            // the message's value carries the execution's result.
            // A node holding no replica skips this: it receives no broadcasts, and applying
            // would only accumulate gaps.
            //
            // A pop skips it too: what the leader replicates is "remove this specific member"
            // rather than "pop one", and applying the original op locally would not line up.
            // Waiting for the broadcast is enough -- the member wanted is already in the reply,
            // and reading one's own write means nothing here
            if (holdsReplica() && !isLeaderResolved(op)) {
                synchronized (stateLock) {
                    applyOrderedLocked(response.epoch(), response.seq(),
                            op, key, field, value, arg);
                    // The step above may only have buffered it, because of a gap or a change of
                    // epoch -- and returning then would have the caller read an earlier value,
                    // or even null, immediately afterwards. Waiting for the local copy to catch
                    // up is what makes "read your own write" actually hold
                    if (response.epoch() >= epoch
                            && !awaitAppliedLocked(response.epoch(), response.seq())
                            && isIdempotent(op)) {
                        // The wait is over -- the write lands locally rather than leaving the
                        // caller to read null.
                        //
                        // This write has already succeeded on the leader, and the reply is the
                        // evidence. The local copy not seeing it is purely a full
                        // synchronisation that has not caught up; and synchronisation is
                        // asynchronous, so exceeding gapTimeoutMs under load is normal -- it
                        // appeared without fail in the full regression and could never be
                        // reproduced on its own, load changing the timing rather than the
                        // correctness.
                        //
                        // Returning silently would cost "the write succeeded and reading
                        // immediately afterwards gives null", breaking read-your-own-write,
                        // which is this cache's most basic promise.
                        //
                        // Applying directly is safe: applied and epoch are untouched, the gap
                        // state is preserved, and when the full synchronisation arrives
                        // store.restore() replaces the table wholesale -- this write is either
                        // already in the snapshot, the leader having executed it, or overwritten
                        // by the correct version. So the fallback affects only the window before
                        // the synchronisation arrives, which is exactly the window in which the
                        // caller reads what it just wrote
                        replayLocked(op, key, field, value, arg);
                    }
                }
            }
            return new CacheStore.Result(response.value(), response.numberResult(),
                    response.booleanResult());
        } catch (TimeoutException e) {
            throw new RetryableException("timed out waiting for the leader's reply");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessingCacheException("the cache write was interrupted");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new RetryableException(cause.toString());
        } finally {
            pendingWrites.remove(requestId);
        }
    }

    // ------------------------------------------------------------------
    // Broadcasting: batching that arises on its own
    // ------------------------------------------------------------------

    /**
     * The broadcast thread's main loop.
     *
     * <p>{@code take()} blocks for the first update, and having one it <b>waits no further</b>
     * and takes whatever else the queue holds. So: writing slowly, each update goes out on its
     * own at the lowest latency; writing quickly, the previous frame is still in flight, the
     * queue gathers of its own accord, and the next frame carries a batch. This is group
     * commit, and its merit is needing no "batching delay" parameter -- the load decides the
     * batch size.
     */
    private void broadcastLoop() {
        List<CacheMessage.Entry> batch = new ArrayList<>(maxBatchSize);
        while (!closed) {
            try {
                batch.clear();
                CacheMessage.Entry first = outbox.take();
                batch.add(first);
                int bytes = sizeOf(first);
                // Only one epoch is packed together: versions do not run continuously across
                // epochs and a receiver would take it for a gap. It is capped by bytes as well
                // -- a frame beyond the cap cannot be sent at all, and the replication stream is
                // strictly ordered by version, so one lost frame blocks everything after it
                while (batch.size() < maxBatchSize && bytes < maxBatchBytes) {
                    CacheMessage.Entry next = outbox.peek();
                    if (next == null || next.epoch() != first.epoch()) {
                        break;
                    }
                    batch.add(outbox.poll());
                    bytes += sizeOf(next);
                }
                sendBatch(first.epoch(), batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // A failed broadcast is not rolled back: the leader has already changed, and a
                // node that missed it pulls a full snapshot on detecting the gap
                log.debug("Broadcasting a cache update failed: {}", e.toString());
            }
        }
    }

    /**
     * Sends one batch of updates, <b>guaranteeing that no frame exceeds the cap</b>.
     *
     * <p>Batching estimated the size once through {@link #sizeOf}, but that is an estimate --
     * a key name's UTF-8 length and the protocol header's real overhead may both differ from
     * it. So the encoded size is <b>measured again</b>, and a batch that really exceeds the cap
     * is split in half and sent as several frames.
     *
     * <p>Splitting is safe: every update carries its own version and the receiver replays them
     * in version order, so how many travel in one frame makes no difference to the result.
     */
    private void sendBatch(long epoch, List<CacheMessage.Entry> batch) {
        if (batch.isEmpty()) {
            return;
        }
        CacheMessage.Entry first = batch.get(0);
        byte[] frame = batch.size() == 1
                ? CacheMessage.update(first.epoch(), first.seq(), first.op(), first.key(),
                first.field(), first.value(), first.arg()).encode()
                : CacheMessage.batch(epoch, batch).encode();

        if (frame.length > maxBatchBytes && batch.size() > 1) {
            // The estimate was wrong and the measurement exceeds the cap. Split in half, and
            // each half comes through this test again
            int mid = batch.size() / 2;
            sendBatch(epoch, List.copyOf(batch.subList(0, mid)));
            sendBatch(epoch, List.copyOf(batch.subList(mid, batch.size())));
            return;
        }
        if (frame.length > maxBatchBytes) {
            // One update alone exceeds the cap -- the value itself is too large and cannot be
            // split further. It is sent regardless: TCP carries it and UDP reports an error at
            // the transport layer. But a clear log line is left, or the symptom is nothing but
            // "why will this key not synchronise?"
            log.error("Update {} encodes to {} bytes, beyond a frame's cap of {} bytes, and "
                            + "will fail to send over UDP. Reduce the value, or use TCP",
                    first.key(), frame.length, maxBatchBytes);
        }
        // Sent only to instances within the replication scope; a null replicationName means
        // the whole cluster
        cluster.multicastOn(CHANNEL, replicationName, frame);
        framesSent.incrementAndGet();
        opsSent.addAndGet(batch.size());
    }

    /** How many bytes one update occupies in a frame, as an upper bound good enough for the
     *  purpose; a key name counts UTF-8's worst case of 3 bytes per character. */
    private static int sizeOf(CacheMessage.Entry e) {
        return 40
                + (e.key() == null ? 0 : e.key().length() * 3)
                + (e.field() == null ? 0 : e.field().length() * 3)
                + (e.value() == null ? 0 : e.value().length);
    }

    // ------------------------------------------------------------------
    // Inbound
    // ------------------------------------------------------------------

    @Override
    public void onPayload(Node sender, byte[] content) {
        CacheMessage msg = CacheMessage.decode(content);
        if (msg == null) {
            log.debug("An undecodable cache message arrived from {}", sender.label());
            return;
        }
        switch (msg.type()) {
            case RESPONSE -> {
                CompletableFuture<CacheMessage> f = pendingWrites.get(msg.requestId());
                if (f != null) {
                    f.complete(msg);
                    break;
                }
                // It may also be a refusal, "I cannot provide a snapshot" -- letting the
                // requester fail at once and find another source rather than waiting out its
                // timeout
                Assembly a = pendingSnapshots.get(msg.requestId());
                if (a != null) {
                    a.done.completeExceptionally(new ProcessingCacheException(msg.message()));
                }
            }
            case SNAPSHOT -> {
                Assembly a = pendingSnapshots.get(msg.requestId());
                if (a != null) {
                    a.add(msg);
                }
            }
            // A replay must keep the arrival order, so it happens on the dispatch thread --
            // store.apply is a matter of microseconds, and going asynchronous would scramble the
            // order and manufacture gaps for nothing
            case UPDATE -> applyOrdered(msg.epoch(), msg.seq(), msg.op(), msg.key(),
                    msg.field(), msg.value(), msg.arg());
            case BATCH -> applyBatch(msg, sender);
            case ACCESS -> {
                // Only the leader uses it: it refreshes the eviction information, producing no
                // version and changing no data
                if (cluster.isLeader()) {
                    List<String> keys = msg.decodeAccessKeys();
                    for (String k : keys) {
                        store.touch(k);
                    }
                    accessReported.addAndGet(keys.size());
                }
            }
            case EPOCH -> {
                if (msg.epoch() > epoch) {
                    resyncAsync("the leader announced epoch " + msg.epoch());
                }
            }
            // These two answer, and sending waits synchronously for an ACK, so they must not
            // occupy the dispatch thread
            case WRITE -> inbound.execute(() -> handleWrite(msg, sender));
            case SYNC -> inbound.execute(() -> handleSync(msg, sender));
        }
    }

    private void applyBatch(CacheMessage msg, Node sender) {
        List<CacheMessage.Entry> entries = msg.decodeEntries();
        if (entries == null) {
            // Applying half a batch would fork the replicas; discarding the batch and pulling a
            // full snapshot is safer
            log.warn("A batch of updates from {} failed to parse; falling back to full "
                    + "synchronisation", sender.label());
            resyncAsync("the batch message was corrupt");
            return;
        }
        // The whole batch is released under one lock: with a few hundred in a batch, the lock
        // operations saved are considerable
        synchronized (stateLock) {
            for (CacheMessage.Entry e : entries) {
                applyOrderedLocked(e.epoch(), e.seq(), e.op(), e.key(), e.field(), e.value(), e.arg());
            }
        }
    }

    /**
     * The leader executes one write request.
     *
     * <h2>The whole stretch is wrapped in the idempotence cache</h2>
     * The request arrived and was executed, and only the reply was lost on the way back -- the
     * caller resends, and what it saw was <b>exactly</b> "the request never arrived". For
     * {@code SET} that is of no consequence, but {@code INCR} and {@code ZINCRBY} executed
     * twice <b>give a wrong result</b>, with nothing reported.
     *
     * <p>So the reply is remembered under "originator plus requestId", and a resend returns the
     * previous one directly. The matching requirement is that the caller reuses the requestId
     * across retries; see {@link #write}.
     */
    private void handleWrite(CacheMessage msg, Node sender) {
        CacheMessage response;
        try {
            response = writeIdempotence.execute(sender.id(), msg.requestId(),
                    () -> executeWrite(msg));
        } catch (NotLeaderException e) {
            response = CacheMessage.fail(msg.requestId(), epoch,
                    "this node is not the leader");
        }
        reply(sender, response);
    }

    /** Really executes one write. {@link #writeIdempotence} guarantees it happens once. */
    private CacheMessage executeWrite(CacheMessage msg) {
        try {
            synchronized (stateLock) {
                if (!cluster.isLeader()) {
                    // The requester's view is out of date, so it looks for the leader again.
                    // This must not be cached as a final reply: by the time it has found the
                    // leader and returns, the answer will be different. So it is thrown -- the
                    // idempotence cache does not cache failures
                    throw new NotLeaderException();
                }
                Executed done = executeLocked(msg.op(), msg.key(), msg.field(),
                        msg.value(), msg.arg());
                return CacheMessage.ok(msg, epoch, done.version(), done.result());
            }
        } catch (NotLeaderException e) {
            throw e;
        } catch (ProcessingCacheException e) {
            // A business failure, a type mismatch and the like, is a settled conclusion and can
            // be cached as the reply -- resending the same request gives the same answer
            return CacheMessage.fail(msg.requestId(), epoch, e.getMessage());
        } catch (RuntimeException e) {
            log.warn("Handling a cache write request errored: {}", e.toString());
            return CacheMessage.fail(msg.requestId(), epoch,
                    "the leader failed to execute it: " + e);
        }
    }

    /**
     * The conclusion "I am not the leader" is <b>temporary</b> and must not be cached as a
     * final reply.
     *
     * <p>An exception rather than a failure reply is exactly what has the idempotence cache
     * treat this execution as failed and remove the entry, so that the next resend decides
     * afresh -- by which time this node may be the leader.
     */
    private static final class NotLeaderException extends RuntimeException {

        NotLeaderException() {
            super(null, null, false, false);
        }
    }

    /**
     * Provides a full snapshot.
     *
     * <p><b>It does not require being the leader</b>: any replica that has aligned can provide
     * one, because the versions in the snapshot use the leader's numbering, and the receiver
     * simply follows the leader's increments once it has installed it.
     *
     * <p>That has two uses: it spreads the snapshot load off the leader, and it rescues the
     * case where a newly promoted leader holds no data -- it can ask any instance of the group
     * for a copy first.
     */
    private void handleSync(CacheMessage msg, Node sender) {
        long snapshotEpoch;
        long snapshotSeq;
        List<byte[]> chunks;
        // The snapshot and the version must be taken together with no write in between, or what
        // the peer installs is a state out of step. This holds writes back briefly -- tens of
        // milliseconds with many keys. It happens only on a change of leader or a node joining,
        // which is acceptable
        synchronized (stateLock) {
            if (epoch == EPOCH_UNKNOWN) {
                reply(sender, CacheMessage.fail(msg.requestId(), epoch,
                        "this node has not aligned yet and has no baseline to offer"));
                return;
            }
            snapshotEpoch = epoch;
            snapshotSeq = applied;
            chunks = store.dump(snapshotChunkBytes);
        }
        for (int i = 0; i < chunks.size(); i++) {
            CacheMessage chunk = CacheMessage.snapshot(msg.requestId(), snapshotEpoch,
                    snapshotSeq, i, chunks.size(), chunks.get(i));
            if (!cluster.unicastOn(CHANNEL, sender, chunk.encode())) {
                log.warn("Failed to send snapshot chunk {}/{} to {}; it will time out and retry",
                        sender.label(), i + 1, chunks.size());
                return;
            }
        }
        log.info("Sent a full snapshot to {}: {} chunk(s), version={}", sender.label(),
                chunks.size(), snapshotSeq);
    }

    private void reply(Node target, CacheMessage response) {
        try {
            cluster.unicastOn(CHANNEL, target, response.encode());
        } catch (Exception e) {
            log.debug("Failed to send the cache reply back to {}: {}", target.label(),
                    e.toString());
        }
    }

    // ------------------------------------------------------------------
    // Replaying in version order
    // ------------------------------------------------------------------

    private void applyOrdered(long msgEpoch, long version, CacheOp op, String key,
                              String field, byte[] value, long arg) {
        synchronized (stateLock) {
            applyOrderedLocked(msgEpoch, version, op, key, field, value, arg);
        }
    }

    private void applyOrderedLocked(long msgEpoch, long version, CacheOp op, String key,
                                    String field, byte[] value, long arg) {
        if (msgEpoch < epoch) {
            // Sent by an old leader and void long since
            return;
        }
        if (msgEpoch > epoch) {
            // The leader has changed and the local versions do not line up with the new one's,
            // so a full snapshot must be pulled. This update is buffered first and released once
            // the snapshot is installed
            bufferLocked(version, new Pending(msgEpoch, op, key, field, value, arg));
            resyncAsync("the epoch changed from " + epoch + " to " + msgEpoch);
            return;
        }
        if (version <= applied) {
            // A duplicate: the write request's reply applied it once, and the broadcast then
            // delivered it again
            return;
        }
        if (version > applied + 1) {
            bufferLocked(version, new Pending(msgEpoch, op, key, field, value, arg));
            return;
        }
        if (!replayLocked(op, key, field, value, arg)) {
            return;
        }
        applied = version;
        drainLocked();
        // Wakes the threads waiting for their own write to land; see awaitAppliedLocked
        stateLock.notifyAll();
    }

    private void bufferLocked(long version, Pending pending) {
        if (buffer.isEmpty()) {
            gapSince = System.currentTimeMillis();
        }
        buffer.put(version, pending);
    }

    /** Releases, in turn, whatever in the buffer now joins on. */
    private void drainLocked() {
        Pending next;
        while ((next = buffer.remove(applied + 1)) != null) {
            if (next.epoch() != epoch) {
                continue;
            }
            if (!replayLocked(next.op(), next.key(), next.field(), next.value(), next.arg())) {
                break;
            }
            applied++;
        }
        if (buffer.isEmpty()) {
            gapSince = 0L;
        }
    }

    /**
     * Waits for the local copy to apply version {@code version}.
     *
     * <h2>Why a completed write still waits</h2>
     * A follower forwards a write to the leader and the reply carries a version. But that
     * version <b>may run ahead of what has been applied locally</b> -- the broadcasts in
     * between are still in flight.
     *
     * <p>Returning then would have the caller read <b>an earlier value</b> immediately
     * afterwards: measured with three nodes writing the same key in turn, 11 of 30 rounds read
     * data from two or three rounds before. And "you can read your own write at once" is this
     * cache's basic published semantics.
     *
     * <h2>Why the epoch is waited on too</h2>
     * A reply whose epoch is newer than the local one means the leader changed between this
     * node sending the request and receiving the answer. {@link #applyOrderedLocked} then takes
     * the {@code msgEpoch > epoch} branch: the value <b>only enters the buffer and never
     * reaches the store</b>, and a full synchronisation is scheduled.
     *
     * <p>This once waited only where {@code response.epoch() == epoch}, so the call on which the
     * epoch advanced returned successfully <b>without waiting at all</b> -- and a caller reading
     * at once found no such key in the local store and read <b>null</b>.
     *
     * <p>The symptom being null rather than a stale value led the investigation astray at the
     * time: it looked like "external storage is losing data", and had nothing to do with
     * external storage -- a change of epoch does the same in a purely in-memory configuration,
     * external storage merely lengthening the write path and making a change of epoch likelier
     * to fall between the write and the read. Reproducing it reliably takes the whole package:
     * only the earlier cases, starting and stopping clusters repeatedly, manufacture enough
     * changes of epoch.
     *
     * <h2>Why the reply cannot simply be written locally</h2>
     * That would break the ordering: the buffer may still hold an <b>earlier</b> write of the
     * same key, and filling the gap would overwrite the new value with the old -- a still harder
     * problem to track down.
     *
     * <h2>The cost of waiting is bounded</h2>
     * It waits only where <b>there really is a gap</b>, and blocks not once without one, since
     * {@code applied >= version} holds directly. The bound is {@code gapTimeoutMs} -- beyond it
     * the missing messages really are lost, a full pull is triggered, and waiting further would
     * achieve nothing.
     *
     * <p>A timeout <b>is not a failure</b> either: the write itself succeeded on the leader and
     * was broadcast, and this node simply cannot see it yet. Throwing would have the caller
     * believe the write failed, which is worse.
     *
     * <p>The caller must hold {@link #stateLock}.
     */
    private boolean awaitAppliedLocked(long targetEpoch, long version) {
        long deadline = System.currentTimeMillis() + gapTimeoutMs;
        // Two cases keep waiting: the epoch has not caught up, or the epoch matches and the
        // version has not. And epoch > targetEpoch means another epoch has passed, so this one
        // is void long since and waiting achieves nothing
        while (!closed
                && (epoch < targetEpoch || (epoch == targetEpoch && applied < version))) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                log.debug("Timed out waiting for the local copy to reach epoch {} version {} "
                        + "(currently epoch {} version {}); falling back locally",
                        targetEpoch, version, epoch, applied);
                return false;
            }
            try {
                // Waited in slices rather than one wait(remaining).
                //
                // Catching the epoch up depends on an asynchronous full synchronisation, and
                // that path breaks silently in two places: resyncAsync's syncing guard swallows
                // a concurrent request rather than queueing it, and pullFrom returns false when
                // UDP fails to deliver and does not retry itself.
                //
                // Once broken, the next attempt waits for maintain() to notice gapSince exceed
                // gapTimeoutMs -- and the wait here is bounded by gapTimeoutMs as well, so it
                // necessarily times out first and never sees it. Measured, the first write after
                // killing the leader hits this.
                //
                // So it gives it a push on waking: syncing being false means the previous round
                // has ended, whether it succeeded or failed, and an epoch still behind schedules
                // another. Scheduling twice is safe, the guard de-duplicating
                stateLock.wait(Math.min(remaining, RESYNC_NUDGE_INTERVAL_MS));
                if (epoch < targetEpoch) {
                    resyncAsync("waiting for epoch " + targetEpoch + " to catch up");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    /**
     * @return false when the replay failed, the replica is no longer trustworthy, and a full
     *         snapshot must be pulled
     */
    private boolean replayLocked(CacheOp op, String key, String field, byte[] value, long arg) {
        try {
            store.apply(op, key, field, value, arg);
            opsApplied.incrementAndGet();
            return true;
        } catch (RuntimeException e) {
            // Reaching here means the local copy and the leader already disagree -- an
            // operation turning a key into a list was missed, say. Releasing more would only
            // compound the error, so it starts again from scratch
            log.error("Replaying cache operation {} {} failed; the local copy may have forked "
                    + "and will be resynchronised: {}",
                    op, key, e.toString());
            resyncAsync("the replay failed");
            return false;
        }
    }

    // ------------------------------------------------------------------
    // Full synchronisation
    // ------------------------------------------------------------------

    private void resyncAsync(String reason) {
        if (closed || !holdsReplica()) {
            // A node outside the replication scope receives no broadcasts, and a full pull
            // means nothing -- it is stale the moment it arrives
            return;
        }
        if (!syncing.compareAndSet(false, true)) {
            // A round is already under way, but this request must not simply be discarded --
            // that round may be pulling a snapshot from an old leader. It is noted, and runs
            // again once that round ends
            resyncAgain.set(true);
            return;
        }
        syncExecutor.execute(() -> {
            try {
                doResync(reason);
            } catch (Exception e) {
                log.warn("Full synchronisation failed ({}): {}; retrying shortly", reason,
                        e.toString());
            } finally {
                syncing.set(false);
                if (resyncAgain.compareAndSet(true, false)) {
                    resyncAsync("another synchronisation was requested during the last one");
                }
            }
        });
    }

    private void doResync(String reason) throws Exception {
        Node leader = cluster.leader();
        if (leader == null) {
            log.debug("A full synchronisation is needed ({}) but leadership is vacant; waiting "
                    + "for the next round", reason);
            return;
        }
        if (leader.id().equals(cluster.self().id())) {
            // This node is the leader, its own copy is the baseline, and there is nothing to
            // pull
            return;
        }
        // The leader is preferred: its data is certainly the most recent
        pullFrom(leader, reason);
    }

    /**
     * Pulls a full snapshot from some node and installs it.
     *
     * @return whether it succeeded
     */
    private boolean pullFrom(Node source, String reason) throws InterruptedException {
        long requestId = requestIdGen.incrementAndGet();
        Assembly assembly = new Assembly();
        pendingSnapshots.put(requestId, assembly);
        try {
            if (!cluster.unicastOn(CHANNEL, source, CacheMessage.sync(requestId).encode())) {
                log.debug("The snapshot request to {} was not delivered", source.label());
                return false;
            }
            assembly.done.get(options.snapshotTimeoutMs(), TimeUnit.MILLISECONDS);
            List<byte[]> chunks = assembly.ordered();
            // Installing and updating the state must happen under one lock: an update slipping
            // in between would act on the old data and then be overwritten by the new applied,
            // leaving the replica wrong
            synchronized (stateLock) {
                store.restore(chunks);
                epoch = assembly.epoch;
                applied = assembly.seq;
                // Anything from an old epoch, or already in the snapshot, is worth nothing
                buffer.entrySet().removeIf(e -> e.getValue().epoch() != epoch || e.getKey() <= applied);
                drainLocked();
                // Wakes the write threads waiting in awaitAppliedLocked for the epoch to catch
                // up. Without this line, a write across a change of epoch could only return by
                // waiting out its timeout -- the value having been installed long before, and a
                // whole gapTimeoutMs wasted
                stateLock.notifyAll();
            }
            resyncCount.incrementAndGet();
            log.info("Full synchronisation complete ({}): source={}, epoch={}, version={}, "
                    + "keys={}",
                    reason, source.label(), assembly.epoch, assembly.seq, store.keyCount());
            return true;
        } catch (ExecutionException | TimeoutException e) {
            log.debug("Pulling a snapshot from {} failed: {}", source.label(),
                    e.getCause() == null ? e.toString() : e.getCause().toString());
            return false;
        } finally {
            pendingSnapshots.remove(requestId);
        }
    }

    /**
     * Having just taken over without a replica in hand, fetches one from an instance of the
     * group.
     *
     * <p>When this arises: {@code applicationName} confines the replicas to one application
     * while the cluster leader happens to be an instance of <b>another</b> -- it has never
     * received a broadcast. Opening a new epoch on empty data then would lose the whole cache
     * outright, and every replica would dutifully clear itself along with it.
     */
    private void adoptBeforeLeading() {
        List<Node> candidates = new ArrayList<>(
                replicationName == null ? cluster.members() : cluster.membersOf(replicationName));
        candidates.removeIf(n -> n.id().equals(cluster.self().id()));
        if (candidates.isEmpty()) {
            log.info("Taking over the cache with no other instance in the group; starting from "
                    + "empty");
            return;
        }
        long deadline = System.currentTimeMillis() + options.snapshotTimeoutMs();
        for (Node candidate : candidates) {
            if (System.currentTimeMillis() >= deadline) {
                log.warn("Fetching a baseline for the takeover has exceeded {}ms; the remaining "
                        + "instances will not be tried", options.snapshotTimeoutMs());
                break;
            }
            try {
                if (pullFrom(candidate, "fetching a baseline before taking over")) {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        log.warn("Fetching a baseline failed against all {} instance(s) of the group while "
                + "taking over the cache; starting from empty", candidates.size());
    }

    /** A snapshot arrives in chunks, and this joins them together. */
    private static final class Assembly {

        final CompletableFuture<Void> done = new CompletableFuture<>();
        final Map<Integer, byte[]> chunks = new ConcurrentHashMap<>();
        volatile int total = -1;
        volatile long epoch;
        volatile long seq;

        void add(CacheMessage msg) {
            total = (int) msg.extra();
            epoch = msg.epoch();
            seq = msg.seq();
            chunks.put((int) msg.arg(), msg.value() == null ? new byte[0] : msg.value());
            if (chunks.size() >= total) {
                done.complete(null);
            }
        }

        List<byte[]> ordered() {
            List<byte[]> out = new ArrayList<>(chunks.size());
            for (int i = 0; i < chunks.size(); i++) {
                byte[] c = chunks.get(i);
                if (c == null) {
                    throw new ProcessingCacheException("the snapshot is missing chunk " + i);
                }
                out.add(c);
            }
            return out;
        }
    }

    // ------------------------------------------------------------------
    // Background maintenance
    // ------------------------------------------------------------------

    private void maintain() {
        if (closed) {
            return;
        }
        try {
            long now = System.currentTimeMillis();
            if (!cluster.isLeader()) {
                // Never aligned with any leader -- just started, or the leader had not been
                // chosen at the last pull
                if (epoch == EPOCH_UNKNOWN && cluster.leader() != null) {
                    resyncAsync("the first synchronisation");
                    return;
                }
                // The gap has been waited on too long, and the missing update is most likely
                // really lost
                long since;
                synchronized (stateLock) {
                    since = gapSince;
                }
                if (since > 0 && now - since > gapTimeoutMs) {
                    resyncAsync("the version gap exceeded " + gapTimeoutMs + "ms");
                }
                return;
            }
            sweepExpired(now);
        } catch (Exception e) {
            log.debug("The cache's background maintenance errored: {}", e.toString());
        }
    }

    /**
     * The leader sweeps expired keys.
     *
     * <p>A deletion is broadcast as <b>an ordinary write</b> rather than decided by each node:
     * having each delete by its own clock would leave the deletions at different instants, and
     * subsequent operations on that key would act on different states on different nodes,
     * forking the replicas.
     */
    private void sweepExpired(long now) {
        List<String> expired = store.expiredKeys(now);
        if (expired.isEmpty()) {
            return;
        }
        int swept = 0;
        // Locked in batches rather than holding the lock for the whole scan, so that many
        // expired keys do not block the write path
        for (String key : expired) {
            synchronized (stateLock) {
                if (!cluster.isLeader() || !store.isExpired(key)) {
                    continue;
                }
                long version = applied + 1;
                store.apply(CacheOp.DEL, key, "", null, 0L);
                applied = version;
                enqueueLocked(version, CacheOp.DEL, key, "", null, 0L);
                swept++;
            }
        }
        if (swept > 0) {
            log.debug("Swept {} expired key(s)", swept);
        }
    }

    // ------------------------------------------------------------------
    // Eviction
    // ------------------------------------------------------------------

    /**
     * Removes a few keys once a cap is exceeded. <b>Only the leader reaches here</b>, and the
     * state lock is already held.
     *
     * <p>A deletion goes out as <b>an ordinary write</b>: a version is allocated, it lands
     * locally, and it enters the broadcast queue. So other nodes receive the leader's eviction
     * decisions rather than making their own -- the sampling being random, each node's decisions
     * would remove entirely different keys and the replicas would fork at once.
     *
     * <p>A round is capped ({@code evictionBatch}): where the cap is greatly overshot, removing
     * tens of thousands here would block the write path and flood the broadcast queue, so it is
     * spread over several rounds. The capacity is slightly exceeded meanwhile, which is of no
     * consequence for a cache.
     */
    private void evictIfOverLimitLocked() {
        if (!options.hasLimit() || options.evictionPolicy() == EvictionPolicy.NONE) {
            return;
        }
        int budget = options.evictionBatch();
        int removed = 0;
        while (removed < budget && overLimit()) {
            String victim = store.pickEvictionCandidate(
                    options.evictionPolicy(), options.evictionSamples());
            if (victim == null) {
                break;
            }
            long version = applied + 1;
            CacheStore.Result r = store.apply(CacheOp.DEL, victim, "", null, 0L);
            if (!r.flag()) {
                // The sampled key was just removed by another operation; another is taken, and
                // no version is consumed
                continue;
            }
            applied = version;
            enqueueLocked(version, CacheOp.DEL, victim, "", null, 0L);
            removed++;
        }
        if (removed > 0) {
            long total = evicted.addAndGet(removed);
            if (removed >= budget) {
                log.warn("This round hit its cap of {} evictions without reaching the limits "
                        + "(keys={}, bytes~{}); it continues next round, {} evicted in total",
                        budget, store.keyCount(), store.approxBytes(), total);
            } else {
                log.debug("Evicted {} key(s); now keys={} bytes~{}, {} in total",
                        removed, store.keyCount(), store.approxBytes(), total);
            }
        }
    }

    private boolean overLimit() {
        long maxKeys = options.maxKeys();
        long maxBytes = options.maxBytes();
        return (maxKeys > 0 && store.keyCount() > maxKeys)
                || (maxBytes > 0 && store.approxBytes() > maxBytes);
    }

    /**
     * Reports the keys read this round to the leader.
     *
     * <p>The leader does not report -- its own reads are recorded on the values directly. Nor
     * does a node holding no replica: it has read nothing at all.
     */
    private void reportAccess() {
        if (closed || cluster.isLeader() || !holdsReplica()) {
            return;
        }
        try {
            Node leader = cluster.leader();
            if (leader == null) {
                return;
            }
            Set<String> keys = store.drainRecentReads();
            if (keys.isEmpty()) {
                return;
            }
            cluster.unicastOn(CHANNEL, leader, CacheMessage.access(keys).encode());
        } catch (Exception e) {
            log.debug("Reporting read accesses failed: {}", e.toString());
        }
    }

    // ------------------------------------------------------------------
    // Cluster events
    // ------------------------------------------------------------------

    @Override
    public void onClusterJoined(Node self, boolean alone) {
        if (alone || cluster.isLeader()) {
            if (epoch != EPOCH_UNKNOWN) {
                // start() has already opened an epoch, and opening another would have the
                // followers pull a full snapshot for nothing
                return;
            }
            lastLeaderId = self.id();
            openNewEpoch();
        } else {
            syncReadReporting();
            resyncAsync("joining the cluster");
        }
    }

    @Override
    public void onLeaderChanged(Node previous, Node current, boolean selfIsLeader) {
        String currentLeaderId = current == null ? null : current.id();
        if (Objects.equals(currentLeaderId, lastLeaderId)) {
            // The leader has not changed; this is only the notification emitted after the quiet
            // period. Opening a new epoch would have every follower pull a full snapshot for
            // nothing, and the writes in between would meet "taking over"
            return;
        }
        lastLeaderId = currentLeaderId;
        // The leader has changed, and the snapshots requested from the old one will never
        // arrive.
        //
        // Without cancelling them, the requester waits out snapshotTimeoutMs, 10 seconds by
        // default -- and syncing stays true throughout, so resyncAsync's de-duplicating guard
        // swallows every subsequent scheduling, including the "the leader changed" one below.
        // The result is a node waiting ten seconds after the old leader's death before it begins
        // synchronising with the new one.
        //
        // What that costs the application: for those ten seconds every write on that node
        // "succeeds and cannot be read" -- the write path waits for the local copy to reach the
        // epoch, and the epoch never catches up.
        cancelPendingSnapshots("the leader has changed");
        if (selfIsLeader) {
            becomeLeader();
        } else if (current != null) {
            syncReadReporting();
            resyncAsync("the leader changed");
        }
    }

    /** Cancels every snapshot request in flight, so the synchronisation threads stuck waiting
     *  return at once and retry. */
    private void cancelPendingSnapshots(String reason) {
        if (pendingSnapshots.isEmpty()) {
            return;
        }
        ProcessingCacheException cause = new ProcessingCacheException(
                "the snapshot request was cancelled: " + reason);
        pendingSnapshots.values().forEach(a -> a.done.completeExceptionally(cause));
        pendingSnapshots.clear();
    }

    @Override
    public void onLeaderBack(Node node) {
        if (!cluster.isLeader()) {
            resyncAsync("the leader is back");
        }
    }

    /**
     * Becomes the leader.
     *
     * <p>A new epoch number is taken, versions begin from 0 again, and <b>the local data is kept
     * as it is</b> as the new baseline. Not clearing it is deliberate: the new leader was
     * already a member holding a complete copy, and clearing would lose the whole cache
     * outright. What is lost is only the few operations the old leader executed without
     * broadcasting to this node, which is acceptable for a cache.
     *
     * <p>The one exception is a new leader that has <b>never</b> held a replica --
     * applicationName is set and it belongs to another application. It then fetches a baseline
     * from the group before opening the epoch, and the writes in between receive "taking over"
     * and retry of their own accord.
     */
    private void becomeLeader() {
        if (needsAdoption() && !adopting) {
            adopting = true;
            syncExecutor.execute(() -> {
                try {
                    adoptBeforeLeading();
                } finally {
                    adopting = false;
                    openNewEpoch();
                }
            });
            return;
        }
        openNewEpoch();
    }

    /**
     * Whether a baseline must be fetched before taking over.
     *
     * <p>Exactly one case needs it: {@code applicationName} confines the replicas to one
     * application and this node <b>does not belong</b> to it -- it has never received a
     * broadcast and holds nothing. Opening a new epoch on empty data would flatten the whole
     * cache.
     *
     * <p>No other case fetches one: a node within the replication scope already holds the data;
     * and at a cold start the cluster has no data to fetch, so fetching would only wait out a
     * snapshot timeout while every write is turned back with "taking over" -- measured at 30
     * seconds of unavailable writes.
     */
    private boolean needsAdoption() {
        return replicationName != null
                && !replicationName.equals(cluster.self().name())
                && epoch == EPOCH_UNKNOWN;
    }

    /** Only a follower gathers read accesses to report; the leader can see its own reads. */
    private void syncReadReporting() {
        boolean shouldReport = options.accessReportIntervalMs() > 0
                && options.needsAccessTracking()
                && !cluster.isLeader() && holdsReplica();
        if (shouldReport) {
            store.enableReadReporting(options.accessReportSampleRate(), options.accessReportMaxKeys());
        } else {
            store.disableReadReporting();
        }
    }

    /**
     * Whether this node writes to disk: <b>one representative per machine</b>.
     *
     * <h2>Two requirements must hold together</h2>
     * <ul>
     *   <li><b>Every machine needs a file</b> -- who leads at the next startup is contended for
     *       and has nothing to do with who led last. With only the old leader's machine holding
     *       a file, another machine starting first would find no data</li>
     *   <li><b>One machine writes one file</b> -- several instances on one machine point at the
     *       same path by default, and all of them writing is mutual overwriting, with whoever
     *       shuts down last surviving, which is arbitrary</li>
     * </ul>
     *
     * <p>So neither "everyone writes" nor "only the leader writes" is right. Real deployments
     * are often mixed: A and B on one machine and C on another, where two files should be
     * written -- one of A or B, plus C.
     *
     * <h2>How the representative is chosen</h2>
     * Among the nodes on one machine, <b>the smallest node id</b>. An id is unique and stable
     * across the cluster, so every node computing independently reaches the same answer, with
     * nothing to negotiate.
     *
     * <p>Being the leader writes directly -- the leader's data is the baseline, and having it
     * do so makes more sense than ordering by id.
     *
     * <p>Containers are not mistaken for one machine: each has its own filesystem, and their
     * hosts are different IPs.
     */
    private boolean shouldDumpOnThisHost() {
        Node self = cluster.self();
        if (cluster.isLeader()) {
            return true;
        }
        String host = self.host();
        // Among the nodes on one machine, the smallest id is responsible. The leader has
        // already returned above, so it is excluded here -- otherwise both the leader and the
        // smallest id would write
        return cluster.members().stream()
                .filter(n -> host.equals(n.host()))
                .filter(n -> !n.id().equals(leaderIdOrNull()))
                .map(Node::id)
                .min(String::compareTo)
                .map(id -> id.equals(self.id()))
                .orElse(true);
    }

    private String leaderIdOrNull() {
        Node leader = cluster.leader();
        return leader == null ? null : leader.id();
    }

    /**
     * Loads the disk data at startup. <b>Only the leader does this.</b>
     *
     * <h2>Why only the leader loads</h2>
     * A follower loading would fork the replicas outright: it holds its own file while the
     * leader holds the leader's, the two differ, and each believes itself correct. And the
     * cache's consistency rests on "the leader orders and the replicas replay" -- a follower
     * conjuring up data that never passed through the leader breaks that premise.
     *
     * <p>What a follower should do is wait for the leader to broadcast a new epoch and then
     * pull a full snapshot through the ordinary path. What it receives is then certainly what
     * the leader endorses.
     *
     * <h2>The file is deleted after a successful load</h2>
     * Keeping it means that a startup after a shutdown that was <b>not</b> graceful -- kill -9,
     * a power cut -- loads the data from the run before last, which looks entirely normal with
     * nothing to suggest the snapshot is stale. An empty cache is better than data of uncertain
     * age.
     */
    private void loadFromDisk() {
        if (persistence == null) {
            return;
        }
        CachePersistence.Loaded loaded = persistence.load();
        if (loaded == null) {
            return;
        }
        try {
            synchronized (stateLock) {
                // true: the file stores absolute expiry instants. Loading compares them with
                // the current instant, and however long the file sat on disk makes no difference
                // -- anything expired is discarded inside restore
                store.restore(loaded.chunks(), true);
            }
            log.info("Loaded the cache from disk: file={}, keys when written={}, keys after "
                            + "loading={}, {}s since it was written; keys that expired meanwhile "
                            + "were discarded",
                    persistence.file(), loaded.keyCount(), store.keyCount(),
                    loaded.elapsedMillis() / 1000);
            persistence.delete();
        } catch (RuntimeException e) {
            log.warn("Loading the cache from disk failed; starting with an empty cache: {}",
                    e.toString());
        }
    }

    /**
     * Writes the whole cache to disk. <b>Every node writes, and only the leader reads.</b>
     *
     * <p>Spring triggers it at container shutdown through {@code destroyMethod = "close"}, and
     * it may also be called directly -- to save a copy by hand before a rolling upgrade, say.
     *
     * <h2>Why writing does not distinguish leader from follower</h2>
     * At the next startup <b>leadership is contended for</b> -- whoever takes the cluster port
     * first decides -- with nothing to do with who led last. Were only the leader to write, a
     * different node starting first next time would hold no file, the whole cache would be lost
     * outright, and the other nodes' files would lie on disk unread.
     *
     * <p>Every node holds a complete copy with identical contents, so each writing its own
     * conflicts with nothing.
     *
     * <h2>Reading, however, must distinguish them</h2>
     * See {@link #loadFromDisk()}: only the leader loads, and a follower takes its data through
     * ordinary full synchronisation. Were all of them to load, the several files need not agree,
     * their nodes having shut down at different moments -- and the cache's consistency rests on
     * exactly "the leader orders and the replicas replay".
     *
     * <h2>Several instances on one machine need separate paths</h2>
     * The default path carries no instance identity, so several instances on one machine write
     * the same file and overwrite each other. In production each node has its own machine and
     * this does not arise; running several instances locally for debugging, separate them with
     * {@code spring.spreader.multiprocessing.cache.persistent-file}.
     *
     * @return how many bytes were written, or -1 when persistence is not enabled
     */
    public long dumpToDisk() {
        if (persistence == null) {
            return -1L;
        }
        if (!shouldDumpOnThisHost()) {
            log.debug("Another node on this machine is responsible for writing to disk; "
                    + "skipping, so as not to overwrite the same file");
            return -1L;
        }
        try {
            List<byte[]> chunks;
            int keys;
            // The snapshot is taken inside the state lock, so no write slips in -- otherwise
            // what reaches disk would be half old and half new
            synchronized (stateLock) {
                keys = store.keyCount();
                // true: TTLs are stored as absolute expiry instants; see
                // CacheStore.dump(int, boolean)
                chunks = store.dump(4 * 1024 * 1024, true);
            }
            long bytes = persistence.dump(chunks, keys);
            log.info("The cache was written to disk: file={}, keys={}, size={} KB, this node is "
                    + "the leader={}",
                    persistence.file(), keys, bytes / 1024, cluster.isLeader());
            return bytes;
        } catch (IOException | RuntimeException e) {
            // A failed write must not hang the shutdown -- that would stop the container from
            // stopping, ending in a SIGKILL, which is far worse than losing a cache
            log.warn("Writing the cache to disk failed; the shutdown is unaffected: {}",
                    e.toString());
            return -1L;
        }
    }

    private void openNewEpoch() {
        synchronized (stateLock) {
            epoch = System.currentTimeMillis();
            applied = 0L;
            buffer.clear();
            gapSince = 0L;
            // Updates the previous term never finished sending; their versions mean nothing
            // under the new epoch
            outbox.clear();
        }
        syncReadReporting();
        log.info("This node became the cache leader: epoch={}, keys={}", epoch,
                store.keyCount());
        announceEpoch();
    }

    /**
     * Announces the new epoch.
     *
     * <p>Sent asynchronously; it must not occupy its caller's thread -- {@code becomeLeader} may
     * be reached on spreader's event dispatch thread, which is a single serial channel, and
     * blocking it would drag every cluster event notification down.
     */
    private void announceEpoch() {
        if (closed) {
            return;
        }
        long current = epoch;
        long version = appliedVersion();
        inbound.execute(() -> {
            try {
                cluster.multicastOn(CHANNEL, replicationName,
                        CacheMessage.epochAnnounce(current, version).encode());
            } catch (Exception e) {
                log.debug("Announcing the epoch failed: {}", e.toString());
            }
        });
    }

    /**
     * A new member has joined, and is told the epoch.
     *
     * <p>It may carry data from an old epoch -- a snapshot pulled from elsewhere, or what
     * survived a restart -- and realigns on receiving the announcement.
     */
    @Override
    public void onNodeJoined(Node node) {
        if (cluster.isLeader()) {
            announceEpoch();
        }
    }

    /**
     * Whether this node holds a replica.
     *
     * <p>Without {@code applicationName} it is everyone; with it, the application's instances
     * plus the leader -- which executes every write and therefore necessarily holds a full
     * copy.
     */
    private boolean holdsReplica() {
        return replicationName == null
                || replicationName.equals(cluster.self().name())
                || cluster.isLeader();
    }

    // ------------------------------------------------------------------
    // Troubleshooting and observability
    // ------------------------------------------------------------------

    public long epoch() {
        return epoch;
    }

    public long appliedVersion() {
        synchronized (stateLock) {
            return applied;
        }
    }

    /** How many updates are backed up behind a gap; normally 0. */
    public int bufferedUpdates() {
        synchronized (stateLock) {
            return buffer.size();
        }
    }

    public boolean isSyncing() {
        return syncing.get();
    }

    /**
     * Observability figures.
     *
     * <p>{@code opsPerFrame} shows the batching directly: near 1 means writing is slow and each
     * update goes out on its own, and markedly above 1 means the batching is working.
     */
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        long frames = framesSent.get();
        long ops = opsSent.get();
        m.put("leader", cluster.isLeader());
        m.put("replicationName", replicationName == null ? "*" : replicationName);
        m.put("holdsReplica", holdsReplica());
        m.put("epoch", epoch);
        m.put("appliedVersion", appliedVersion());
        m.put("keyCount", store.keyCount());
        m.put("approxBytes", store.approxBytes());
        m.put("maxKeys", options.maxKeys());
        m.put("maxBytes", options.maxBytes());
        m.put("evictionPolicy", options.evictionPolicy().name());
        m.put("evicted", evicted.get());
        m.put("accessReported", accessReported.get());
        m.put("bufferedUpdates", bufferedUpdates());
        m.put("outboxDepth", outbox.size());
        m.put("outboxOverflow", outboxOverflow.get());
        m.put("framesSent", frames);
        m.put("opsSent", ops);
        m.put("opsPerFrame", frames == 0 ? 0.0 : Math.round(ops * 100.0 / frames) / 100.0);
        m.put("opsApplied", opsApplied.get());
        m.put("resyncCount", resyncCount.get());
        m.put("syncing", syncing.get());
        return m;
    }

    /** One update received but not yet applicable. */
    private record Pending(long epoch, CacheOp op, String key, String field,
                           byte[] value, long arg) {
    }

    /** An internal signal: this attempt failed, but another leader or another moment may
     *  succeed. It is never thrown outward. */
    private static final class RetryableException extends RuntimeException {

        RetryableException(String message) {
            super(message, null, false, false);
        }
    }

    private static ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }
}
