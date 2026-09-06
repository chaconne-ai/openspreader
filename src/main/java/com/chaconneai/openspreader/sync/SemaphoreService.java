package com.chaconneai.openspreader.sync;

import com.chaconneai.openspreader.MultiProcessingService;
import com.chaconneai.spreader.GossipCluster;
import com.chaconneai.spreader.Node;
import com.chaconneai.openspreader.concurrent.ExecutorServiceHolder;
import com.chaconneai.openspreader.cluster.SelfRegisteringListener;
import com.chaconneai.spreader.event.BufferedGossipListener;
import com.chaconneai.openspreader.idempotence.IdempotentRequestCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The distributed semaphore's core. Its structure matches {@link MutexService} exactly:
 * requests converge on the leader for adjudication, replies return the same way, and it uses a
 * channel of its own ({@link #CHANNEL}) so as not to disturb the application.
 *
 * <h2>Three differences from a lock</h2>
 * <ol>
 *   <li><b>It counts per holder.</b> A lock records only who holds it, while a semaphore
 *       records how many each holder has; see {@link PermitRegistry}.</li>
 *   <li><b>The local layer is a semaphore rather than a mutex.</b> A lock admits one thread
 *       within a process, a semaphore admits N. See {@link Gate} below.</li>
 *   <li><b>It is not reentrant.</b> One thread acquiring twice holds two permits and must
 *       return two. That is {@code java.util.concurrent.Semaphore}'s meaning, and a
 *       semaphore's meaning to begin with.</li>
 * </ol>
 *
 * <h2>A change of leader must return the local permits</h2>
 * This is a trap peculiar to semaphores: a change of leader takes the register with it and
 * invalidates every permit this node holds. But <b>the permits in the local semaphore are
 * still in the "taken" state</b> -- clearing only the remote record leaves this process
 * permanently short, shorter with each change of leader, until nothing can be acquired at all.
 * So invalidation must restore the local permits by the number held; see
 * {@link Gate#reset()}.
 *
 * <h2>The safety boundary</h2>
 * The same as a lock's: its strength does not exceed "there is one leader". Under a
 * cross-machine network partition each side may have a leader admitting by its own register,
 * and the total admitted exceeds the configured number. It suits rate limiting and bounding
 * concurrency, where letting a few extra through only makes things slower; do not use it to
 * guarantee a hard "never more than N".
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class SemaphoreService extends BufferedGossipListener
        implements MultiProcessingService, SelfRegisteringListener {

    private static final Logger log = LoggerFactory.getLogger(SemaphoreService.class);

    /** How many times one request is resent at most. See {@link #exchange}. */
    private static final int RESEND_ATTEMPTS = 2;

    /** The semaphore protocol's own channel, separate from locks and from the application. */
    public static final String CHANNEL = "spreader.semaphore";

    private final GossipCluster cluster;
    private final long leaseMs;
    private final long requestTimeoutMs;
    private final long retryIntervalMs;

    /** Used only while this node is the leader. */
    private final PermitRegistry registry = new PermitRegistry();

    /** This node's local gate and held count for each semaphore. */
    private final Map<String, Gate> gates = new ConcurrentHashMap<>();

    /** Requests awaiting a reply. */
    private final Map<Long, CompletableFuture<SemaphoreMessage>> pending = new ConcurrentHashMap<>();

    private final AtomicLong requestIdGen = new AtomicLong();

    // Runtime statistics
    private final AtomicLong acquired = new AtomicLong();
    private final AtomicLong localBlocked = new AtomicLong();
    private final AtomicLong remoteDenied = new AtomicLong();
    private final AtomicLong acquireTimeouts = new AtomicLong();
    private final AtomicLong released = new AtomicLong();
    private final AtomicLong staleReleases = new AtomicLong();
    private final AtomicLong waitMillis = new AtomicLong();

    /** The leader-side reply cache, so a resend gets the previous reply rather than another
     *  permit. */
    private final IdempotentRequestCache<SemaphoreMessage> requestIdempotence =
            new IdempotentRequestCache<>(30_000L, 10_000);

    /** The leader epoch, meaning what {@link MutexService#epoch()} means. */
    private volatile long epoch;
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
    /**
     * The two scheduled-task pools; see {@link ExecutorServiceHolder}.
     *
     * <p>Renewal and cleanup run apart: cleanup may take hundreds of milliseconds, and one late
     * renewal releases a lock wrongly.
     */
    private final ExecutorServiceHolder executors;

    private volatile boolean closed;

    /**
     * @param executors the scheduled-task pools. Spring's configuration passes the shared one
     *                  in, so the container manages the lifecycle and Actuator can see it
     */
    public SemaphoreService(GossipCluster cluster, long leaseMs, long requestTimeoutMs,
                            long retryIntervalMs, ExecutorServiceHolder executors) {
        // Requests are buffered and replies pass straight through; see shouldBuffer. One
        // consumer thread: the register has to be serial anyway, more threads would only add
        // lock contention, and serialising also handles requests in arrival order
        super("semaphore", 2048, 1, LoggerFactory.getLogger("spreader.semaphore"));
        this.cluster = cluster;
        this.leaseMs = leaseMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryIntervalMs = retryIntervalMs;
        this.executors = executors;
    }

    public void start() {
        // The consumer thread starts before subscribing: the other way round, messages
        // arriving after the subscription would be discarded as "not started yet"
        startDispatch();
        cluster.addListener(CHANNEL, this);
        lastLeaderId = cluster.leader() == null ? null : cluster.leader().id();
        if (cluster.isLeader()) {
            epoch = System.currentTimeMillis();
        }
        long renewInterval = Math.max(200L, leaseMs / 3);
        executors.forRenewal().scheduleWithFixedDelay(this::renewAll, renewInterval, renewInterval, TimeUnit.MILLISECONDS);
        executors.forMaintenance().scheduleWithFixedDelay(this::evictExpired, leaseMs, leaseMs, TimeUnit.MILLISECONDS);
        log.info("Distributed semaphore service started: lease={}ms, request timeout={}ms",
                leaseMs, requestTimeoutMs);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        cluster.removeListener(this);
        // Held permits go back, so nobody has to wait out a lease
        for (Map.Entry<String, Gate> e : gates.entrySet()) {
            int count = e.getValue().held.get();
            for (int i = 0; i < count; i++) {
                try {
                    release(e.getKey());
                } catch (Exception ex) {
                    log.debug("Failed to return permit {} while shutting down: {}",
                            e.getKey(), ex.toString());
                    break;
                }
            }
        }
        // executors is not closed: it belongs to the container, and one service shutting down
        // should not take another's thread pools with it
        stopDispatch();
        pending.values().forEach(f -> f.completeExceptionally(
                new ProcessingSemaphoreException("the semaphore service is closed")));
        pending.clear();
        registry.clear();
        gates.clear();
    }

    // ------------------------------------------------------------------
    // The public surface: acquire, release, query
    // ------------------------------------------------------------------

    /** Acquires a permit without waiting. */
    public boolean tryAcquire(String name, int permits, String value) {
        checkOpen();
        Gate gate = gate(name, permits);

        // The local gate first: with this process's quota already full, there is no need to
        // ask the leader at all
        if (!gate.local.tryAcquire()) {
            localBlocked.incrementAndGet();
            return false;
        }
        boolean granted = false;
        try {
            SemaphoreMessage response =
                    exchange(SemaphoreMessageType.ACQUIRE, name, value, permits);
            granted = response != null && response.success();
            if (granted) {
                gate.held.incrementAndGet();
                gate.epoch = response.epoch();
                acquired.incrementAndGet();
            } else {
                // The local gate allowed it and the leader refused: other processes hold the
                // permits. Counted apart from localBlocked -- that one means this process's own
                // quota is full and a larger local gate fixes it, while this one means the whole
                // cluster is under pressure and the local gate will not help
                remoteDenied.incrementAndGet();
            }
            return granted;
        } finally {
            // The far end refused, so the local one goes back too, or this process's available
            // permits leak away
            if (!granted) {
                gate.local.release();
            }
        }
    }

    /**
     * Acquires a permit, retrying until the timeout.
     *
     * @param timeoutMs below 0 waits indefinitely
     */
    public boolean acquire(String name, int permits, String value, long timeoutMs)
            throws InterruptedException {
        checkOpen();
        long deadline = timeoutMs < 0
                ? Long.MAX_VALUE
                : saturatedAdd(System.currentTimeMillis(), timeoutMs);
        long startedAt = System.currentTimeMillis();
        while (true) {
            if (tryAcquire(name, permits, value)) {
                waitMillis.addAndGet(System.currentTimeMillis() - startedAt);
                return true;
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                acquireTimeouts.incrementAndGet();
                return false;
            }
            Thread.sleep(Math.min(retryIntervalMs, remaining));
        }
    }

    /** Returns a permit. Holding none, it does nothing. */
    public void release(String name) {
        Gate gate = gates.get(name);
        if (gate == null) {
            return;
        }
        // The held count is decremented by CAS first, and failing to decrement means nothing
        // was held, so it returns. This one step catches two kinds of wrong release: calling
        // release twice, and calling release after a change of leader has already invalidated
        // the permit and returned it to the local gate -- where one extra release would
        // conjure a permit into the gate out of nothing
        if (!gate.tryDecrement()) {
            staleReleases.incrementAndGet();
            return;
        }
        try {
            SemaphoreMessage response = exchange(SemaphoreMessageType.RELEASE, name, "", gate.permits);
            if (response == null || !response.success()) {
                log.debug("The release of permit {} was not confirmed; the lease will make it "
                        + "good", name);
            }
        } finally {
            released.incrementAndGet();
            gate.local.release();
        }
    }

    /** @return the permits available across the cluster, or -1 when it cannot be found */
    public int availablePermits(String name, int permits) {
        checkOpen();
        SemaphoreMessage response = exchange(SemaphoreMessageType.QUERY, name, "", permits);
        return response == null ? -1 : response.available();
    }

    /** How many permits this node holds. Decided locally, with no network request. */
    public int heldPermits(String name) {
        Gate gate = gates.get(name);
        return gate == null ? 0 : gate.held.get();
    }

    private Gate gate(String name, int permits) {
        return gates.computeIfAbsent(name, k -> new Gate(permits));
    }

    // ------------------------------------------------------------------
    // Sending requests
    // ------------------------------------------------------------------

    /**
     * Sends a request, and on failure <b>resends it under the same requestId</b>.
     *
     * <p>What this guards against is "the permit really was acquired, and only the reply was
     * lost on the way back": the caller believes it failed, the layer above retries, the same
     * holder's count becomes 2, and it will return only one -- <b>a permit leaks</b> until the
     * lease expires.
     *
     * <p>Reusing the requestId lets the leader's idempotence cache return the previous
     * reply.
     */
    private SemaphoreMessage exchange(SemaphoreMessageType type, String name, String value,
                                      int permits) {
        long requestId = requestIdGen.incrementAndGet();
        for (int attempt = 0; attempt <= RESEND_ATTEMPTS; attempt++) {
            SemaphoreMessage response = exchangeOnce(requestId, type, name, value, permits);
            if (response != null) {
                return response;
            }
        }
        return null;
    }

    private SemaphoreMessage exchangeOnce(long requestId, SemaphoreMessageType type, String name,
                                          String value, int permits) {
        Node leader = cluster.leader();
        if (leader == null) {
            log.debug("Leadership is vacant, so semaphore request {} cannot be handled yet",
                    name);
            return null;
        }

        SemaphoreMessage request =
                SemaphoreMessage.request(type, requestId, epoch, name, value, permits, leaseMs);

        // This node is the leader: the local register is consulted directly, by the same logic
        // as remotely, saving only the round trip
        if (leader.id().equals(cluster.self().id())) {
            return handleRequest(request, cluster.self());
        }

        CompletableFuture<SemaphoreMessage> future = new CompletableFuture<>();
        pending.put(requestId, future);
        try {
            if (!cluster.unicastOn(CHANNEL, leader, request.encode())) {
                return null;
            }
            return future.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.debug("Semaphore request {} timed out waiting for the leader's reply", name);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            log.debug("Semaphore request {} failed: {}", name,
                    e.getCause() == null ? e : e.getCause().toString());
            return null;
        } finally {
            pending.remove(requestId);
        }
    }

    // ------------------------------------------------------------------
    // Inbound handling
    // ------------------------------------------------------------------

    /**
     * Which messages are buffered. Replies pass straight through, being a
     * {@code future.complete} measured in nanoseconds; requests are queued.
     */
    @Override
    protected boolean shouldBuffer(Node sender, byte[] content) {
        SemaphoreMessage msg = SemaphoreMessage.decode(content);
        return msg != null && msg.type() != SemaphoreMessageType.RESPONSE;
    }

    /** Handles one semaphore message. Requests run on the consumer thread and replies pass
     *  straight through; see {@link #shouldBuffer}. */
    @Override
    protected void handlePayload(Node sender, byte[] content) {
        SemaphoreMessage msg = SemaphoreMessage.decode(content);
        if (msg == null) {
            log.debug("An undecodable semaphore message arrived from {}", sender.label());
            return;
        }

        if (msg.type() == SemaphoreMessageType.RESPONSE) {
            CompletableFuture<SemaphoreMessage> future = pending.get(msg.requestId());
            if (future != null) {
                future.complete(msg);
            }
            return;
        }

        SemaphoreMessage response = handleRequest(msg, sender);
        try {
            cluster.unicastOn(CHANNEL, sender, response.encode());
        } catch (Exception e) {
            log.debug("Failed to send the semaphore reply back to {}: {}", sender.label(),
                    e.toString());
        }
    }

    /** The buffer is full: what is dropped is a request, and the sender times out and
     *  retries. */
    @Override
    protected void onOverflow(Node sender, byte[] content) {
        log.warn("The semaphore request buffer is full; dropping a request from {} ({} so far)",
                sender.label(), dropped());
    }

    /**
     * The leader handles one semaphore request.
     *
     * <p>Acquiring, releasing and renewing all change state, so a resend must go through the
     * idempotence cache; a query must be current and does not.
     */
    private SemaphoreMessage handleRequest(SemaphoreMessage msg, Node sender) {
        if (hasSideEffect(msg.type())) {
            return requestIdempotence.execute(sender.id(), msg.requestId(),
                    () -> handleRequestOnce(msg, sender));
        }
        return handleRequestOnce(msg, sender);
    }

    /** The request types that change state. */
    private static boolean hasSideEffect(SemaphoreMessageType type) {
        return type == SemaphoreMessageType.ACQUIRE
                || type == SemaphoreMessageType.RELEASE
                || type == SemaphoreMessageType.RENEW;
    }

    private SemaphoreMessage handleRequestOnce(SemaphoreMessage msg, Node sender) {
        if (!cluster.isLeader()) {
            return SemaphoreMessage.fail(msg.requestId(), epoch, msg.name(), -1,
                    "this node is not the leader");
        }

        String ownerId = sender.id();
        return switch (msg.type()) {
            case ACQUIRE -> {
                PermitRegistry.Result r = registry.tryAcquire(
                        msg.name(), ownerId, msg.value(), msg.permits(), msg.leaseMs());
                yield r.success()
                        ? SemaphoreMessage.ok(msg.requestId(), epoch, msg.name(), r.available())
                        : SemaphoreMessage.fail(msg.requestId(), epoch, msg.name(),
                                r.available(), r.message());
            }
            case RELEASE -> {
                PermitRegistry.Result r = registry.release(msg.name(), ownerId);
                yield r.success()
                        ? SemaphoreMessage.ok(msg.requestId(), epoch, msg.name(), r.available())
                        : SemaphoreMessage.fail(msg.requestId(), epoch, msg.name(),
                                r.available(), r.message());
            }
            case RENEW -> registry.renew(msg.name(), ownerId, msg.leaseMs())
                    ? SemaphoreMessage.ok(msg.requestId(), epoch, msg.name(),
                            registry.available(msg.name()))
                    : SemaphoreMessage.fail(msg.requestId(), epoch, msg.name(), -1,
                            "the permit is no longer valid");
            case QUERY -> SemaphoreMessage.ok(msg.requestId(), epoch, msg.name(),
                    registry.available(msg.name()));
            case RESPONSE -> SemaphoreMessage.fail(msg.requestId(), epoch, msg.name(), -1,
                    "not a legitimate request type");
        };
    }

    // ------------------------------------------------------------------
    // Keeping the leases alive
    // ------------------------------------------------------------------

    private void renewAll() {
        if (closed || gates.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Gate> e : Map.copyOf(gates).entrySet()) {
            Gate gate = e.getValue();
            if (gate.held.get() <= 0) {
                continue;
            }
            try {
                SemaphoreMessage response =
                        exchange(SemaphoreMessageType.RENEW, e.getKey(), "", gate.permits);
                if (response == null) {
                    // The leader is temporarily unreachable; left to the next round, with time
                    // before the lease expires
                    continue;
                }
                if (!response.success() || response.epoch() != gate.epoch) {
                    int lost = gate.reset();
                    log.warn("{} permit(s) of semaphore {} are no longer valid ({})",
                            lost, e.getKey(),
                            response.success() ? "the leader has changed" : response.message());
                }
            } catch (Exception ex) {
                log.debug("Renewing semaphore {} errored: {}", e.getKey(), ex.toString());
            }
        }
    }

    private void evictExpired() {
        if (!closed && cluster.isLeader()) {
            int n = registry.evictExpired();
            if (n > 0) {
                log.debug("Swept {} expired permit(s)", n);
            }
        }
    }

    // ------------------------------------------------------------------
    // Cluster events
    // ------------------------------------------------------------------

    @Override
    public void onLeaderChanged(Node previous, Node current, boolean selfIsLeader) {
        String currentLeaderId = current == null ? null : current.id();
        if (Objects.equals(currentLeaderId, lastLeaderId)) {
            // The leader has not changed; this is only the notification emitted after the
            // quiet period. Clearing the register or invalidating here would silently swallow
            // everything handed out during startup
            return;
        }
        lastLeaderId = currentLeaderId;
        if (selfIsLeader) {
            epoch = System.currentTimeMillis();
            registry.clear();
            // With the register cleared, the cached replies have lost their basis -- keeping
            // them would let a resend after a change of leader receive an answer belonging to
            // the previous term. Which also shows that the idempotence cache is not there for
            // changes of leader: what it guards against is "the leader is alive and only the
            // reply was lost", which is where resends mostly come from
            requestIdempotence.clear();
            log.info("This node became the leader; the permit register was reset at epoch={}",
                    epoch);
        } else {
            registry.clear();
        }
        invalidateHeldPermits("the leader changed");
    }

    @Override
    public void onLeaderLeft(Node node) {
        invalidateHeldPermits("the leader departed");
    }

    /**
     * A change of leader takes the register with it and invalidates every permit this node
     * holds.
     *
     * <p>What matters is restoring <b>the local gate</b> at the same time: those permits are
     * still in the "taken" state inside the local semaphore, and clearing only the remote
     * record leaves this process permanently short.
     */
    private void invalidateHeldPermits(String reason) {
        int total = 0;
        for (Map.Entry<String, Gate> e : gates.entrySet()) {
            total += e.getValue().reset();
        }
        if (total > 0) {
            log.warn("{}: all {} permit(s) this node held are invalid and have been returned "
                    + "to the local gate", reason, total);
        }
    }

    @Override
    public void onNodeLeft(Node node, boolean graceful) {
        if (!cluster.isLeader()) {
            return;
        }
        Map<String, Integer> released = registry.releaseAllOf(node.id());
        if (!released.isEmpty()) {
            log.info("Member {} departed; the permits it held were released: {}",
                    node.label(), released);
        }
    }

    private static long saturatedAdd(long a, long b) {
        long sum = a + b;
        return ((a ^ sum) & (b ^ sum)) < 0 ? Long.MAX_VALUE : sum;
    }

    private void checkOpen() {
        if (closed) {
            throw new ProcessingSemaphoreException("the semaphore service is closed");
        }
    }

    /** For troubleshooting: a snapshot of the register, which holds anything only on the
     *  leader. */
    public Map<String, PermitRegistry.Snapshot> registrySnapshot() {
        return registry.snapshot();
    }

    /**
     * Runtime figures.
     *
     * <h2>How to read them</h2>
     * <ul>
     *   <li>{@code acquired - released} is <b>the permits currently held</b>. A difference that
     *       grows without falling is a leak: something acquired without a finally. A semaphore
     *       leak is harder to find than a lock leak -- exhausted permits show up as "everyone
     *       stalls" rather than as an error anywhere</li>
     *   <li>{@code localBlocked} and {@code remoteDenied} <b>call for different remedies</b>:
     *       the first means this process's own quota is full, which a larger local gate fixes,
     *       and the second means the whole cluster is under pressure, where the local gate will
     *       not help and either the total permits must rise or the load must fall</li>
     *   <li>{@code staleReleases} counts <b>releases of permits that were never held</b>. A
     *       non-zero value usually means a duplicate release, or a release after an epoch change
     *       had already invalidated the permit</li>
     *   <li>A persistently non-zero {@code acquireTimeouts} means there are simply not enough
     *       permits</li>
     * </ul>
     */
    @Override
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        long ok = acquired.get();
        long denied = localBlocked.get() + remoteDenied.get();
        m.put("acquired", ok);
        m.put("localBlocked", localBlocked.get());
        m.put("remoteDenied", remoteDenied.get());
        m.put("acquireTimeouts", acquireTimeouts.get());
        m.put("released", released.get());
        m.put("staleReleases", staleReleases.get());
        m.put("heldNow", ok - released.get());
        m.put("contentionRate", ok + denied == 0 ? 0d
                : Math.round(denied * 10000.0 / (ok + denied)) / 10000.0);
        m.put("avgWaitMillis", ok == 0 ? 0d
                : Math.round(waitMillis.get() * 100.0 / ok) / 100.0);
        m.put("gates", gates.size());
        m.put("pendingRequests", pending.size());
        m.put("epoch", epoch);
        return m;
    }

    public long epoch() {
        return epoch;
    }

    /**
     * This node's local gate for one semaphore.
     *
     * <p>The local layer is not superfluous: without it, a hundred threads in the process would
     * all send requests to the leader, which would refuse 99 of them, wasting 99 network round
     * trips. With it, requests beyond this process's quota are stopped locally.
     */
    private static final class Gate {
        final int permits;
        final Semaphore local;
        final AtomicInteger held = new AtomicInteger();
        volatile long epoch;

        Gate(int permits) {
            this.permits = permits;
            this.local = new Semaphore(permits);
        }

        /**
         * Decrements by one if there is anything to decrement, atomically.
         *
         * @return whether one was really taken away
         */
        boolean tryDecrement() {
            while (true) {
                int current = held.get();
                if (current <= 0) {
                    return false;
                }
                if (held.compareAndSet(current, current - 1)) {
                    return true;
                }
            }
        }

        /**
         * Invalidates every permit this node holds and restores the local gate to full.
         *
         * @return how many permits were invalidated
         */
        int reset() {
            int lost = held.getAndSet(0);
            if (lost > 0) {
                local.release(lost);
            }
            return lost;
        }
    }
}
