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

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The distributed lock's core: requests converge on the leader for adjudication, and replies
 * return the same way.
 *
 * <h2>Why the leader</h2>
 * Every node in the cluster knows who the leader is -- whoever holds the cluster port -- so no
 * separate registry is needed and nothing has to be negotiated. Everyone sends requests to the
 * same place, which adjudicates them serially, and two parties cannot both succeed.
 *
 * <h2>Its own channel, undisturbing to the application</h2>
 * Every lock message travels on {@link #CHANNEL}. The application's own listeners subscribe to
 * the default channel and therefore <b>never see</b> these internal messages -- no application
 * has to recognise another component's message format.
 *
 * <h2>Request/response is built at this layer</h2>
 * spreader's business channel is one-way: a message goes out, the peer's {@code onPayload}
 * receives it, and nothing returns. Here each request carries an id, a
 * {@link CompletableFuture} is hung in {@link #pending} once it is sent, and the reply finds it
 * by id and completes it. A timeout counts as failure.
 *
 * <h2>Why the lock retries by polling while latches and barriers wait for a push</h2>
 * {@link LatchService} and {@link BarrierService} in this same package do not poll -- a waiter
 * hangs and the leader pushes once the condition holds. <b>A lock cannot do that</b>, and the
 * difference lies in what a push can express:
 *
 * <ul>
 *   <li>A latch reaching zero, or a barrier filling, is <b>a settled fact</b>. The push means
 *       "it is done, you may go", and every waiter returns successfully.</li>
 *   <li>A lock being released is only "it is free now". The push can say no more than "you may
 *       try again" -- the waiter still <b>has to race for it</b> on waking, and only one can
 *       win. Ten waiters woken to race for one lock means nine wasted trips: a thundering
 *       herd.</li>
 * </ul>
 *
 * So the lock keeps to "failing to take it, wait {@code retryIntervalMs} and try again".
 * <b>Do not change it to a push for consistency with the latch</b> -- that is not consistency
 * but forcing a contention primitive into the shape of a notification primitive.
 *
 * <h2>The leader's own requests go the same way</h2>
 * A process on the leader acquiring a lock gets no special treatment: the same request is
 * built and the same register adjudicates it, only without the round trip, calling the local
 * {@link LockRegistry} directly. The logic is identical.
 *
 * <p><b>A side effect: the leader has an advantage in contention.</b> It consults a register in
 * its own memory while everyone else needs a network round trip -- measured at about 1.5
 * milliseconds' difference -- so with several parties racing for one lock, the leader nearly
 * always wins. That is of no consequence where anyone may take it, but it makes a
 * {@code @MultiProcessingScheduled} task settle on the leader's machine rather than rotating
 * among instances. Genuinely even distribution would need a rotation policy at the register
 * level.
 *
 * <h2>What it guarantees, and what it does not</h2>
 * <p><b>It does</b> guarantee that during normal operation one lock name is held by one process
 * at a time, and that the lock is released when its holder crashes, departs gracefully, or gets
 * stuck and stops renewing.
 *
 * <p><b>It does not</b> exceed <b>the strength of "there is one leader"</b>. spreader elects by
 * contending for the cluster port, and on one machine port exclusion is the operating system's
 * guarantee -- but <b>across machines it is only an agreement about timing</b>. Under a network
 * partition each side may have a leader maintaining a register of its own, and the same lock is
 * held by two processes at once.
 *
 * <p>So: coordinating things where repeating the work is merely wasteful rather than wrong --
 * scheduled tasks, cache warming, exclusive consumption -- is appropriate; <b>do not</b> use it
 * to protect a transfer or a debit, where repeating it causes real harm. That calls for a CP
 * lock with a fencing token, and something with a consensus protocol behind it: etcd,
 * ZooKeeper, or the like.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MutexService extends BufferedGossipListener
        implements MultiProcessingService, SelfRegisteringListener {

    private static final Logger log = LoggerFactory.getLogger(MutexService.class);

    /** How many times one request is resent at most. See {@link #exchangeWithLease}. */
    private static final int RESEND_ATTEMPTS = 2;

    /** The lock protocol's own channel. The application's listeners subscribe to the default
     *  channel and never see these messages. */
    public static final String CHANNEL = "spreader.mutex";

    private final GossipCluster cluster;
    private final long leaseMs;
    private final long requestTimeoutMs;
    private final long retryIntervalMs;

    /** Used only while this node is the leader. */
    private final LockRegistry registry = new LockRegistry();

    /** The locks this node currently holds. */
    private final Map<String, HeldLock> held = new ConcurrentHashMap<>();

    /** Requests awaiting a reply. */
    private final Map<Long, CompletableFuture<MutexMessage>> pending = new ConcurrentHashMap<>();

    private final AtomicLong requestIdGen = new AtomicLong();

    // ------------------------------------------------------------------
    // Statistics: lock contention is the commonest hidden bottleneck in production, and it
    // makes no sound at all in the log
    // ------------------------------------------------------------------

    /** Successful tryAcquire calls. */
    private final AtomicLong acquired = new AtomicLong();

    /** Failed tryAcquire calls, the lock being held by someone else. Together with acquired,
     *  this is the degree of contention. */
    private final AtomicLong contended = new AtomicLong();

    /** How many acquire calls timed out without the lock. Persistently non-zero means this
     *  lock is a bottleneck. */
    private final AtomicLong acquireTimeouts = new AtomicLong();

    /** Releases. A clear disagreement with acquired means something took a lock and never
     *  returned it -- a leak. */
    private final AtomicLong released = new AtomicLong();

    /** The total milliseconds acquire calls waited from start to holding the lock; divided by
     *  acquired, this is the mean wait. */
    private final AtomicLong waitMillis = new AtomicLong();

    /** The leader-side reply cache, so a resend gets the previous reply rather than taking the
     *  lock again. */
    private final IdempotentRequestCache<MutexMessage> requestIdempotence =
            new IdempotentRequestCache<>(30_000L, 10_000);

    /**
     * The leader's epoch.
     *
     * <p>It takes a new value whenever this node becomes the leader. A holder remembers the
     * epoch at which it took the lock, and a changed epoch tells it that the register has
     * changed hands and the lock it holds no longer counts.
     */
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
    public MutexService(GossipCluster cluster, long leaseMs, long requestTimeoutMs, long retryIntervalMs, ExecutorServiceHolder executors) {
        // Requests are buffered and replies pass straight through; see shouldBuffer. One
        // consumer thread: the register has to be serial anyway, more threads would only add
        // lock contention, and serialising also handles requests in arrival order
        super("mutex", 2048, 1, LoggerFactory.getLogger("spreader.mutex"));
        this.cluster = cluster;
        this.leaseMs = leaseMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryIntervalMs = retryIntervalMs;
        this.executors = executors;
    }

    public void start() {
        // Only the lock channel is subscribed to: it receives no business messages, and its own
        // messages never reach the application. The consumer thread starts before subscribing:
        // the other way round, messages arriving after the subscription would be discarded as
        // "not started yet"
        startDispatch();
        cluster.addListener(CHANNEL, this);
        lastLeaderId = cluster.leader() == null ? null : cluster.leader().id();
        if (cluster.isLeader()) {
            epoch = System.currentTimeMillis();
        }
        // The renewal interval is a third of the lease: losing one or two still leaves time to
        // make it good before it expires
        long renewInterval = Math.max(200L, leaseMs / 3);
        executors.forRenewal().scheduleWithFixedDelay(this::renewAll, renewInterval, renewInterval, TimeUnit.MILLISECONDS);
        executors.forMaintenance().scheduleWithFixedDelay(this::evictExpired, leaseMs, leaseMs, TimeUnit.MILLISECONDS);
        log.info("Distributed lock service started: lease={}ms, request timeout={}ms",
                leaseMs, requestTimeoutMs);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        cluster.removeListener(this);
        // Held locks are released deliberately, so nobody has to wait out a lease
        for (String lockName : List.copyOf(held.keySet())) {
            try {
                release(lockName);
            } catch (Exception e) {
                log.debug("Failed to release lock {} while shutting down: {}", lockName,
                        e.toString());
            }
        }
        // executors is not closed: it belongs to the container, and one service shutting down
        // should not take another's thread pools with it
        stopDispatch();
        pending.values().forEach(f -> f.completeExceptionally(
                new ProcessingMutexException("the lock service is closed")));
        pending.clear();
        registry.clear();
    }

    // ------------------------------------------------------------------
    // The public surface: acquire, release, query
    // ------------------------------------------------------------------

    /**
     * Tries once, without retrying.
     *
     * @return whether it was acquired
     */
    public boolean tryAcquire(String lockName, String value) {
        checkOpen();
        MutexMessage response = exchange(MutexMessageType.ACQUIRE, lockName, value);
        if (response != null && response.success()) {
            held.put(lockName, new HeldLock(value, response.epoch()));
            acquired.incrementAndGet();
            return true;
        }
        // Not acquired: someone else may hold it, or the leader may be temporarily
        // unavailable. Both count as "not acquired this time"; telling them apart means looking
        // at the tps and error-rate metrics
        contended.incrementAndGet();
        return false;
    }

    /**
     * Acquires, retrying until the timeout.
     *
     * @param timeoutMs below 0 waits indefinitely, until it is acquired or the thread is
     *                  interrupted
     */
    public boolean acquire(String lockName, String value, long timeoutMs) throws InterruptedException {
        checkOpen();
        // Saturating addition: a plain sum would overflow negative for a very large timeout,
        // turning "wait a long time" into "fail at once"
        long deadline = timeoutMs < 0
                ? Long.MAX_VALUE
                : saturatedAdd(System.currentTimeMillis(), timeoutMs);
        long startedAt = System.currentTimeMillis();
        while (true) {
            if (tryAcquire(lockName, value)) {
                // The wait is recorded rather than averaged here: the mean is computed at the
                // reading end, so that several processes' figures combine correctly -- sums add,
                // means do not
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
            // While leadership is vacant this spins for a few rounds, as expected: it resumes
            // once a new leader takes the cluster port
            Thread.sleep(Math.min(retryIntervalMs, remaining));
        }
    }

    /** Releases it. Holding nothing, it does nothing. */
    public void release(String lockName) {
        release(lockName, 0L);
    }

    /**
     * Releases it and makes the lock unobtainable by anyone for the next {@code cooldownMs}.
     *
     * <p>This is for scheduled tasks: releasing the moment a task finishes in a few
     * milliseconds lets another instance take it at once, and the same round runs several
     * times.
     */
    public void release(String lockName, long cooldownMs) {
        HeldLock local = held.remove(lockName);
        if (local == null) {
            return;
        }
        released.incrementAndGet();
        // The leaseMs field of a RELEASE message carries the cooldown here -- a lease means
        // nothing for a lock already released
        MutexMessage response = exchangeWithLease(
                MutexMessageType.RELEASE, lockName, local.value(), cooldownMs);
        if (response == null || !response.success()) {
            // No matter: the lock is released of its own accord once the lease expires
            log.debug("The release of lock {} was not confirmed; the lease will make it good",
                    lockName);
        }
    }

    /** @return the identifier the current holder wrote, or null when nobody holds it */
    public String currentOccupied(String lockName) {
        checkOpen();
        MutexMessage response = exchange(MutexMessageType.QUERY, lockName, "");
        if (response == null || !response.success()) {
            return null;
        }
        return response.value().isEmpty() ? null : response.value();
    }

    /**
     * Whether this node still holds the lock.
     *
     * <p>This is <b>decided locally</b>, with no network request: as long as the leader has not
     * changed and the lease is still being renewed, it counts as held. A change of leader
     * invalidates the local record at once, because the register lived in the old leader's
     * memory and is gone.
     */
    public boolean isHeld(String lockName) {
        return held.containsKey(lockName);
    }

    // ------------------------------------------------------------------
    // Sending requests
    // ------------------------------------------------------------------

    /**
     * Sends a request to the leader and waits for the reply.
     *
     * @return the reply, or null when leadership is vacant, the send failed, or it timed
     *         out
     */
    private MutexMessage exchange(MutexMessageType type, String lockName, String value) {
        return exchangeWithLease(type, lockName, value, leaseMs);
    }

    /**
     * Sends a request, and on failure <b>resends it under the same requestId</b>.
     *
     * <h2>Why resending, and why the id must be reused</h2>
     * "The request never arrived" and "it arrived and the reply was lost" look identical to the
     * caller. The second is the dangerous one: the leader has given me the lock while I believe
     * it failed -- the layer above retries, the reentrancy count becomes 2, and I will release
     * only once, so <b>the lock leaks</b> until the lease expires.
     *
     * <p>Reusing the requestId lets the leader's idempotence cache recognise the resend and
     * return the previous reply -- "the lock is yours" -- as it was. Both cases are thereby
     * fixed, rather than one being chosen over the other.
     *
     * <p>Note that this is a different thing from <b>the polling retry</b> above: that means
     * "race for it again", each attempt being a semantically new request that should carry a new
     * id. What is resent here is <b>the same</b> request.
     */
    private MutexMessage exchangeWithLease(MutexMessageType type, String lockName,
                                           String value, long leaseOrCooldownMs) {
        long requestId = requestIdGen.incrementAndGet();
        for (int attempt = 0; attempt <= RESEND_ATTEMPTS; attempt++) {
            MutexMessage response = exchangeOnce(requestId, type, lockName, value,
                    leaseOrCooldownMs);
            if (response != null) {
                return response;
            }
        }
        return null;
    }

    private MutexMessage exchangeOnce(long requestId, MutexMessageType type, String lockName,
                                      String value, long leaseOrCooldownMs) {
        Node leader = cluster.leader();
        if (leader == null) {
            // The gap after the old leader has gone and before the new one takes the cluster
            // port. It fails fast, and the layer above decides whether to retry
            log.debug("Leadership is vacant, so lock request {} cannot be handled yet",
                    lockName);
            return null;
        }

        MutexMessage request = MutexMessage.request(
                type, requestId, epoch, lockName, value, leaseOrCooldownMs);

        // This node is the leader: the local register is consulted directly, by exactly the
        // same logic as remotely, saving only the round trip. Note that this makes the leader a
        // step quicker in contention; see the class javadoc on the leader's own requests
        if (leader.id().equals(cluster.self().id())) {
            return handleRequest(request, cluster.self());
        }

        CompletableFuture<MutexMessage> future = new CompletableFuture<>();
        pending.put(requestId, future);
        try {
            if (!cluster.unicastOn(CHANNEL, leader, request.encode())) {
                return null;
            }
            return future.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.debug("Lock request {} timed out waiting for the leader's reply", lockName);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            log.debug("Lock request {} failed: {}", lockName,
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
     * Which messages are buffered.
     *
     * <p>Replies pass straight through: handling one is a {@code future.complete}, measured in
     * nanoseconds. A thread switch for that is a pure loss -- the switch itself costs an order
     * of magnitude more than the work, and the caller is blocked waiting for the reply.
     *
     * <p>Requests are buffered: they consult the register and send a reply, waiting
     * synchronously for its ACK, which is the part that does real work.
     */
    @Override
    protected boolean shouldBuffer(Node sender, byte[] content) {
        MutexMessage msg = MutexMessage.decode(content);
        return msg != null && msg.type() != MutexMessageType.RESPONSE;
    }

    /**
     * Handles one lock message.
     *
     * <p>Requests run on the buffer's consumer thread while replies pass straight through, on
     * the dispatch thread; see {@link #shouldBuffer}.
     */
    @Override
    protected void handlePayload(Node sender, byte[] content) {
        // This is the lock's own channel, so everything arriving should be a lock message;
        // anything that will not decode is malformed
        MutexMessage msg = MutexMessage.decode(content);
        if (msg == null) {
            log.debug("An undecodable lock message arrived from {}", sender.label());
            return;
        }

        if (msg.type() == MutexMessageType.RESPONSE) {
            CompletableFuture<MutexMessage> future = pending.get(msg.requestId());
            if (future != null) {
                future.complete(msg);
            }
            return;
        }

        MutexMessage response = handleRequest(msg, sender);
        try {
            cluster.unicastOn(CHANNEL, sender, response.encode());
        } catch (Exception e) {
            log.debug("Failed to send the lock reply back to {}: {}", sender.label(),
                    e.toString());
        }
    }

    /** The buffer is full: what is dropped is a request, and the sender times out and retries.
     *  Happening repeatedly means this node is not keeping up. */
    @Override
    protected void onOverflow(Node sender, byte[] content) {
        log.warn("The lock request buffer is full; dropping a request from {} ({} so far)",
                sender.label(), dropped());
    }

    /**
     * The leader handles one lock request.
     *
     * <h2>Requests with side effects go through the idempotence cache</h2>
     * Acquiring, releasing and renewing all change state, and a caller resends when a reply is
     * lost. Without idempotence, a resent ACQUIRE would add another level of reentrancy while
     * the caller releases only once -- the lock leaks until the lease expires.
     *
     * <p>Queries do not enter the cache: they have no side effects to begin with, and must
     * return <b>current</b> state -- an answer from tens of seconds ago would be wrong.
     *
     * <p>Only the leader adjudicates. Receiving a request while not the leader means the
     * requester's view is out of date, so it is told plainly "I am not the leader" and looks
     * again.
     */
    private MutexMessage handleRequest(MutexMessage msg, Node sender) {
        if (hasSideEffect(msg.type())) {
            return requestIdempotence.execute(sender.id(), msg.requestId(),
                    () -> handleRequestOnce(msg, sender));
        }
        return handleRequestOnce(msg, sender);
    }

    /** The request types that change state. */
    private static boolean hasSideEffect(MutexMessageType type) {
        return type == MutexMessageType.ACQUIRE
                || type == MutexMessageType.RELEASE
                || type == MutexMessageType.RENEW;
    }

    private MutexMessage handleRequestOnce(MutexMessage msg, Node sender) {
        if (!cluster.isLeader()) {
            return MutexMessage.fail(msg.requestId(), epoch, msg.lockName(),
                    "this node is not the leader");
        }

        String ownerId = sender.id();
        return switch (msg.type()) {
            case ACQUIRE -> {
                LockEntry blocker = registry.tryAcquire(msg.lockName(), ownerId, msg.value(), msg.leaseMs());
                yield blocker == null
                        ? MutexMessage.ok(msg.requestId(), epoch, msg.lockName(), msg.value())
                        : MutexMessage.fail(msg.requestId(), epoch, msg.lockName(),
                                "already held by: " + blocker.value());
            }
            // A RELEASE's leaseMs carries the cooldown; see release(String, long)
            case RELEASE -> registry.release(msg.lockName(), ownerId, msg.leaseMs())
                    ? MutexMessage.ok(msg.requestId(), epoch, msg.lockName(), "")
                    : MutexMessage.fail(msg.requestId(), epoch, msg.lockName(),
                            "the lock is not in your hands");
            case RENEW -> registry.renew(msg.lockName(), ownerId, msg.leaseMs())
                    ? MutexMessage.ok(msg.requestId(), epoch, msg.lockName(), "")
                    : MutexMessage.fail(msg.requestId(), epoch, msg.lockName(),
                            "the lock is no longer valid");
            case QUERY -> {
                LockEntry current = registry.current(msg.lockName());
                yield MutexMessage.ok(msg.requestId(), epoch, msg.lockName(),
                        current == null ? "" : current.value());
            }
            case RESPONSE -> MutexMessage.fail(msg.requestId(), epoch, msg.lockName(),
                    "not a legitimate request type");
        };
    }

    // ------------------------------------------------------------------
    // Keeping the leases alive
    // ------------------------------------------------------------------

    /**
     * Renews the lease on every lock held.
     *
     * <p>A failed renewal removes the local record -- the lock is no longer this node's, and
     * continuing to treat it as held would have two processes both believe they hold it. Erring
     * towards "lost" is right here; erring towards "still held" is not.
     */
    private void renewAll() {
        if (closed || held.isEmpty()) {
            return;
        }
        for (Map.Entry<String, HeldLock> e : List.copyOf(held.entrySet())) {
            String lockName = e.getKey();
            HeldLock local = e.getValue();
            try {
                MutexMessage response = exchange(MutexMessageType.RENEW, lockName, local.value());
                if (response == null) {
                    // The leader is temporarily unreachable; kept for the next round, with
                    // time before the lease expires
                    continue;
                }
                if (!response.success() || response.epoch() != local.epoch()) {
                    held.remove(lockName, local);
                    log.warn("Lock {} is no longer valid ({}); this node no longer holds it",
                            lockName,
                            response.success() ? "the leader has changed" : response.message());
                }
            } catch (Exception ex) {
                log.debug("Renewing {} errored: {}", lockName, ex.toString());
            }
        }
    }

    private void evictExpired() {
        if (!closed && cluster.isLeader()) {
            int n = registry.evictExpired();
            if (n > 0) {
                log.debug("Swept {} expired lock record(s)", n);
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
            // This node has just become the leader: a new epoch invalidates every token the
            // old leader handed out
            epoch = System.currentTimeMillis();
            registry.clear();
            // With the register cleared, the cached replies have lost their basis -- keeping
            // them would let a resend after a change of leader receive an answer belonging to
            // the previous term. Which also shows that the idempotence cache is not there for
            // changes of leader: what it guards against is "the leader is alive and only the
            // reply was lost", which is where resends mostly come from
            requestIdempotence.clear();
            log.info("This node became the leader; the lock register was reset at epoch={}",
                    epoch);
        } else {
            registry.clear();
        }
        invalidateHeldLocks("the leader changed");
    }

    @Override
    public void onLeaderLeft(Node node) {
        invalidateHeldLocks("the leader departed");
    }

    /**
     * A change of leader takes the register with it -- it lives only in memory, and is neither
     * replicated nor persisted. So every lock this node holds must be invalidated at once, or
     * the state becomes "the new leader believes nobody holds it while this process still
     * believes it does": a double hold.
     */
    private void invalidateHeldLocks(String reason) {
        if (held.isEmpty()) {
            return;
        }
        List<String> lost = new ArrayList<>(held.keySet());
        held.clear();
        log.warn("{}: all {} lock(s) this node held are no longer valid: {}",
                reason, lost.size(), lost);
    }

    @Override
    public void onNodeLeft(Node node, boolean graceful) {
        if (!cluster.isLeader()) {
            return;
        }
        // The holder is gone, so its locks are released at once and nobody has to idle out a
        // lease
        List<String> released = registry.releaseAllOf(node.id());
        if (!released.isEmpty()) {
            log.info("Member {} departed; the {} lock(s) it held were released: {}",
                    node.label(), released.size(), released);
        }
    }

    private static long saturatedAdd(long a, long b) {
        long sum = a + b;
        // Two values of the same sign giving the opposite sign means it overflowed
        return ((a ^ sum) & (b ^ sum)) < 0 ? Long.MAX_VALUE : sum;
    }

    private void checkOpen() {
        if (closed) {
            throw new ProcessingMutexException("the lock service is closed");
        }
    }

    /** For troubleshooting: a snapshot of the register, which holds anything only on the
     *  leader. */
    public Map<String, LockEntry> registrySnapshot() {
        return registry.snapshot();
    }

    public long epoch() {
        return epoch;
    }

    /** The locks this node holds. */
    public Map<String, HeldLock> heldLocks() {
        return Map.copyOf(held);
    }

    /**
     * The local record of a holding.
     *
     * @param value the identifier written when it was acquired
     * @param epoch the leader's epoch when the lock was taken, used to detect a change of
     *              leader
     */
    public record HeldLock(String value, long epoch) {
    }

    /**
     * The lock's runtime figures.
     *
     * <h2>How to read them</h2>
     * <ul>
     *   <li>{@code contended / (acquired + contended)} -- <b>the contention rate</b>.
     *       Persistently above 0.5 means this lock turns away more than half the requests, and
     *       it should be split or held for less time</li>
     *   <li>{@code avgWaitMillis} -- the mean wait. It is more direct than the contention rate:
     *       heavy contention with a wait of a few milliseconds goes unnoticed</li>
     *   <li>{@code acquireTimeouts} -- <b>persistently non-zero is a fault</b>, meaning requests
     *       timed out without the lock</li>
     *   <li>{@code acquired - released} -- the locks currently held. A difference that
     *       <b>grows without falling</b> is a leak: something acquired without a finally</li>
     * </ul>
     */
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        long ok = acquired.get();
        long fail = contended.get();
        m.put("acquired", ok);
        m.put("contended", fail);
        m.put("acquireTimeouts", acquireTimeouts.get());
        m.put("released", released.get());
        m.put("heldNow", held.size());
        m.put("contentionRate", ok + fail == 0 ? 0d
                : Math.round(fail * 10000.0 / (ok + fail)) / 10000.0);
        m.put("avgWaitMillis", ok == 0 ? 0d
                : Math.round(waitMillis.get() * 100.0 / ok) / 100.0);
        m.put("registered", registry == null ? 0 : registry.size());
        return m;
    }

}
