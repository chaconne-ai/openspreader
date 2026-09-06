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
package com.chaconneai.openspreader.sync;

import com.chaconneai.openspreader.MultiProcessingService;
import com.chaconneai.spreader.GossipCluster;
import com.chaconneai.spreader.Node;
import com.chaconneai.openspreader.concurrent.ExecutorServiceHolder;
import com.chaconneai.openspreader.cluster.SelfRegisteringListener;
import com.chaconneai.spreader.event.BufferedGossipListener;
import com.chaconneai.openspreader.idempotence.IdempotentRequestCache;
import com.chaconneai.spreader.transport.TransportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The cross-process latch's core: counts converge on the leader, which notifies every waiter
 * when the count reaches zero.
 *
 * <h2>The same approach as the lock</h2>
 * Every node in the cluster knows who the leader is -- whoever holds the cluster port -- and
 * every request goes to that one place to be handled serially. It uses its own channel
 * {@link #CHANNEL}; the application's listeners subscribe to the default channel and never see
 * these internal messages.
 *
 * <h2>Three differences from the lock</h2>
 * <ol>
 *   <li><b>It waits for a signal rather than asking for an answer.</b> A waiter hangs and the
 *       leader pushes to it when the count reaches zero -- a wake-up latency measured at about
 *       14ms. <b>There is no periodic polling</b>: with many waiters, polling is pure wasted
 *       traffic, and the case it would rescue -- a lost push -- is caught by the caller's own
 *       timeout anyway.</li>
 *   <li><b>A departing party does not decrement the count.</b> A lock's holder departing must
 *       release the lock at once; a latch is the opposite -- the count-downs it made are
 *       accomplished facts and cannot be taken back.</li>
 *   <li><b>A change of leader costs more</b>; see below.</li>
 * </ol>
 *
 * <h2>What a change of leader does</h2>
 * The register lives only in the leader's memory, so a change takes it away and the count
 * returns to its initial value.
 *
 * <p>Because there is no polling, "the register is gone" <b>can only be discovered through the
 * cluster events this node receives</b> ({@code onLeaderChanged} and {@code onLeaderLeft}) --
 * a dead leader pushes nothing more, and asking during the vacancy reaches nobody. So those
 * two events advance {@code leaderGeneration} and wake every waiter, which compares on waking,
 * finds what it held invalid, and exits by throwing {@link ProcessingMutexException}.
 *
 * <p>The cost of omitting that step was measured: a waiter hangs until its own timeout, 115
 * seconds in the test, with nothing to suggest anything is wrong.
 *
 * <p>To make it recoverable across a change of leader, use
 * {@link #countDown(String, String)} with a participantId: the leader de-duplicates by party,
 * every party counts down again after the change, and the count returns to its correct value.
 * <b>This is the recommended usage.</b> The count-only version cannot recover from a change of
 * leader.
 *
 * <h2>What it guarantees, and what it does not</h2>
 * <b>It does</b> guarantee that during normal operation the count only falls, never
 * double-decrements, and that every waiter wakes once it reaches zero.
 *
 * <p><b>It does not</b> exceed the strength of "there is one leader". Under a network
 * partition each side has a leader maintaining a count of its own, and each reaches zero once.
 * Coordinating things where running twice is merely wasteful is fine; do not use it as a
 * distributed transaction's commit point.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class LatchService extends BufferedGossipListener
        implements MultiProcessingService, SelfRegisteringListener {

    private static final Logger log = LoggerFactory.getLogger(LatchService.class);

    /** How many times one request is resent at most. See {@link #exchange}. */
    private static final int RESEND_ATTEMPTS = 2;

    /** The latch protocol's own channel. */
    public static final String CHANNEL = "spreader.latch";

    private final GossipCluster cluster;
    private final long requestTimeoutMs;
    private final long idleTimeoutMs;
    /**
     * How long one suspension may last before waking to ask again.
     *
     * <p><b>Under TCP this is unbounded -- it waits for the push and polls not at all.</b>
     * Business messages over TCP carry ACKs and resends, so the push is reliable and polling is
     * pure waste.
     *
     * <p><b>Under UDP that will not do</b>: a datagram does not guarantee delivery to begin
     * with and may still be lost after several resends, and once a push is lost the waiter can
     * only hang until the caller's timeout -- measured as an occasional party waiting the full
     * 20 seconds after a few rounds of a three-party barrier. So UDP falls back to bounded
     * re-checking.
     *
     * <p>This is not "polling by default"; it is "covering for a transport that cannot
     * deliver".
     */
    private final long maxWaitSliceMs;

    /** Used only while this node is the leader. */
    private final LatchRegistry registry = new LatchRegistry();

    /** Suspending and waking local waiters. */
    private final SignalBox signals = new SignalBox();

    private final Map<Long, CompletableFuture<SyncMessage>> pending = new ConcurrentHashMap<>();
    private final AtomicLong requestIdGen = new AtomicLong();

    /** The leader-side reply cache, so a resend gets the previous reply rather than counting
     *  down again. */
    private final IdempotentRequestCache<SyncMessage> requestIdempotence =
            new IdempotentRequestCache<>(30_000L, 10_000);

    /** The leader epoch, which changes with each change of leader. It travels in requests and
     *  replies, so that a register having changed hands can be detected. */
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
     * How many changes of leader this node has observed.
     *
     * <p>Without polling this is <b>the only way to discover in time that the register is
     * gone</b>. A dead leader pushes nothing more and a waiter asking reaches nobody, there
     * being no leader during the vacancy, so all that is left is the cluster events this node
     * receives. A waiter records this number on entry and compares on each waking; a change
     * means what it was waiting on is invalid.
     */
    private final AtomicLong leaderGeneration = new AtomicLong();

    // Runtime statistics
    private final AtomicLong declares = new AtomicLong();
    private final AtomicLong countDowns = new AtomicLong();
    private final AtomicLong awaits = new AtomicLong();
    private final AtomicLong satisfied = new AtomicLong();
    private final AtomicLong awaitTimeouts = new AtomicLong();
    private final AtomicLong invalidations = new AtomicLong();
    private final AtomicLong waitMillis = new AtomicLong();
    /**
     * A single thread devoted to pushes.
     *
     * <p><b>It must not be shared with the buffer's consumer thread</b>: the consumer waits
     * synchronously for an ACK while sending a reply, and the "the condition is satisfied" push
     * is started while handling an inbound request. Sharing one pool, several inbound threads
     * block on sending together and the push task queues behind them, never leaving -- the
     * waiter receives no push and, with no polling to fall back on, hangs until its own
     * timeout.
     *
     * <p>That is exactly how it hung in testing: a few rounds of a three-party barrier, with an
     * occasional party waiting the full 20 seconds.
     */
    private final ExecutorService notifier;

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
    public LatchService(GossipCluster cluster, long requestTimeoutMs, long idleTimeoutMs, ExecutorServiceHolder executors) {
        // Requests are buffered and replies pass straight through; see shouldBuffer. One
        // consumer thread: the register has to be serial anyway, more threads would only add
        // lock contention, and serialising also handles requests in arrival order
        super("latch", 4096, 1, LoggerFactory.getLogger("spreader.latch"));
        this.cluster = cluster;
        this.requestTimeoutMs = requestTimeoutMs;
        this.idleTimeoutMs = idleTimeoutMs;
        this.maxWaitSliceMs = resolveWaitSlice(cluster);
        this.notifier = executors.forLatchNotify();
        this.executors = executors;
    }

    /**
     * The upper bound on a suspension: the longest interval before waking to check.
     *
     * <h2>TCP needs a bound too</h2>
     * TCP once used {@code Long.MAX_VALUE}, on the grounds that "TCP is reliable, so waiting
     * for the push is enough". <b>Those grounds do not hold</b>: what TCP guarantees is
     * "delivered or reported", and a failed push is swallowed inside {@code notifyWaiters},
     * leaving only a debug line. A full queue at the peer, or a connection reclaimed at just
     * the wrong moment, loses the push -- and the waiter knows nothing about it.
     *
     * <p>Without a backstop, one failed push means one party waiting out the entire call
     * timeout, measured at 15 to 20 seconds, with the others hanging alongside. That was the
     * last remaining path behind the occasional hang.
     *
     * <p>So TCP gets an upper bound too, only a wider one -- a lost push is rare over TCP and
     * looking back every two seconds costs nothing, while packet loss is normal over UDP and
     * 500ms suits it better.
     *
     * <p><b>This is not polling</b>: normally the push arrives and wakes it, and this bound is
     * never reached. It matters only when a push really is lost.
     */
    private static long resolveWaitSlice(GossipCluster cluster) {
        return cluster.config().transportType() == TransportType.UDP ? 500L : 2_000L;
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
        long tick = Math.max(1_000L, idleTimeoutMs / 4);
        executors.forMaintenance().scheduleWithFixedDelay(this::evictIdle, tick, tick, TimeUnit.MILLISECONDS);
        log.info("Cross-process latch service started: request timeout={}ms, idle reclaim={}ms",
                requestTimeoutMs, idleTimeoutMs);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        cluster.removeListener(this);
        // executors is not closed: it belongs to the container, and one service shutting down
        // should not take another's thread pools with it
        stopDispatch();
        // notifier is not closed either: it belongs to ExecutorServiceHolder, and one service
        // shutting down should not take another's thread pools with it
        pending.values().forEach(f -> f.completeExceptionally(
                new ProcessingMutexException("the latch service is closed")));
        pending.clear();
        // Every waiter still hanging is woken, so that they find the service gone rather than
        // waiting indefinitely
        signals.clear();
        registry.clear();
    }

    // ------------------------------------------------------------------
    // The public surface
    // ------------------------------------------------------------------

    /**
     * Declares a latch with an initial count.
     *
     * @return the reason for failure, or null on success
     */
    public String declare(String name, long count) {
        checkOpen();
        declares.incrementAndGet();
        SyncMessage r = exchange(SyncMessageType.LATCH_DECLARE, name, "", count, 0L);
        if (r == null) {
            return "the leader is temporarily unavailable";
        }
        return r.success() ? null : r.message();
    }

    /**
     * Decrements the count by one.
     *
     * @param participantId the party's identifier; when non-blank, it de-duplicates so that
     *                      repeated reports decrement once. <b>Strongly recommended</b>, since
     *                      without it the count cannot recover from a change of leader
     * @return the count remaining, or -1 on failure
     */
    public long countDown(String name, String participantId) {
        checkOpen();
        countDowns.incrementAndGet();
        SyncMessage r = exchange(SyncMessageType.LATCH_COUNT_DOWN, name, participantId, 0L, 0L);
        return r != null && r.success() ? r.count() : -1L;
    }

    /**
     * Waits for the count to reach zero.
     *
     * @param timeoutMs below 0 waits indefinitely
     * @return true when it reached zero, false on timeout
     * @throws ProcessingMutexException when a change of leader invalidated the latch
     */
    public boolean await(String name, long count, long timeoutMs) throws InterruptedException {
        checkOpen();
        awaits.incrementAndGet();
        long startedAt = System.currentTimeMillis();
        long deadline = timeoutMs < 0 ? Long.MAX_VALUE : saturatedAdd(System.currentTimeMillis(), timeoutMs);

        // The latch is ensured to exist first -- a waiter may arrive before the first
        // countDown
        String error = declare(name, count);
        if (error != null) {
            throw new ProcessingMutexException(error);
        }
        long startGeneration = leaderGeneration.get();
        long startEpoch = 0L;

        try {
            while (true) {
                // The signal sequence is taken before registering. The other way round, a push
                // arriving between registering and suspending would be lost -- and without
                // polling, a lost push means waiting out the timeout
                long token = signals.token(name);

                SyncMessage r = exchange(SyncMessageType.LATCH_AWAIT, name, "", 0L, 0L);
                if (r != null && r.success()) {
                    if (r.state() == SyncState.SATISFIED) {
                        // Only a wait that reaches here counts towards the mean. A timed-out
                        // one always equals the configured timeout, and an invalidated one
                        // depends on when the leader died -- neither reflects how long the latch
                        // really took
                        satisfied.incrementAndGet();
                        waitMillis.addAndGet(System.currentTimeMillis() - startedAt);
                        return true;
                    }
                    if (startEpoch == 0L) {
                        startEpoch = r.epoch();
                    } else if (r.epoch() != startEpoch) {
                        throw invalidated(name, startEpoch, r.epoch());
                    }
                }
                // The leader has changed. This must come from a local event -- a dead leader
                // pushes nothing and asking reaches nobody, so without this the only outcome is
                // hanging until the timeout
                if (leaderGeneration.get() != startGeneration) {
                    throw invalidated(name, startEpoch, epoch);
                }

                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    awaitTimeouts.incrementAndGet();
                    return false;
                }
                // It hangs waiting for the push and does not poll. There are three ways to
                // wake: the leader pushed, this node observed a change of leader, or it timed
                // out
                signals.await(name, token, Math.min(remaining, maxWaitSliceMs));
            }
        } finally {
            // Having stopped waiting, it removes itself from the waiter list so the leader
            // stops pushing here
            exchange(SyncMessageType.LATCH_CANCEL, name, "", 0L, 0L);
        }
    }

    private ProcessingMutexException invalidated(String name, long from, long to) {
        invalidations.incrementAndGet();
        return new ProcessingMutexException("latch " + name + " is no longer valid: the leader "
                + "changed (epoch " + from + " -> " + to + "). A countDown carrying a "
                + "participantId lets every party report again and recover the count");
    }

    /** @return the count remaining, or -1 when it cannot be found */
    public long remaining(String name) {
        checkOpen();
        SyncMessage r = exchange(SyncMessageType.LATCH_AWAIT, name, "", 0L, 0L);
        return r != null && r.success() ? r.count() : -1L;
    }

    // ------------------------------------------------------------------
    // Sending requests
    // ------------------------------------------------------------------

    /**
     * Sends a request, and on failure <b>resends it under the same requestId</b>.
     *
     * <p>A latch's {@code countDown} is already idempotent at the register level, being
     * de-duplicated by party, but declaring and cancelling are not -- and resending also stops
     * a lost reply from failing a whole round.
     */
    private SyncMessage exchange(SyncMessageType type, String name, String participantId,
                                 long count, long generation) {
        long requestId = requestIdGen.incrementAndGet();
        for (int attempt = 0; attempt <= RESEND_ATTEMPTS; attempt++) {
            SyncMessage response = exchangeOnce(requestId, type, name, participantId,
                    count, generation);
            if (response != null) {
                return response;
            }
        }
        return null;
    }

    private SyncMessage exchangeOnce(long requestId, SyncMessageType type, String name,
                                     String participantId,
                                 long count, long generation) {
        Node leader = cluster.leader();
        if (leader == null) {
            log.debug("Leadership is vacant, so latch request {} cannot be handled yet", name);
            return null;
        }
        SyncMessage request = SyncMessage.request(type, requestId, epoch, name,
                participantId, count, generation);

        // This node is the leader: the local register is consulted directly, by exactly the
        // same logic as remotely
        if (leader.id().equals(cluster.self().id())) {
            return handleRequest(request, cluster.self());
        }

        CompletableFuture<SyncMessage> future = new CompletableFuture<>();
        pending.put(requestId, future);
        try {
            if (!cluster.unicastOn(CHANNEL, leader, request.encode())) {
                return null;
            }
            return future.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.debug("Latch request {} timed out waiting for the leader's reply", name);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            log.debug("Latch request {} failed: {}", name,
                    e.getCause() == null ? e : e.getCause().toString());
            return null;
        } finally {
            pending.remove(requestId);
        }
    }

    // ------------------------------------------------------------------
    // Inbound
    // ------------------------------------------------------------------

    /**
     * Which messages are buffered.
     *
     * <p>Replies and wake-ups both pass straight through: the first is a
     * {@code future.complete} and the second a {@code signals.signal}, both measured in
     * nanoseconds. The wake-up especially -- queueing it would wake every waiter a step
     * later.
     */
    @Override
    protected boolean shouldBuffer(Node sender, byte[] content) {
        SyncMessage msg = SyncMessage.decode(content);
        if (msg == null) {
            return false;
        }
        return msg.type() != SyncMessageType.RESPONSE && msg.type() != SyncMessageType.NOTIFY;
    }

    /** Handles one latch message. Requests run on the consumer thread while replies and
     *  wake-ups pass straight through; see {@link #shouldBuffer}. */
    @Override
    protected void handlePayload(Node sender, byte[] content) {
        SyncMessage msg = SyncMessage.decode(content);
        if (msg == null) {
            log.debug("An undecodable latch message arrived from {}", sender.label());
            return;
        }
        switch (msg.type()) {
            case RESPONSE -> {
                CompletableFuture<SyncMessage> f = pending.get(msg.requestId());
                if (f != null) {
                    f.complete(msg);
                }
            }
            // The push only accelerates: it wakes local waiters so they ask the leader sooner.
            // The real decision always rests on the leader's reply, so a lost push costs no
            // more than one waiting slice
            case NOTIFY -> signals.signal(msg.name());
            default -> {
                SyncMessage response = handleRequest(msg, sender);
                try {
                    cluster.unicastOn(CHANNEL, sender, response.encode());
                } catch (Exception e) {
                    log.debug("Failed to send the latch reply back to {}: {}", sender.label(),
                            e.toString());
                }
            }
        }
    }

    /** The buffer is full: what is dropped is a request, and the sender times out and
     *  retries. */
    @Override
    protected void onOverflow(Node sender, byte[] content) {
        log.warn("The latch request buffer is full; dropping a request from {} ({} so far)",
                sender.label(), dropped());
    }

    /**
     * The leader handles one latch request.
     *
     * <p>Declaring, counting down and cancelling all change state, so a resend goes through the
     * idempotence cache; waiting and querying must be current and do not.
     */
    private SyncMessage handleRequest(SyncMessage msg, Node sender) {
        if (hasSideEffect(msg.type())) {
            return requestIdempotence.execute(sender.id(), msg.requestId(),
                    () -> handleRequestOnce(msg, sender));
        }
        return handleRequestOnce(msg, sender);
    }

    /** The request types that change state. */
    private static boolean hasSideEffect(SyncMessageType type) {
        return type == SyncMessageType.LATCH_DECLARE
                || type == SyncMessageType.LATCH_COUNT_DOWN
                || type == SyncMessageType.LATCH_CANCEL;
    }

    private SyncMessage handleRequestOnce(SyncMessage msg, Node sender) {
        if (!cluster.isLeader()) {
            return SyncMessage.fail(msg.requestId(), epoch, msg.name(),
                    "this node is not the leader");
        }
        String name = msg.name();
        return switch (msg.type()) {
            case LATCH_DECLARE -> {
                String error = registry.declare(name, msg.count());
                yield error == null
                        ? state(msg, name)
                        : SyncMessage.fail(msg.requestId(), epoch, name, error);
            }
            case LATCH_COUNT_DOWN -> {
                long remaining = registry.countDown(name, msg.participantId());
                if (remaining < 0) {
                    yield SyncMessage.fail(msg.requestId(), epoch, name,
                            "no such latch; declare it first");
                }
                if (remaining == 0) {
                    // It reached zero, so every waiter is notified. Sent on another thread --
                    // a request is still being handled here, and sending waits synchronously
                    // for an ACK, blocking longer the more waiters there are
                    notifyWaiters(name);
                }
                yield state(msg, name);
            }
            case LATCH_AWAIT -> {
                registry.addWaiter(name, sender.id());
                yield state(msg, name);
            }
            case LATCH_CANCEL -> {
                registry.removeWaiter(name, sender.id());
                yield state(msg, name);
            }
            default -> SyncMessage.fail(msg.requestId(), epoch, name,
                    "not a legitimate request type: " + msg.type());
        };
    }

    private SyncMessage state(SyncMessage request, String name) {
        LatchEntry entry = registry.get(name);
        if (entry == null) {
            return SyncMessage.fail(request.requestId(), epoch, name, "no such latch");
        }
        return SyncMessage.ok(request.requestId(), epoch, name, entry.remaining(), 0L,
                entry.isSatisfied() ? SyncState.SATISFIED : SyncState.PENDING);
    }

    private void notifyWaiters(String name) {
        Set<String> waiters = registry.waiters(name);
        if (waiters.isEmpty()) {
            return;
        }
        long currentEpoch = epoch;
        notifier.execute(() -> {
            SyncMessage push = SyncMessage.notify(currentEpoch, name, 0L, SyncState.SATISFIED);
            byte[] encoded = push.encode();
            for (Node member : cluster.members()) {
                if (!waiters.contains(member.id())) {
                    continue;
                }
                if (member.id().equals(cluster.self().id())) {
                    signals.signal(name);   // this node is waiting too, so wake it directly
                    continue;
                }
                try {
                    cluster.unicastOn(CHANNEL, member, encoded);
                } catch (Exception e) {
                    // A lost push does no harm; the peer's re-check catches it
                    log.debug("Failed to push the completion of latch {} to {}: {}",
                            name, member.label(), e.toString());
                }
            }
        });
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
            log.info("This node became the latch leader; the register was reset at epoch={}",
                    epoch);
        } else {
            registry.clear();
        }
        // The local generation advances before waking, so a woken thread compares and finds
        // what it waited on invalid. The other way round, a woken thread might compare before
        // the generation updates and go back to sleep
        leaderGeneration.incrementAndGet();
        signals.signalAll();
    }

    /**
     * The leader has gone and the cluster is briefly without one.
     *
     * <p>Waiters <b>must be woken at once</b>: no push will arrive from here on, and with no
     * polling to fall back on, leaving them asleep means each hangs until its own timeout.
     */
    @Override
    public void onLeaderLeft(Node node) {
        leaderGeneration.incrementAndGet();
        signals.signalAll();
    }

    /**
     * A member departed.
     *
     * <p><b>Only its waiter registration is removed; the count is untouched</b> -- the
     * count-downs it made are accomplished facts. This is the opposite of the lock: a lock's
     * departing holder must release the lock at once, whereas moving a latch's count back would
     * leave the remaining parties waiting for a zero that never comes.
     */
    @Override
    public void onNodeLeft(Node node, boolean graceful) {
        if (cluster.isLeader()) {
            registry.onNodeLeft(node.id());
        }
    }

    // ------------------------------------------------------------------

    private void evictIdle() {
        if (!closed && cluster.isLeader()) {
            int n = registry.evictIdle(idleTimeoutMs);
            if (n > 0) {
                log.debug("Swept {} idle latch(es)", n);
            }
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new ProcessingMutexException("the latch service is closed");
        }
    }

    private static long saturatedAdd(long a, long b) {
        long sum = a + b;
        return ((a ^ sum) & (b ^ sum)) < 0 ? Long.MAX_VALUE : sum;
    }

    /**
     * Runtime figures.
     *
     * <h2>How to read them</h2>
     * <ul>
     *   <li>A persistently non-zero {@code awaitTimeouts} <b>is a fault</b>: waiters timed out
     *       with the latch still above zero, usually because some party never called countDown
     *       -- it died, or an exception path missed it</li>
     *   <li>{@code invalidations} counts <b>invalidations caused by a change of leader</b>. A
     *       non-zero value means the leader is unsteady and waiters were forced to start again.
     *       Without a participantId on countDown, the count <b>cannot recover</b> from a change
     *       of leader</li>
     *   <li>{@code awaits - satisfied - awaitTimeouts - invalidations} is the waiters currently
     *       hanging. Rising without falling means someone waits for ever</li>
     *   <li>{@code avgWaitMillis} counts only the waits that <b>succeeded</b>. A timed-out one
     *       always equals the configured timeout, and mixing those in would make the mean a
     *       function of the configuration</li>
     * </ul>
     */
    @Override
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        long ok = satisfied.get();
        m.put("declares", declares.get());
        m.put("countDowns", countDowns.get());
        m.put("awaits", awaits.get());
        m.put("satisfied", ok);
        m.put("awaitTimeouts", awaitTimeouts.get());
        m.put("invalidations", invalidations.get());
        m.put("avgWaitMillis", ok == 0 ? 0d
                : Math.round(waitMillis.get() * 100.0 / ok) / 100.0);
        m.put("waitingNow", Math.max(0L, awaits.get() - ok
                - awaitTimeouts.get() - invalidations.get()));
        m.put("pendingRequests", pending.size());
        m.put("epoch", epoch);
        m.put("leaderGeneration", leaderGeneration.get());
        return m;
    }

    public long epoch() {
        return epoch;
    }

    /** For troubleshooting: a snapshot of the register, which holds anything only on the
     *  leader. */
    public Map<String, String> registrySnapshot() {
        return registry.snapshot();
    }
}
