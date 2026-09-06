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

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The cross-process barrier's core: arrivals converge on the leader, and once parties of them
 * have gathered they are released together and a new generation begins.
 *
 * <h2>How it differs from a latch</h2>
 * A latch is single-use, its count falling one way to zero; a barrier <b>can be reused</b>,
 * moving to a new generation each time it fills, ready for the next round. So a barrier suits
 * aligning progress round by round -- several processes handling data in phases, each waiting
 * for everyone else at the end of a phase before the next begins.
 *
 * <h2>A party is a thread</h2>
 * As in the JDK: one {@code await()} call is one arrival. So {@code parties=5} may be five
 * processes with one thread each, five threads in one process, or any mixture. A party is
 * identified by its node id plus its thread id.
 *
 * <h2>Breaking</h2>
 * By the JDK's semantics, as soon as one party can wait no longer, every other party must
 * <b>fail at once</b> rather than keep waiting -- the gathering they are waiting for can never
 * happen now. There are three ways in:
 * <ul>
 *   <li>A party's {@code await} times out or is interrupted</li>
 *   <li>The node a party lives on departs</li>
 *   <li>The leader changes, taking the register in its memory with it</li>
 * </ul>
 * All of them appear as {@link BrokenBarrierException}, exactly as in the single-machine
 * version. Once broken, {@link #reset} is needed before it can be used again.
 *
 * <h2>It waits for pushes and does not poll</h2>
 * Exactly the latch's mechanism: a waiter hangs, and the leader pushes when it releases or
 * breaks. <b>There is no periodic polling</b> -- with many waiters, polling is pure wasted
 * traffic. A lost push leaves a waiter waiting until its own await times out, so the timeout
 * should be set for the worst case.
 *
 * <p>The one thing a push cannot cover is <b>a change of leader</b>: a dead leader pushes
 * nothing more. That case is woken by the cluster events this node receives; see
 * {@code leaderGeneration}.
 *
 * <h2>What not to use it for</h2>
 * Its safety does not exceed the strength of "there is one leader". Under a network partition
 * each side has a leader, each gathers a full set, and the same round is released twice.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class BarrierService extends BufferedGossipListener
        implements MultiProcessingService, SelfRegisteringListener {

    private static final Logger log = LoggerFactory.getLogger(BarrierService.class);

    /**
     * How many times an arrival request is resent at most.
     *
     * <p>Two -- three attempts including the first -- covers occasional packet loss, and costs
     * at worst another {@code 2 x requestTimeoutMs}. Where the leader really is unreachable,
     * two extra attempts cost far less than every other party waiting out its timeout.
     */
    private static final int RESEND_ATTEMPTS = 2;

    /** The barrier protocol's own channel. */
    public static final String CHANNEL = "spreader.barrier";

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
    private final BarrierRegistry registry = new BarrierRegistry();

    /** Suspending and waking local waiters. */
    private final SignalBox signals = new SignalBox();

    private final Map<Long, CompletableFuture<SyncMessage>> pending = new ConcurrentHashMap<>();
    private final AtomicLong requestIdGen = new AtomicLong();

    /**
     * The leader-side reply cache, so a resent arrival gets the previous reply rather than
     * arriving again.
     *
     * <p>It is the premise on which resending is safe. Without it, a resend goes wrong at the
     * boundary where the set has just filled: the leader has moved to a new generation and
     * cleared the register, and the resend records itself as the first party of the next
     * generation.
     */
    private final IdempotentRequestCache<SyncMessage> requestIdempotence =
            new IdempotentRequestCache<>(30_000L, 10_000);

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
     * <p>The same mechanism as the latch's: without polling, "the register is gone" can only be
     * discovered through the cluster events this node receives -- a dead leader pushes nothing
     * more, and asking during the vacancy reaches nobody.
     */
    private final AtomicLong leaderGeneration = new AtomicLong();

    // Runtime statistics
    private final AtomicLong declares = new AtomicLong();
    private final AtomicLong awaits = new AtomicLong();
    private final AtomicLong tripped = new AtomicLong();
    private final AtomicLong broken = new AtomicLong();
    private final AtomicLong awaitTimeouts = new AtomicLong();
    private final AtomicLong resets = new AtomicLong();
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
    public BarrierService(GossipCluster cluster, long requestTimeoutMs, long idleTimeoutMs, ExecutorServiceHolder executors) {
        // A buffer of 4096: a barrier's request volume is small to begin with, at most parties
        // per round, and this leaves headroom for a great many barriers opening a round at once.
        // One consumer thread: handling a request changes the register and is serial anyway,
        // more threads would only add lock contention, and serialising also handles requests in
        // arrival order
        super("barrier", 4096, 1, LoggerFactory.getLogger("spreader.barrier"));
        this.cluster = cluster;
        this.requestTimeoutMs = requestTimeoutMs;
        this.idleTimeoutMs = idleTimeoutMs;
        this.maxWaitSliceMs = resolveWaitSlice(cluster);

        this.notifier = executors.forBarrierNotify();
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
        log.info("Cross-process barrier service started: request timeout={}ms, idle "
                + "reclaim={}ms", requestTimeoutMs, idleTimeoutMs);
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
                new ProcessingMutexException("the barrier service is closed")));
        pending.clear();
        signals.clear();
        registry.clear();
    }

    // ------------------------------------------------------------------
    // The public surface
    // ------------------------------------------------------------------

    /** @return the reason for failure, or null on success */
    public String declare(String name, int parties) {
        checkOpen();
        declares.incrementAndGet();
        SyncMessage r = exchange(SyncMessageType.BARRIER_QUERY, name, "", parties, 0L);
        if (r == null) {
            return "the leader is temporarily unavailable";
        }
        return r.success() ? null : r.message();
    }

    /**
     * Arrives at the barrier and waits for the other parties.
     *
     * @param timeoutMs below 0 waits indefinitely
     * @return the arrival index; {@code parties - 1} means arriving last
     * @throws BrokenBarrierException when the barrier was broken -- a party timed out, was
     *                                interrupted or departed, or the leader changed
     * @throws TimeoutException       when the wait timed out. <b>Note that this breaks the
     *                                barrier too</b>, as in the JDK
     */
    public long await(String name, int parties, long timeoutMs)
            throws InterruptedException, BrokenBarrierException, TimeoutException {
        checkOpen();
        long deadline = timeoutMs < 0 ? Long.MAX_VALUE : saturatedAdd(System.currentTimeMillis(), timeoutMs);
        String participantId = participantId();

        awaits.incrementAndGet();
        long startedAt = System.currentTimeMillis();
        try {
            long arrival = awaitOnce(name, participantId, parties, deadline);
            // Only a genuine release counts towards the mean. The durations of a break or a
            // timeout do not reflect how long a round takes to gather, and mixing them in would
            // make the figure unreadable
            tripped.incrementAndGet();
            waitMillis.addAndGet(System.currentTimeMillis() - startedAt);
            return arrival;
        } catch (BrokenBarrierException e) {
            // The barrier was broken: someone timed out, was interrupted, departed, or the
            // leader changed. A different thing from timing out here -- this one means another
            // party has a problem
            broken.incrementAndGet();
            throw e;
        } catch (TimeoutException e) {
            awaitTimeouts.incrementAndGet();
            throw e;
        }
    }

    /** One complete round of arriving and waiting. */
    private long awaitOnce(String name, String participantId, int parties, long deadline)
            throws InterruptedException, BrokenBarrierException, TimeoutException {

        // The signal sequence must be taken before the arrival request is sent. The other way
        // round there is a race that cannot be won: between my arrival and taking the sequence,
        // the last party arrives and the leader pushes "released" -- and that push is lost, with
        // no polling to fall back on, so I hang until my own timeout. That is exactly how it
        // hung in testing: an occasional party of a three-party barrier waiting the full 20
        // seconds
        long token = signals.token(name);
        long startLeaderGen = leaderGeneration.get();

        SyncMessage first = exchangeWithResend(SyncMessageType.BARRIER_AWAIT, name,
                participantId, parties, 0L, deadline);
        if (first == null) {
            // Several resends without a reply means the leader really is unreachable.
            //
            // Note that "the request may have arrived after all" need not worry us here: the
            // resends above reuse the same requestId, and the leader's idempotence cache returns
            // the previous reply. The "it arrived and the reply was lost" path is already
            // closed
            throw new BrokenBarrierException();
        }
        if (!first.success()) {
            throw new ProcessingMutexException(first.message());
        }
        if (first.state() == SyncState.BROKEN) {
            throw new BrokenBarrierException();
        }
        if (first.state() == SyncState.SATISFIED) {
            // Last to arrive, so it is released directly with no waiting
            return first.count();
        }

        long myGeneration = first.generation();
        long myIndex = first.count();
        long startEpoch = first.epoch();

        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                // A party that times out breaks the barrier: the gathering the others are
                // waiting for can no longer happen, so they fail at once rather than hanging
                // alongside until morning. This matches the JDK's semantics
                exchange(SyncMessageType.BARRIER_LEAVE, name, participantId, parties, myGeneration);
                throw new TimeoutException("timed out waiting on barrier " + name);
            }
            if (leaderGeneration.get() != startLeaderGen) {
                // Any movement in the leadership voids this round -- a new leader's register is
                // a blank sheet, and waiting on is pointless.
                //
                // "Decide by the leader's id whether it really changed" was tried, to avoid
                // needless voiding during a wobble, and measured worse: for the seconds
                // leadership is vacant nobody can be asked, and a waiter only spins until its
                // timeout. Voiding and starting again is right, and await handles the starting
                // again
                throw new BrokenBarrierException();
            }
            try {
                signals.await(name, token, Math.min(remaining, maxWaitSliceMs));
            } catch (InterruptedException e) {
                // An interrupted party breaks the barrier too
                exchange(SyncMessageType.BARRIER_LEAVE, name, participantId, parties, myGeneration);
                throw e;
            }

            // The next round's sequence is taken before the query too, for the reason above
            token = signals.token(name);
            if (leaderGeneration.get() != startLeaderGen) {
                throw new BrokenBarrierException();
            }

            SyncMessage r = exchange(SyncMessageType.BARRIER_QUERY, name, participantId, parties, myGeneration);
            if (r == null || !r.success()) {
                // The request itself failed -- a lost packet, or vacant leadership. Going back
                // to wait for a push would be wrong here: that push may already have been sent
                // during this failed round trip, and waiting would waste an entire timeout.
                //
                // Note that this is not "polling the state" but "retrying a failed request", two
                // different things: the first asks repeatedly while the leader says nothing, the
                // second is a message that never arrived at all. Under UDP this path is
                // normal.
                sleepBriefly(remaining);
                continue;
            }
            if (r.epoch() != startEpoch) {
                // The register has turned over and this round's arrivals are gone. The same
                // thing as a change of leader, and likewise a matter of starting again rather
                // than failing the application
                throw new BrokenBarrierException();
            }
            if (r.state() == SyncState.BROKEN) {
                throw new BrokenBarrierException();
            }
            if (r.generation() > myGeneration) {
                // The current generation is beyond the one I entered on, so my generation
                // gathered and was released
                return myIndex;
            }
        }
    }

    /**
     * Pauses before retrying a failed request.
     *
     * <p>Without the pause it becomes a busy wait, capable of saturating a CPU while leadership
     * is vacant. The bound is what remains of the time, so the caller's timeout is never
     * overrun.
     */
    private static void sleepBriefly(long remainingMs) throws InterruptedException {
        Thread.sleep(Math.max(1L, Math.min(50L, remainingMs)));
    }

    /** Resets: the current generation is discarded and every waiting party receives
     *  "broken". */
    public void reset(String name, int parties) {
        checkOpen();
        resets.incrementAndGet();
        exchange(SyncMessageType.BARRIER_RESET, name, "", parties, 0L);
    }

    /** @return the current state, or null when it cannot be found */
    public SyncMessage query(String name, int parties) {
        checkOpen();
        return exchange(SyncMessageType.BARRIER_QUERY, name, "", parties, 0L);
    }

    /**
     * A party's identifier: node id plus thread id.
     *
     * <p>Counting by thread matches the JDK -- five threads in one process each calling await
     * are five parties. The node id is the prefix so that a departing node's parties can all be
     * found.
     */
    private String participantId() {
        return cluster.self().id() + "#" + Thread.currentThread().getId();
    }

    // ------------------------------------------------------------------
    // Sending requests
    // ------------------------------------------------------------------

    private SyncMessage exchange(SyncMessageType type, String name, String participantId,
                                 long parties, long generation) {
        return exchangeOnce(requestIdGen.incrementAndGet(), type, name, participantId,
                parties, generation);
    }

    /**
     * Sends a request, and on failure <b>resends it under the same requestId</b>.
     *
     * <h2>Why the requestId must be reused</h2>
     * "The request never arrived" and "it arrived and the reply was lost" look identical here.
     * Resending the second under a new id would have the leader treat it as an entirely new
     * arrival -- when it had already recorded me the first time.
     *
     * <p>Reusing the id lets the leader's idempotence cache ({@link #requestIdempotence})
     * recognise the resend and return the previous reply as it was. Both cases are thereby
     * fixed, rather than one being chosen over the other.
     *
     * <p>It is used for <b>arrivals</b> only. A query is idempotent to begin with and the next
     * turn of the loop asks again after a timeout, while LEAVE and RESET are best-effort
     * notifications not worth waiting out two more timeouts for.
     */
    private SyncMessage exchangeWithResend(SyncMessageType type, String name,
                                           String participantId, long parties, long generation,
                                           long deadline) {
        long requestId = requestIdGen.incrementAndGet();
        for (int attempt = 0; attempt <= RESEND_ATTEMPTS; attempt++) {
            if (attempt > 0 && System.currentTimeMillis() >= deadline) {
                return null;
            }
            SyncMessage response = exchangeOnce(requestId, type, name, participantId,
                    parties, generation);
            if (response != null) {
                if (attempt > 0) {
                    log.debug("The request for barrier {} received a reply after {} resend(s)",
                            name, attempt);
                }
                return response;
            }
        }
        return null;
    }

    private SyncMessage exchangeOnce(long requestId, SyncMessageType type, String name,
                                     String participantId, long parties, long generation) {
        Node leader = cluster.leader();
        if (leader == null) {
            log.debug("Leadership is vacant, so barrier request {} cannot be handled yet",
                    name);
            return null;
        }
        SyncMessage request = SyncMessage.request(type, requestId, epoch, name,
                participantId, parties, generation);

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
            log.debug("Barrier request {} timed out waiting for the leader's reply", name);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            log.debug("Barrier request {} failed: {}", name,
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
     * <h2>Replies and wake-ups must pass straight through</h2>
     * Handling either is a matter of nanoseconds -- a {@code future.complete} and a
     * {@code signals.signal} -- and <b>their latency decides the barrier's performance
     * outright</b>: NOTIFY is the "everyone may go" signal, and queueing it once delays every
     * waiter's release by a step.
     *
     * <p>A thread switch for such an operation is a pure loss: the switch costs an order of
     * magnitude more than the work.
     *
     * <h2>Requests are buffered</h2>
     * A request consults the register and may trigger a release and a broadcast, which is the
     * part that does real work. Buffered, they no longer hold the dispatch thread up.
     */
    @Override
    protected boolean shouldBuffer(Node sender, byte[] content) {
        SyncMessage msg = SyncMessage.decode(content);
        if (msg == null) {
            return false;
        }
        return msg.type() != SyncMessageType.RESPONSE && msg.type() != SyncMessageType.NOTIFY;
    }

    /**
     * Handles one barrier message.
     *
     * <p>Requests run on the buffer's consumer thread; replies and wake-ups pass straight
     * through, on the dispatch thread. See {@link #shouldBuffer} for the distinction.
     */
    @Override
    protected void handlePayload(Node sender, byte[] content) {
        SyncMessage msg = SyncMessage.decode(content);
        if (msg == null) {
            log.debug("An undecodable barrier message arrived from {}", sender.label());
            return;
        }
        switch (msg.type()) {
            case RESPONSE -> {
                CompletableFuture<SyncMessage> f = pending.get(msg.requestId());
                if (f != null) {
                    f.complete(msg);
                }
            }
            case NOTIFY -> signals.signal(msg.name());
            default -> {
                SyncMessage response = handleRequest(msg, sender);
                try {
                    cluster.unicastOn(CHANNEL, sender, response.encode());
                } catch (Exception e) {
                    log.debug("Failed to send the barrier reply back to {}: {}", sender.label(),
                            e.toString());
                }
            }
        }
    }

    /**
     * The buffer is full.
     *
     * <p>What is dropped is a <b>request</b>; replies and wake-ups pass straight through and
     * never reach here. A dropped request means the sender times out without a reply and
     * resends -- so it is not a disaster, but happening repeatedly means this node can no
     * longer keep up.
     */
    @Override
    protected void onOverflow(Node sender, byte[] content) {
        log.warn("The barrier request buffer is full; dropping a request from {} ({} so far). "
                + "The peer will time out and resend, but this node is no longer keeping up",
                sender.label(), dropped());
    }

    /**
     * The leader handles one barrier request.
     *
     * <h2>Arrivals go through the idempotence cache</h2>
     * An arrival has side effects -- it records the party and may trigger a release -- and a
     * caller resends when a reply is lost. Without idempotence, a resend at the boundary where
     * the set has just filled records the party as <b>the first of the next generation</b>,
     * where it hangs waiting for a group that passed long ago.
     *
     * <p>The other request types are either idempotent to begin with, as queries are, or
     * best-effort notifications, as breaking and resetting are, so neither needs this
     * protection nor occupies the cache.
     */
    private SyncMessage handleRequest(SyncMessage msg, Node sender) {
        if (msg.type() == SyncMessageType.BARRIER_AWAIT) {
            return requestIdempotence.execute(sender.id(), msg.requestId(),
                    () -> handleRequestOnce(msg, sender));
        }
        return handleRequestOnce(msg, sender);
    }

    private SyncMessage handleRequestOnce(SyncMessage msg, Node sender) {
        if (!cluster.isLeader()) {
            return SyncMessage.fail(msg.requestId(), epoch, msg.name(),
                    "this node is not the leader");
        }
        String name = msg.name();
        int parties = (int) msg.count();

        String error = registry.declare(name, parties);
        if (error != null) {
            return SyncMessage.fail(msg.requestId(), epoch, name, error);
        }
        BarrierEntry entry = registry.get(name);

        return switch (msg.type()) {
            case BARRIER_AWAIT -> {
                long index = entry.arrive(msg.participantId(), sender.id());
                if (index < 0) {
                    yield SyncMessage.ok(msg.requestId(), epoch, name, 0L,
                            entry.generation(), SyncState.BROKEN);
                }
                boolean tripped = index == parties - 1L;
                if (tripped) {
                    // The set has filled, so every waiter is woken. Sent on another thread --
                    // a request is still being handled here, and sending waits synchronously
                    // for an ACK
                    notifyWaiters(name);
                }
                yield SyncMessage.ok(msg.requestId(), epoch, name, index, entry.generation(),
                        tripped ? SyncState.SATISFIED : SyncState.PENDING);
            }
            case BARRIER_LEAVE -> {
                entry.breakBarrier("party " + msg.participantId()
                        + " left partway through, having timed out or been interrupted");
                notifyWaiters(name);
                yield SyncMessage.ok(msg.requestId(), epoch, name, entry.arrivedCount(),
                        entry.generation(), SyncState.BROKEN);
            }
            case BARRIER_RESET -> {
                entry.reset();
                notifyWaiters(name);
                yield SyncMessage.ok(msg.requestId(), epoch, name, 0L,
                        entry.generation(), SyncState.PENDING);
            }
            case BARRIER_QUERY -> SyncMessage.ok(msg.requestId(), epoch, name,
                    entry.arrivedCount(), entry.generation(),
                    entry.isBroken() ? SyncState.BROKEN : SyncState.PENDING);
            default -> SyncMessage.fail(msg.requestId(), epoch, name,
                    "not a legitimate request type: " + msg.type());
        };
    }

    /**
     * Wakes everyone waiting on this barrier.
     *
     * <p>It <b>broadcasts to every member</b> rather than pushing precisely to the waiters: the
     * moment the barrier releases, the arrival list is cleared and the precise list no longer
     * exists. A few useless pushes are cheap -- a recipient that is not waiting has no
     * corresponding object in its {@code SignalBox} and simply returns -- while missing one
     * costs the peer another re-check interval.
     */
    private void notifyWaiters(String name) {
        long currentEpoch = epoch;
        notifier.execute(() -> {
            signals.signal(name);   // those waiting in this process
            try {
                cluster.multicastOn(CHANNEL, null,
                        SyncMessage.notify(currentEpoch, name, 0L, SyncState.SATISFIED).encode());
            } catch (Exception e) {
                // A lost push does no harm; the peer's re-check catches it
                log.debug("Failed to push the state change of barrier {}: {}", name,
                        e.toString());
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
            log.info("This node became the barrier leader; the register was reset at epoch={}",
                    epoch);
        } else {
            registry.clear();
        }
        // With the register cleared, the cached replies mean nothing -- keeping them would let
        // a resend after a change of leader receive an answer belonging to the previous term
        requestIdempotence.clear();
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
     * A member departed: the parties still waiting on it can never arrive, so every barrier
     * involved is broken.
     *
     * <p>The opposite of the latch -- a latch party departing does not affect counts already
     * reported, while a barrier requires everyone <b>at once</b>, and one party short means the
     * round cannot complete.
     */
    @Override
    public void onNodeLeft(Node node, boolean graceful) {
        if (!cluster.isLeader()) {
            return;
        }
        List<String> broken = registry.onNodeLeft(node.id());
        if (!broken.isEmpty()) {
            log.info("Member {} departed; the {} barrier(s) it took part in were broken: {}",
                    node.label(), broken.size(), broken);
            broken.forEach(this::notifyWaiters);
        }
    }

    // ------------------------------------------------------------------

    private void evictIdle() {
        if (!closed && cluster.isLeader()) {
            int n = registry.evictIdle(idleTimeoutMs);
            if (n > 0) {
                log.debug("Swept {} idle barrier(s)", n);
            }
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new ProcessingMutexException("the barrier service is closed");
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
     *   <li>{@code broken} and {@code awaitTimeouts} <b>must be read apart</b>: the first means
     *       another party had a problem -- someone timed out, was interrupted, departed, or the
     *       leader changed -- and the second means this party timed out. A barrier's typical
     *       failure is agreeing on N parties and receiving N-1, which shows up as a spread of
     *       broken rather than of timeout</li>
     *   <li>{@code tripped} counts successful releases. A round of an N-party barrier returns
     *       from N calls, so {@code tripped} counts calls rather than rounds -- divide by the
     *       party count for rounds</li>
     *   <li>{@code avgWaitMillis} counts only the releases. It reflects how slow <b>the slowest
     *       party</b> is, since everyone waits for all of them</li>
     *   <li>A non-zero {@code resets} means something is deliberately discarding the current
     *       generation, and the waiters all receive broken</li>
     * </ul>
     */
    @Override
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        long ok = tripped.get();
        m.put("declares", declares.get());
        m.put("awaits", awaits.get());
        m.put("tripped", ok);
        m.put("broken", broken.get());
        m.put("awaitTimeouts", awaitTimeouts.get());
        m.put("resets", resets.get());
        m.put("avgWaitMillis", ok == 0 ? 0d
                : Math.round(waitMillis.get() * 100.0 / ok) / 100.0);
        m.put("waitingNow", Math.max(0L, awaits.get() - ok
                - broken.get() - awaitTimeouts.get()));
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
