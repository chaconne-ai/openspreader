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
import com.chaconneai.openspreader.cluster.SelfRegisteringListener;
import com.chaconneai.openspreader.concurrent.ExecutorServiceHolder;
import com.chaconneai.openspreader.idempotence.IdempotentRequestCache;
import com.chaconneai.openspreader.serialization.ObjectCodec;
import com.chaconneai.spreader.GossipCluster;
import com.chaconneai.spreader.Node;
import com.chaconneai.spreader.event.BufferedGossipListener;
import com.chaconneai.spreader.transport.TransportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The cross-process exchanger's core: two parties meet under one name on the leader and swap
 * items.
 *
 * <h2>How it differs from a barrier</h2>
 * A barrier gathers N parties and tells them all that everyone arrived; it carries nothing.
 * An exchanger pairs <b>exactly two</b> and <b>moves data between them</b> -- the rendezvous
 * is only the means. That one difference shapes everything below: the protocol needs a
 * payload of its own, pairing is a two-sided thing rather than a count, and the failure
 * modes have to be judged by what happens to the item rather than by who is left waiting.
 *
 * <h2>The two sides are not symmetrical</h2>
 * The party arriving <b>second</b> triggers the pairing and takes its partner's item back in
 * the reply to its own ARRIVE, so its exchange costs <b>one round trip</b>. The party that
 * arrived <b>first</b> is asleep, and its item sits on the leader until it wakes and sends
 * COLLECT. So a complete exchange costs three messages to the leader, not two.
 *
 * <p>Leaving the item on the leader rather than pushing it is deliberate: a push that goes
 * missing then costs only a delay, because the item is still where the sleeper can ask for
 * it. Pushing the item would make every lost push a lost item.
 *
 * <h2>Resending is what keeps items from being lost</h2>
 * An ARRIVE whose reply went missing is the dangerous case: the leader may already have
 * paired and handed this party's item to a partner, and a naive resend under a fresh id
 * would park the party in the slot a second time while its incoming item sat uncollected.
 *
 * <p>So an ARRIVE is resent <b>under the same requestId</b>, and the leader's
 * {@link #requestIdempotence} returns the original reply -- <b>including the partner's item
 * inside it</b>. That cache is not an optimisation here; without it a lost reply is a lost
 * item.
 *
 * <h2>It waits for pushes and does not poll</h2>
 * The same mechanism as the latch and the barrier: a waiter parks, and the leader pushes
 * when a partner arrives. The push is a <b>unicast to the one node the sleeper is on</b>,
 * not a broadcast: unlike a barrier, which clears its arrival list the moment it trips,
 * pairing knows exactly who the sleeper is, so there is nothing to gain from telling the
 * whole cluster.
 *
 * <p>A bounded wait slice backs the push up; see {@link #resolveWaitSlice}. A lost push then
 * costs one slice, not the caller's whole timeout.
 *
 * <h2>What is not guaranteed</h2>
 * Its safety does not exceed the strength of "there is one leader".
 * <ul>
 *   <li>Under a network partition each side has a leader and pairs within itself, so two
 *       parties that expected to meet may each meet somebody else</li>
 *   <li>A change of leader loses an exchange that had <b>paired but not yet been
 *       collected</b>: the item lived in the old leader's memory. A party that had not yet
 *       paired is safe, leaves with {@link ProcessingExchangerException}, and still holds its
 *       own item</li>
 * </ul>
 * An exchange is a rendezvous, not a transaction.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class ExchangerService extends BufferedGossipListener
        implements MultiProcessingService, SelfRegisteringListener {

    private static final Logger log = LoggerFactory.getLogger(ExchangerService.class);

    /**
     * How many times an ARRIVE is resent at most.
     *
     * <p>Two -- three attempts including the first -- as for the barrier. It matters more
     * here: the reply to an ARRIVE may be carrying the partner's item, so giving up early
     * risks losing it rather than merely failing a wait.
     */
    private static final int RESEND_ATTEMPTS = 2;

    /** The exchanger protocol's own channel. */
    public static final String CHANNEL = "spreader.exchanger";

    private static final byte[] NO_PAYLOAD = new byte[0];

    private final GossipCluster cluster;
    private final ObjectCodec codec;
    private final long requestTimeoutMs;
    private final long idleTimeoutMs;

    /**
     * How long one suspension may last before waking to ask again.
     *
     * <p>The same reasoning as the barrier's, and the same values. It is not "polling by
     * default" but "covering for a push that went missing": under TCP that is rare and
     * looking back every two seconds costs nothing, while under UDP a datagram may simply be
     * dropped and 500ms suits it better.
     */
    private final long maxWaitSliceMs;

    /** Used only while this node is the leader. */
    private final ExchangerRegistry registry = new ExchangerRegistry();

    /** Suspending and waking local waiters. */
    private final SignalBox signals = new SignalBox();

    private final Map<Long, CompletableFuture<ExchangeMessage>> pending = new ConcurrentHashMap<>();
    private final AtomicLong requestIdGen = new AtomicLong();

    /**
     * The leader-side reply cache, so a resent ARRIVE gets the previous reply rather than
     * arriving again.
     *
     * <p>See the class comment: for the exchanger this is a <b>correctness</b> mechanism, not
     * a performance one. The cached reply may hold the partner's item, and it is the only
     * copy.
     */
    private final IdempotentRequestCache<ExchangeMessage> requestIdempotence =
            new IdempotentRequestCache<>(30_000L, 10_000);

    private volatile long epoch;

    /**
     * Who the last known leader was.
     *
     * <p>As in the lock, latch and barrier services: whether the leader <b>really</b> changed
     * must be decided by identity rather than by the event arriving. The "leader changed"
     * event is emitted to every node after the quiet period even when the leader has been the
     * same node throughout, and treating that as a change would silently void every exchange
     * in flight during startup.
     */
    private volatile String lastLeaderId;

    /**
     * How many changes of leader this node has observed.
     *
     * <p>Without polling, "the register is gone" can only be discovered through the cluster
     * events this node receives -- a dead leader pushes nothing more, and asking during the
     * vacancy reaches nobody.
     */
    private final AtomicLong leaderGeneration = new AtomicLong();

    // Runtime statistics
    private final AtomicLong arrivals = new AtomicLong();
    private final AtomicLong exchanges = new AtomicLong();
    private final AtomicLong pairings = new AtomicLong();
    private final AtomicLong timeouts = new AtomicLong();
    /**
     * Calls that left without an item for a reason other than running out of time: the
     * mechanism failed, or the thread was interrupted.
     *
     * <p>It exists so that {@code waitingNow} can be arithmetic rather than a guess. Every
     * call leaves by exactly one of four doors, and a door nobody counts makes "how many are
     * blocked right now" drift upwards for ever.
     */
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong lateExchanges = new AtomicLong();
    private final AtomicLong invalidations = new AtomicLong();
    private final AtomicLong waitMillis = new AtomicLong();

    /**
     * A single thread devoted to pushes.
     *
     * <p>It must not be shared with the buffer's consumer thread, for the reason written out
     * at length on {@code BarrierService}: sending waits synchronously for an ACK, and the
     * push is started while an inbound request is being handled. Sharing one pool, the push
     * task queues behind the very threads it is meant to release.
     */
    private final ExecutorService notifier;

    private final ExecutorServiceHolder executors;

    private volatile boolean closed;

    /**
     * @param codec     how an item becomes bytes. The whole cluster must be configured the
     *                  same way, or neither side can read what the other handed over
     * @param executors the scheduled-task pools. Spring's configuration passes the shared one
     *                  in, so the container manages the lifecycle and Actuator can see it
     */
    public ExchangerService(GossipCluster cluster, ObjectCodec codec, long requestTimeoutMs,
                            long idleTimeoutMs, ExecutorServiceHolder executors) {
        // A buffer of 1024 rather than the barrier's 4096: every entry here carries a
        // serialised item, so the queue's memory is set by the items rather than by the
        // message headers, and a few thousand large items back up into a great deal of heap.
        // One consumer thread: pairing mutates the register and is serial anyway, and
        // serialising also pairs parties in the order they arrive
        super("exchanger", 1024, 1, LoggerFactory.getLogger("spreader.exchanger"));
        this.cluster = cluster;
        this.codec = codec;
        this.requestTimeoutMs = requestTimeoutMs;
        this.idleTimeoutMs = idleTimeoutMs;
        this.maxWaitSliceMs = resolveWaitSlice(cluster);
        this.notifier = executors.forExchangerNotify();
        this.executors = executors;
    }

    /**
     * The upper bound on a suspension: the longest interval before waking to check.
     *
     * <p>Identical to the barrier's, and for the identical reason. TCP guarantees "delivered
     * or reported", and a push that fails to send is swallowed inside {@link #notifyPartner}
     * with only a debug line -- so TCP needs a backstop too, merely a wider one. Without it,
     * one failed push means one party waiting out its entire timeout while its item sits on
     * the leader.
     *
     * <p><b>This is not polling</b>: normally the push arrives and wakes the waiter, and this
     * bound is never reached.
     */
    private static long resolveWaitSlice(GossipCluster cluster) {
        return cluster.config().transportType() == TransportType.UDP ? 500L : 2_000L;
    }

    @Override
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
        executors.forMaintenance().scheduleWithFixedDelay(this::evictIdle, tick, tick,
                TimeUnit.MILLISECONDS);
        log.info("Cross-process exchanger service started: request timeout={}ms, idle "
                + "reclaim={}ms, serialization={}", requestTimeoutMs, idleTimeoutMs, codec.type());
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        cluster.removeListener(this);
        // executors and notifier are not closed: they belong to the container, and one service
        // shutting down should not take another's thread pools with it
        stopDispatch();
        pending.values().forEach(f -> f.completeExceptionally(
                new ProcessingExchangerException("the exchanger service is closed")));
        pending.clear();
        signals.clear();
        registry.clear();
    }

    // ------------------------------------------------------------------
    // The public surface
    // ------------------------------------------------------------------

    /**
     * Hands an item over and waits for a partner to hand one back.
     *
     * @param timeoutMs below 0 waits indefinitely
     * @return the partner's item
     * @throws TimeoutException when no partner arrived in time and <b>no pairing had
     *                          happened</b>. Where one had, this returns normally instead;
     *                          see {@link ExchangeEntry#cancel}
     */
    public Object exchange(String name, Object item, long timeoutMs)
            throws InterruptedException, TimeoutException {
        checkOpen();
        long deadline = timeoutMs < 0 ? Long.MAX_VALUE
                : saturatedAdd(System.currentTimeMillis(), timeoutMs);
        String participantId = participantId();
        byte[] payload = codec.encode(item);

        arrivals.incrementAndGet();
        long startedAt = System.currentTimeMillis();
        try {
            Object partnerItem = exchangeOnce(name, participantId, payload, deadline);
            exchanges.incrementAndGet();
            waitMillis.addAndGet(System.currentTimeMillis() - startedAt);
            return partnerItem;
        } catch (TimeoutException e) {
            timeouts.incrementAndGet();
            throw e;
        } catch (InterruptedException | RuntimeException e) {
            // Interrupted, or something went wrong. Counted together because the question
            // they answer is the same one: this call left empty-handed and is no longer
            // waiting.
            //
            // RuntimeException rather than ProcessingExchangerException alone, so that the
            // count stays exact. A deserialisation failure on a pairing that did succeed
            // leaves by this door too, and a door nobody counts makes waitingNow drift
            // upwards for ever
            failures.incrementAndGet();
            throw e;
        }
    }

    /** One complete round of handing over and waiting. */
    private Object exchangeOnce(String name, String participantId, byte[] payload, long deadline)
            throws InterruptedException, TimeoutException {

        // The signal sequence must be taken before the ARRIVE is sent. The other way round
        // there is a race that cannot be won: between handing over and taking the sequence, a
        // partner arrives and the leader pushes -- and that push is lost, with no polling to
        // fall back on, so this party sleeps until its own timeout while its item waits on the
        // leader
        long token = signals.token(name);
        long startLeaderGen = leaderGeneration.get();

        ExchangeMessage first = sendWithResend(ExchangeMessageType.ARRIVE, name, participantId,
                payload, deadline);
        if (first == null) {
            // Several resends without a reply. Note that "it arrived and the reply was lost"
            // need not worry us: the resends reuse one requestId and the leader's idempotence
            // cache would have returned the original reply, item and all
            throw new ProcessingExchangerException("no leader could be reached to exchange at "
                    + name + "; the item was not handed over");
        }
        if (!first.success()) {
            throw new ProcessingExchangerException(first.message());
        }
        if (first.state() == SyncState.SATISFIED) {
            // A partner was already waiting, so the exchange is done in one round trip
            return decode(first.payload());
        }
        if (first.state() == SyncState.BROKEN) {
            throw new ProcessingExchangerException("exchange point " + name
                    + " is unavailable: " + first.message());
        }

        long startEpoch = first.epoch();

        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                byte[] late = cancelAndTakeItem(name, participantId);
                if (late != null) {
                    // Paired in the instant before the cancel landed. The partner has already
                    // gone away with this party's item, so throwing here would drop the
                    // incoming half and leave the exchange half-completed
                    lateExchanges.incrementAndGet();
                    return decode(late);
                }
                throw new TimeoutException("timed out waiting for a partner at exchange point "
                        + name);
            }
            if (leaderGeneration.get() != startLeaderGen) {
                // The register lived in the old leader's memory and is gone. The item was
                // never handed to anybody, so the caller still has it and may try again
                invalidations.incrementAndGet();
                throw new ProcessingExchangerException("the leader changed while waiting at "
                        + "exchange point " + name + "; the item was not handed over");
            }
            try {
                signals.await(name, token, Math.min(remaining, maxWaitSliceMs));
            } catch (InterruptedException e) {
                byte[] late = cancelAndTakeItem(name, participantId);
                if (late != null) {
                    // The same reasoning as the timeout above: the pairing cannot be undone,
                    // so the item is returned and the interrupt is re-raised for the caller's
                    // next blocking call to find. Dropping an item to honour an interrupt
                    // punctually is the worse trade
                    Thread.currentThread().interrupt();
                    lateExchanges.incrementAndGet();
                    return decode(late);
                }
                throw e;
            }

            // The next round's sequence is taken before the query too, for the reason above
            token = signals.token(name);
            if (leaderGeneration.get() != startLeaderGen) {
                invalidations.incrementAndGet();
                throw new ProcessingExchangerException("the leader changed while waiting at "
                        + "exchange point " + name + "; the item was not handed over");
            }

            ExchangeMessage r = send(ExchangeMessageType.COLLECT, name, participantId, NO_PAYLOAD);
            if (r == null || !r.success()) {
                // The request itself failed -- a lost packet, or vacant leadership. This is
                // retrying a failed request, not polling a state: going back to sleep would be
                // wrong, because the push this party is waiting for may already have been sent
                // and missed during the failed round trip
                sleepBriefly(remaining);
                continue;
            }
            if (r.epoch() != startEpoch) {
                invalidations.incrementAndGet();
                throw new ProcessingExchangerException("the register at exchange point " + name
                        + " turned over while waiting; the item was not handed over");
            }
            if (r.state() == SyncState.SATISFIED) {
                return decode(r.payload());
            }
            if (r.state() == SyncState.BROKEN) {
                invalidations.incrementAndGet();
                throw new ProcessingExchangerException("exchange point " + name
                        + " no longer knows this party: " + r.message());
            }
        }
    }

    /**
     * Stops waiting, and takes the item back if a pairing had already happened.
     *
     * @return the partner's item when the pairing beat the cancel, or null when this party was
     *         simply removed from the slot
     */
    private byte[] cancelAndTakeItem(String name, String participantId) {
        ExchangeMessage r = send(ExchangeMessageType.CANCEL, name, participantId, NO_PAYLOAD);
        if (r != null && r.success() && r.state() == SyncState.SATISFIED) {
            return r.payload();
        }
        // A cancel that never arrived leaves this party in the leader's slot until a partner
        // arrives and pairs with a ghost, or until the record falls idle. It is not worth two
        // more round trips at the moment a caller has already run out of time: the partner's
        // own COLLECT finds nothing and its own timeout ends it
        return null;
    }

    /** @return the current state, or null when the leader cannot be reached */
    public ExchangeMessage query(String name) {
        checkOpen();
        return send(ExchangeMessageType.QUERY, name, "", NO_PAYLOAD);
    }

    /**
     * A party's identifier: node id plus thread id.
     *
     * <p>Counting by thread matches the JDK -- two threads in one process calling
     * {@code exchange} are two parties and pair with each other. The node id is the prefix so
     * that a departing node's parties can all be found.
     */
    private String participantId() {
        return cluster.self().id() + "#" + Thread.currentThread().getId();
    }

    private Object decode(byte[] payload) {
        // An empty payload is a null item, which the JDK's Exchanger permits too
        return payload == null || payload.length == 0 ? null : codec.decode(payload);
    }

    // ------------------------------------------------------------------
    // Sending requests
    // ------------------------------------------------------------------

    private ExchangeMessage send(ExchangeMessageType type, String name, String participantId,
                                 byte[] payload) {
        return sendOnce(requestIdGen.incrementAndGet(), type, name, participantId, payload);
    }

    /**
     * Sends a request, and on failure <b>resends it under the same requestId</b>.
     *
     * <p>Used for ARRIVE alone, and it is the mechanism that keeps items from being lost.
     * "The request never arrived" and "it arrived and the reply was lost" look identical from
     * here; under a fresh id the second case would hand the item over twice, while under the
     * same id the leader's idempotence cache returns the original reply exactly as it was,
     * partner's item included.
     *
     * <p>COLLECT is idempotent in effect and the loop asks again anyway; CANCEL and QUERY are
     * best-effort and not worth waiting out two more timeouts for.
     */
    private ExchangeMessage sendWithResend(ExchangeMessageType type, String name,
                                           String participantId, byte[] payload, long deadline) {
        long requestId = requestIdGen.incrementAndGet();
        for (int attempt = 0; attempt <= RESEND_ATTEMPTS; attempt++) {
            if (attempt > 0 && System.currentTimeMillis() >= deadline) {
                return null;
            }
            ExchangeMessage response = sendOnce(requestId, type, name, participantId, payload);
            if (response != null) {
                if (attempt > 0) {
                    log.debug("The request for exchange point {} received a reply after {} "
                            + "resend(s)", name, attempt);
                }
                return response;
            }
        }
        return null;
    }

    private ExchangeMessage sendOnce(long requestId, ExchangeMessageType type, String name,
                                     String participantId, byte[] payload) {
        Node leader = cluster.leader();
        if (leader == null) {
            log.debug("Leadership is vacant, so the request for exchange point {} cannot be "
                    + "handled yet", name);
            return null;
        }
        ExchangeMessage request = ExchangeMessage.request(type, requestId, epoch, name,
                participantId, payload);

        if (leader.id().equals(cluster.self().id())) {
            return handleRequest(request, cluster.self());
        }

        CompletableFuture<ExchangeMessage> future = new CompletableFuture<>();
        pending.put(requestId, future);
        try {
            if (!cluster.unicastOn(CHANNEL, leader, request.encode())) {
                return null;
            }
            return future.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.debug("The request for exchange point {} timed out waiting for the leader's "
                    + "reply", name);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            log.debug("The request for exchange point {} failed: {}", name,
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
     * <p>The same split as the barrier's. A reply or a wake-up is a {@code future.complete} or
     * a {@code signals.signal} -- nanoseconds of work whose latency decides how quickly a
     * sleeping party collects its item -- so queueing one behind a thread switch is a pure
     * loss. A request consults the register and may pair and push, which is the part that does
     * real work, so it is buffered and no longer holds the dispatch thread up.
     */
    @Override
    protected boolean shouldBuffer(Node sender, byte[] content) {
        ExchangeMessage msg = ExchangeMessage.decode(content);
        if (msg == null) {
            return false;
        }
        return msg.type() != ExchangeMessageType.RESPONSE
                && msg.type() != ExchangeMessageType.NOTIFY;
    }

    /**
     * Handles one exchanger message.
     *
     * <p>Requests run on the buffer's consumer thread; replies and wake-ups pass straight
     * through, on the dispatch thread. See {@link #shouldBuffer} for the distinction.
     */
    @Override
    protected void handlePayload(Node sender, byte[] content) {
        ExchangeMessage msg = ExchangeMessage.decode(content);
        if (msg == null) {
            log.debug("An undecodable exchanger message arrived from {}", sender.label());
            return;
        }
        switch (msg.type()) {
            case RESPONSE -> {
                CompletableFuture<ExchangeMessage> f = pending.get(msg.requestId());
                if (f != null) {
                    f.complete(msg);
                }
            }
            case NOTIFY -> signals.signal(msg.name());
            default -> {
                ExchangeMessage response = handleRequest(msg, sender);
                try {
                    cluster.unicastOn(CHANNEL, sender, response.encode());
                } catch (Exception e) {
                    log.debug("Failed to send the exchanger reply back to {}: {}",
                            sender.label(), e.toString());
                }
            }
        }
    }

    /**
     * The buffer is full.
     *
     * <p>What is dropped is a <b>request</b>; replies and wake-ups pass straight through and
     * never reach here. A dropped ARRIVE means the sender times out without a reply and
     * resends under the same id, so no item is lost -- but happening repeatedly means this
     * node can no longer keep up.
     */
    @Override
    protected void onOverflow(Node sender, byte[] content) {
        log.warn("The exchanger request buffer is full; dropping a request from {} ({} so far)."
                + " The peer will time out and resend, but this node is no longer keeping up",
                sender.label(), dropped());
    }

    /**
     * The leader handles one exchanger request.
     *
     * <h2>ARRIVE goes through the idempotence cache</h2>
     * It is the only type with a side effect that cannot be repeated, and the only one whose
     * reply may carry an item. Without the cache, a resend after a lost reply would park the
     * party in the slot a second time while the item meant for it sat uncollected on the
     * leader -- and nothing would report it.
     *
     * <p>COLLECT, CANCEL and QUERY are all safe to repeat: the first two remove what they
     * find and do nothing when there is nothing to find, and the third reads.
     */
    private ExchangeMessage handleRequest(ExchangeMessage msg, Node sender) {
        if (msg.type() == ExchangeMessageType.ARRIVE) {
            return requestIdempotence.execute(sender.id(), msg.requestId(),
                    () -> handleRequestOnce(msg, sender));
        }
        return handleRequestOnce(msg, sender);
    }

    private ExchangeMessage handleRequestOnce(ExchangeMessage msg, Node sender) {
        if (!cluster.isLeader()) {
            return ExchangeMessage.fail(msg.requestId(), epoch, msg.name(),
                    "this node is not the leader");
        }
        String name = msg.name();
        ExchangeEntry entry = registry.get(name);

        return switch (msg.type()) {
            case ARRIVE -> {
                ExchangeEntry.Result paired = entry.arrive(msg.participantId(), sender,
                        msg.payload());
                if (paired == null) {
                    yield ExchangeMessage.pending(msg.requestId(), epoch, name);
                }
                pairings.incrementAndGet();
                // The partner is asleep, so wake it to collect now rather than at its next
                // re-check. Its item is on the leader either way, so a lost push costs a delay
                // and nothing more
                notifyPartner(name, paired.node());
                yield ExchangeMessage.exchanged(msg.requestId(), epoch, name, paired.payload());
            }
            case COLLECT -> {
                ExchangeEntry.Result r = entry.collect(msg.participantId());
                if (r != null) {
                    yield ExchangeMessage.exchanged(msg.requestId(), epoch, name, r.payload());
                }
                if (entry.isWaiting(msg.participantId())) {
                    yield ExchangeMessage.pending(msg.requestId(), epoch, name);
                }
                // Neither holding the slot nor holding a result. The register has turned over,
                // or this party was cancelled and came back. Either way waiting on is
                // pointless, and saying so beats leaving it to a timeout
                yield ExchangeMessage.broken(msg.requestId(), epoch, name,
                        "this party is no longer registered at the exchange point");
            }
            case CANCEL -> {
                ExchangeEntry.Result r = entry.cancel(msg.participantId());
                yield r == null
                        ? ExchangeMessage.pending(msg.requestId(), epoch, name)
                        : ExchangeMessage.exchanged(msg.requestId(), epoch, name, r.payload());
            }
            // SATISFIED here reads as "a party is standing here", PENDING as "the point is
            // empty". The message has no boolean field of its own, and adding one for a
            // diagnostic query would put a byte on every ARRIVE to no purpose
            case QUERY -> entry.hasWaiter()
                    ? ExchangeMessage.exchanged(msg.requestId(), epoch, name, NO_PAYLOAD)
                    : ExchangeMessage.pending(msg.requestId(), epoch, name);
            default -> ExchangeMessage.fail(msg.requestId(), epoch, name,
                    "not a legitimate request type: " + msg.type());
        };
    }

    /**
     * Wakes the one party that was left asleep by a pairing.
     *
     * <p><b>A unicast, not a broadcast.</b> The barrier broadcasts because the moment it trips
     * its arrival list is cleared and the precise list no longer exists; pairing has no such
     * problem, since the sleeper is named in the result. So the whole cluster need not be told
     * that two of its members met.
     *
     * <p>A partner on this node is signalled inline: it is a {@code notifyAll} on one monitor,
     * and handing that to another thread would cost more than doing it.
     */
    private void notifyPartner(String name, Node partner) {
        if (partner.id().equals(cluster.self().id())) {
            signals.signal(name);
            return;
        }
        long currentEpoch = epoch;
        notifier.execute(() -> {
            try {
                cluster.unicastOn(CHANNEL, partner,
                        ExchangeMessage.notify(currentEpoch, name, "").encode());
            } catch (Exception e) {
                // A lost push does no harm: the item stays on the leader and the sleeper's
                // next re-check collects it
                log.debug("Failed to push the pairing at exchange point {} to {}: {}", name,
                        partner.label(), e.toString());
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
            // quiet period. Clearing the register here would throw away every item handed
            // over during startup
            return;
        }
        lastLeaderId = currentLeaderId;
        if (selfIsLeader) {
            epoch = System.currentTimeMillis();
            registry.clear();
            log.info("This node became the exchanger leader; the register was reset at "
                    + "epoch={}", epoch);
        } else {
            registry.clear();
        }
        // With the register cleared the cached replies mean nothing, and one of them may hold
        // an item belonging to the previous term
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
     * polling to fall back on, leaving them asleep means each holds its item until its own
     * timeout.
     */
    @Override
    public void onLeaderLeft(Node node) {
        leaderGeneration.incrementAndGet();
        signals.signalAll();
    }

    /**
     * A member departed.
     *
     * <p>Its parties can neither complete a pairing nor collect an item, so both are cleared.
     * The point is not to reclaim memory but to keep the <b>next</b> arrival from pairing with
     * a party that no longer exists: that arrival would be told the exchange succeeded, hand
     * its item to nobody, and take away an item from a process that has gone.
     */
    @Override
    public void onNodeLeft(Node node, boolean graceful) {
        if (!cluster.isLeader()) {
            return;
        }
        List<String> affected = registry.onNodeLeft(node.id());
        if (!affected.isEmpty()) {
            // Nobody is woken here, and that is not an omission. Only the departed node's own
            // parties were cleared, and they went with the process. A party elsewhere is
            // either still holding the slot or still holding an uncollected item, and neither
            // was touched, so waking them would buy one wasted round trip each
            log.info("Member {} departed; the {} exchange point(s) it was standing at were "
                    + "cleared: {}", node.label(), affected.size(), affected);
        }
    }

    // ------------------------------------------------------------------

    private void evictIdle() {
        if (!closed && cluster.isLeader()) {
            int n = registry.evictIdle(idleTimeoutMs);
            if (n > 0) {
                log.debug("Swept {} idle exchange point(s)", n);
            }
        }
    }

    /**
     * Pauses before retrying a failed request.
     *
     * <p>Without the pause it becomes a busy wait, capable of saturating a CPU while
     * leadership is vacant. The bound is what remains of the time, so the caller's timeout is
     * never overrun.
     */
    private static void sleepBriefly(long remainingMs) throws InterruptedException {
        Thread.sleep(Math.max(1L, Math.min(50L, remainingMs)));
    }

    private void checkOpen() {
        if (closed) {
            throw new ProcessingExchangerException("the exchanger service is closed");
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
     *   <li>{@code arrivals} counts calls and {@code exchanges} counts the ones that got an
     *       item back. A persistent gap between them is the shape of the usual mistake: one
     *       side of a pairing was never written, so every call waits out its timeout</li>
     *   <li>{@code pairings} is counted <b>on the leader only</b>, and one pairing serves two
     *       {@code exchanges}. On a follower it stays at zero, which is correct rather than
     *       broken</li>
     *   <li>{@code lateExchanges} counts exchanges that completed after their deadline or
     *       after an interrupt, because the pairing beat the cancellation. Non-zero is not an
     *       error -- it is the mechanism that keeps items from being dropped -- but a large
     *       one means timeouts are set close to the real waiting time</li>
     *   <li>{@code failures} counts every other way out: interrupted, or the mechanism
     *       failed. {@code invalidations} is the subset of those caused by a change of
     *       leader, so the two are read together rather than added</li>
     *   <li>{@code invalidations} counts waits ended by a change of leader. A steady trickle
     *       means leadership is moving, and the exchanges caught mid-pairing by it are the
     *       ones that lose items</li>
     *   <li>{@code avgWaitMillis} covers successful exchanges only, and measures <b>how long
     *       the second party took to turn up</b>, which is the number worth watching</li>
     * </ul>
     */
    @Override
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        long ok = exchanges.get();
        m.put("arrivals", arrivals.get());
        m.put("exchanges", ok);
        m.put("pairings", pairings.get());
        m.put("timeouts", timeouts.get());
        m.put("failures", failures.get());
        m.put("lateExchanges", lateExchanges.get());
        m.put("invalidations", invalidations.get());
        m.put("avgWaitMillis", ok == 0 ? 0d
                : Math.round(waitMillis.get() * 100.0 / ok) / 100.0);
        m.put("waitingNow", Math.max(0L, arrivals.get() - ok - timeouts.get()
                - failures.get()));
        m.put("exchangePoints", registry.size());
        m.put("waitersHeld", registry.waiterCount());
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
