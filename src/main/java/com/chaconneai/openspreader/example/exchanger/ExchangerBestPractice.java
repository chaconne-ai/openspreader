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
package com.chaconneai.openspreader.example.exchanger;

import com.chaconneai.openspreader.sync.ProcessingExchanger;
import com.chaconneai.openspreader.sync.ProcessingExchangerException;
import com.chaconneai.openspreader.sync.ProcessingSyncService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A cross-process exchange point: two parties meet under one name and swap items.
 *
 * <h2>What separates it from the other synchronisers</h2>
 * <ul>
 *   <li>A <b>latch</b> is one-shot, and waiters and counters are different parties. "Wait for
 *       five things to finish"</li>
 *   <li>A <b>barrier</b> aligns N parties round after round, and <b>carries nothing</b>.
 *       "Five parties align once per round"</li>
 *   <li>An <b>exchange point</b> pairs exactly two and <b>moves data between them</b>. "Hand
 *       over a full buffer and take back an empty one"</li>
 * </ul>
 *
 * <p>The item is the point. If the two sides only need to know that the other has arrived,
 * a barrier is smaller and cheaper.
 *
 * <h2>The typical use: a producer and a consumer swapping buffers</h2>
 * One instance fills a batch while another drains one. When both are ready they trade: the
 * filler hands over the full batch and takes back an emptied one to fill again. Nothing is
 * allocated per round, and neither side needs a queue in between.
 *
 * <ul>
 *   <li>Double buffering across processes: fill one, drain the other, swap</li>
 *   <li>Pairing up work: two instances each holding half a job meet and trade halves</li>
 *   <li>Handing a batch to whichever instance is free, rather than to a named one</li>
 * </ul>
 *
 * <h2>Before using it, three things to be sure of</h2>
 * <ol>
 *   <li><b>Exactly two.</b> A third arrival does not join the pair; it waits for a fourth.
 *       "Everyone gathers" is a barrier, not this</li>
 *   <li><b>The item crosses the network.</b> Under the default JDK serialisation it and all
 *       its fields must be {@link Serializable}, and the partner receives <b>a copy</b>. Two
 *       threads in one process get a copy too, since the item travels through the leader
 *       either way</li>
 *   <li><b>A rendezvous is not a transaction.</b> A change of leader mid-pairing loses the
 *       item. Where an item must never be lost, use a queue with storage behind it</li>
 * </ol>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class ExchangerBestPractice {

    /** The name both sides must agree on. One name, one item type; see
     *  {@link #oneNameOneType()}. */
    private static final String BUFFER_SWAP = "buffer-swap";

    private final ProcessingSyncService syncs;

    public ExchangerBestPractice(ProcessingSyncService syncs) {
        this.syncs = syncs;
    }

    // ==================================================================
    // 1. The common form
    // ==================================================================

    /**
     * <b>The recommended basic form</b>: exchange with a timeout, and have something sensible
     * to do when no partner turns up.
     *
     * <p>Three points:
     * <ol>
     *   <li>Use the version with a timeout. {@link ProcessingExchanger#exchange(Object)} waits
     *       for ever, and "the partner was never deployed" then looks exactly like "the
     *       partner is slow"</li>
     *   <li>A {@link TimeoutException} means <b>no pairing happened</b>, so the item is still
     *       yours. Keep it and try again; nothing was handed to anybody</li>
     *   <li>{@link ProcessingExchangerException} means the mechanism failed, and it makes the
     *       same promise: no pairing happened. So both paths below can safely keep the
     *       batch</li>
     * </ol>
     *
     * @return the partner's batch, or empty when nobody turned up and {@code mine} is still
     *         the caller's to keep
     */
    public Optional<List<String>> swapBatch(List<String> mine) throws InterruptedException {
        ProcessingExchanger<List<String>> point = syncs.applicationExchanger(BUFFER_SWAP);
        try {
            return Optional.of(point.exchange(mine, 30, TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            // No partner within the window. The batch was never handed over, so holding on to
            // it and carrying on is correct rather than a recovery hack
            return Optional.empty();
        } catch (ProcessingExchangerException e) {
            // The leader changed, or could not be reached. Same promise: nothing was handed
            // over
            return Optional.empty();
        }
    }

    /**
     * Double buffering across processes, which is what the JDK's {@code Exchanger} was
     * written for.
     *
     * <p>The producer fills its batch, then trades it for the consumer's emptied one and goes
     * on filling. No queue sits between them, and no batch is allocated per round: the same
     * two lists circulate for the life of the loop.
     *
     * <p>Note that the loop <b>keeps its own batch when the exchange times out</b>. That is
     * what makes the shape safe to write as a loop: a round that finds no partner costs a
     * round, not a batch.
     */
    public void produceForever(int rounds) throws InterruptedException {
        ProcessingExchanger<List<String>> point = syncs.applicationExchanger(BUFFER_SWAP);
        List<String> filling = new ArrayList<>();

        for (int i = 0; i < rounds; i++) {
            fill(filling);
            try {
                // Hand over the full one, take back an emptied one
                filling = point.exchange(filling, 30, TimeUnit.SECONDS);
                filling.clear();
            } catch (TimeoutException | ProcessingExchangerException e) {
                // No consumer this round. The batch is still ours, so it is drained here
                // instead rather than dropped
                drain(filling);
                filling.clear();
            }
        }
    }

    /** The consumer's half of the loop above, running in another process. */
    public void consumeForever(int rounds) throws InterruptedException {
        ProcessingExchanger<List<String>> point = syncs.applicationExchanger(BUFFER_SWAP);
        List<String> empty = new ArrayList<>();

        for (int i = 0; i < rounds; i++) {
            try {
                List<String> full = point.exchange(empty, 30, TimeUnit.SECONDS);
                drain(full);
                empty = full;
                empty.clear();
            } catch (TimeoutException | ProcessingExchangerException e) {
                // No producer this round; nothing to do but come round again
            }
        }
    }

    // ==================================================================
    // 2. Choosing a granularity
    // ==================================================================

    /**
     * Application granularity, which is what nearly everything wants: only instances of
     * <b>this</b> application meet here.
     *
     * <p>Cluster granularity exists ({@code clusterExchanger}) but deserves a moment's
     * thought first: the two sides of an exchange must agree on the item's type, and across
     * applications the partner may be a service that has never heard of the class being sent.
     * The pairing then succeeds and the <b>deserialisation</b> fails, which is a worse place
     * to find out.
     *
     * <p>So use cluster granularity only where the item is something every application knows:
     * a {@code String}, a {@code byte[]}, or a type from a shared library both sides depend
     * on.
     */
    public Optional<byte[]> swapAcrossApplications(byte[] mine) throws InterruptedException {
        // byte[] is safe at cluster granularity: no application can fail to have the class
        ProcessingExchanger<byte[]> point = syncs.clusterExchanger("cross-app-handoff");
        try {
            return Optional.of(point.exchange(mine, 10, TimeUnit.SECONDS));
        } catch (TimeoutException | ProcessingExchangerException e) {
            return Optional.empty();
        }
    }

    // ==================================================================
    // 3. Observing it
    // ==================================================================

    /**
     * {@code hasWaiter()} is for monitoring, <b>not for making decisions</b>.
     *
     * <p>It asks the leader and is true of the instant it was asked; by the time it returns, a
     * partner may have arrived or given up. Writing
     * {@code if (point.hasWaiter()) { point.exchange(...); }} is wrong twice over: the check
     * can be stale, and it buys nothing, because {@code exchange} already handles arriving
     * first by waiting.
     *
     * <p>Where it earns its place is a dashboard: a value that sits true means one side keeps
     * turning up and the other does not.
     */
    public void reportMetrics() {
        ProcessingExchanger<List<String>> point = syncs.applicationExchanger(BUFFER_SWAP);
        metrics("exchanger.waiting", point.hasWaiter() ? 1 : 0);
    }

    // ==================================================================
    // 4. Easy mistakes
    // ==================================================================

    /**
     * <b>One name carries one item type.</b>
     *
     * <p>{@code V} is erased before the item reaches the network, so two call sites using one
     * name with different types compile cleanly. The failure arrives later, as a
     * {@link ClassCastException} in whichever thread received the wrong item, a long way from
     * the line that caused it.
     *
     * <p>Declaring the name as a constant next to the type it carries, as
     * {@link #BUFFER_SWAP} is here, costs nothing and keeps the pair together.
     */
    public void oneNameOneType() {
        // Nothing to do; this method exists only to carry the note above
    }

    /**
     * <b>Do not exchange something whose identity matters.</b>
     *
     * <p>The item is serialised and travels through the leader, so the partner receives a
     * copy. This is true even of two threads in the same process. An open file handle, a
     * database connection, a cache entry the sender goes on mutating: none of these survive
     * the crossing in any useful form.
     *
     * <p>Exchange the <b>data</b>, and let each side use its own handles.
     */
    public void exchangeDataNotHandles() {
        // Nothing to do; this method exists only to carry the note above
    }

    /**
     * <b>Timing out never loses an item, and that has a consequence worth knowing.</b>
     *
     * <p>A pairing can complete on the leader in the instant between this party giving up and
     * its cancellation arriving. By then the partner has already gone away with this party's
     * item, so rather than throw and drop the incoming half, {@code exchange} <b>returns
     * successfully</b>, a little over its deadline.
     *
     * <p>So the deadline is a target rather than a hard bound. Code that must not overrun by
     * even a little should not be sitting in a rendezvous to begin with; everything else
     * simply benefits from never losing a batch.
     *
     * <p>The same applies to an interrupt: the item comes back and the thread's interrupt flag
     * is set again, so the caller still notices at its next blocking call.
     */
    public void timingOutDoesNotLoseTheItem() {
        // Nothing to do; this method exists only to carry the note above
    }

    /**
     * <b>Acquire and release of a lock must stay on one thread; an exchange need not.</b>
     *
     * <p>Worth stating because the two sit on the same facade. A {@code ProcessingMutex} keeps
     * a local {@code ReentrantLock}, so releasing it from another thread is silently ignored.
     * An exchange is one call that blocks and returns on the same thread by construction, so
     * there is nothing to get wrong: an {@code @Async} method, a plain
     * {@code new Thread(...)}, or a pool task are all equally valid places to call it, and
     * each counts as one party.
     *
     * <p>Two threads in the same process may perfectly well pair with each other.
     */
    public void anyThreadMayBeAParty() {
        // Nothing to do; this method exists only to carry the note above
    }

    // ==================================================================

    private void fill(List<String> batch) {
        batch.add("record");
    }

    private void drain(List<String> batch) {
    }

    private void metrics(String name, long value) {
    }
}
