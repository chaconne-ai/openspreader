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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A cross-process exchange point: two parties meet under one name and swap items.
 *
 * <p>Used as {@code java.util.concurrent.Exchanger} is, except that the partner may be a
 * thread in another process:
 * <pre>{@code
 * ProcessingExchanger<Batch> point = syncs.applicationExchanger("buffer-swap");
 *
 * Batch full = fillBatch();
 * Batch empty = point.exchange(full, 30, TimeUnit.SECONDS);  // hand over full, take back empty
 * }</pre>
 *
 * <p>What sets it apart from {@link ProcessingCountDownLatch} and
 * {@link ProcessingCyclicBarrier} is the <b>item</b>: they only tell the parties that everyone
 * has arrived, while this moves data between exactly two of them.
 *
 * <h2>A party is a thread</h2>
 * As in the JDK, one {@link #exchange} call is one party. Two threads in one process may
 * pair with each other; so may one thread in each of two processes. Beyond two, arrivals
 * pair up two at a time in the order they reach the leader -- four parties make two pairs,
 * and a fifth waits for a sixth.
 *
 * <h2>The item crosses the network</h2>
 * It is serialised by the configured {@code ObjectCodec}
 * ({@code spring.spreader.multiprocessing.serialization}), so under the default JDK
 * serialisation it and every one of its fields must be {@link java.io.Serializable}. The
 * same object identity does not survive the crossing: a partner in another process receives
 * <b>a copy</b>, and two threads in the same process receive a copy too, since the item
 * travels through the leader either way. Do not exchange something whose identity matters.
 *
 * <p>{@code null} is permitted, and comes back as {@code null}. The JDK's {@code Exchanger}
 * permits it too.
 *
 * <h2>Timing out never loses an item</h2>
 * A deliberate departure from the JDK's semantics. A pairing can land on the leader in the
 * instant between this party giving up and its cancellation arriving, and by then the partner
 * has left with this party's item. Rather than throw and drop the incoming half,
 * {@link #exchange(Object, long, TimeUnit)} <b>returns successfully</b>, a little over its
 * deadline. An interrupt is handled the same way, with the interrupt flag set again so the
 * caller still notices at its next blocking call.
 *
 * <h2>What not to use it for</h2>
 * Its safety is only as strong as "there is one leader". Under a partition each side pairs
 * within itself, so two parties expecting each other can meet someone else, and a change of
 * leader loses any exchange paired but not yet collected. An exchange is a rendezvous, not a
 * transaction: where an item must never be lost, use a queue with storage behind it.
 *
 * @param <V> the type of item exchanged
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public interface ProcessingExchanger<V> {

    /** The exchange point's full name, including the granularity prefix. */
    String name();

    /**
     * Hands over an item and waits, without a time limit, for a partner to hand one back.
     *
     * <p><b>Without a timeout, a partner that never arrives means waiting for ever.</b> The
     * only things that end the wait are a partner arriving, an interrupt, or the leader
     * changing. Prefer {@link #exchange(Object, long, TimeUnit)} anywhere a partner is not
     * guaranteed.
     *
     * @param item the item to hand over; may be null
     * @return the partner's item
     * @throws InterruptedException           when interrupted before a partner arrived. Where
     *                                        the pairing had <b>already happened</b>, the
     *                                        partner's item is returned instead and the
     *                                        interrupt flag is set again, so that no item is
     *                                        dropped
     * @throws ProcessingExchangerException   when the mechanism failed: the leader is
     *                                        unreachable, the leader changed, or the service
     *                                        is closed. No pairing happened, so {@code item}
     *                                        is still the caller's
     */
    V exchange(V item) throws InterruptedException;

    /**
     * Hands over an item and waits, up to a limit, for a partner to hand one back.
     *
     * @param item the item to hand over; may be null
     * @return the partner's item
     * @throws TimeoutException             when no partner arrived in time. No pairing
     *                                      happened and {@code item} is still the caller's
     * @throws InterruptedException         as {@link #exchange(Object)}
     * @throws ProcessingExchangerException as {@link #exchange(Object)}
     */
    V exchange(V item, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;

    /**
     * Whether a party is currently standing at this exchange point waiting for a partner.
     *
     * <p>It queries the leader rather than deciding locally, and is <b>a reading of a moment
     * that has already passed</b> by the time it returns: a partner may arrive or give up in
     * between. Use it for diagnostics, never to decide whether to call {@code exchange} --
     * that decision is the race this class exists to remove.
     */
    boolean hasWaiter();
}
