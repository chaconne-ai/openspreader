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

/**
 * A cross-process mutual exclusion lock: one thread cluster-wide holds it at a time.
 *
 * <p>Used exactly as {@code ReentrantLock} is, and note that the check comes before the
 * release:
 * <pre>{@code
 * if (mutex.tryAcquire()) {
 *     try {
 *         // This is the only thread running here, cluster-wide
 *     } finally {
 *         mutex.release();
 *     }
 * }
 * }</pre>
 *
 * <p>One thread may acquire it repeatedly -- it is reentrant -- and must release it as many
 * times as it acquired it.
 *
 * <p><b>The lock is bound to its thread</b>: whoever calls {@code acquire} must call
 * {@code release}, exactly as with {@code ReentrantLock}. Releasing from another thread is
 * ignored, leaving a debug line and throwing nothing, so that cleanup in a finally block is
 * not interrupted. The ordinary try/finally shape never meets this; but a cross-thread usage
 * -- acquiring in one request and releasing in another -- has to pin both operations to the
 * same thread itself.
 *
 * <h2>What it guarantees</h2>
 * During normal operation one lock name has one holder; and when a holder crashes, departs,
 * or gets stuck and stops renewing, the lock is released.
 *
 * <h2>What it does not guarantee</h2>
 * This lock's safety <b>does not exceed the strength of "there is one leader"</b>. Across
 * machines, that uniqueness is only an agreement about timing, and under a network partition
 * each side may have a leader of its own -- so two processes can hold the same lock at once.
 *
 * <p>So: coordinating things where repeating the work is merely wasteful rather than wrong
 * -- scheduled tasks, cache warming, exclusive consumption -- is fine. <b>Do not</b> use it
 * to protect a transfer or a debit, where repeating it causes real harm.
 *
 * <p>Note too that a change of leader invalidates every lock immediately, since the register
 * lives only in the leader's memory. Where slow work happens while holding one, re-check with
 * {@link #isHeld()} rather than assuming that having acquired it means still holding it.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface ProcessingMutex {

    /** The lock's full name, including the granularity prefix. */
    String name();

    /**
     * Tries once, without waiting.
     *
     * @return whether it was acquired
     */
    boolean tryAcquire();

    /**
     * Acquires the lock, retrying until the timeout.
     *
     * @return whether it was acquired
     */
    boolean acquire(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Acquires the lock, retrying until it succeeds.
     *
     * <p>While leadership is vacant it keeps retrying, resuming once a new leader takes over.
     * An interrupted thread gives up and returns false.
     *
     * <p><b>There is no deadlock detection.</b> The wait is unbounded, so two processes each
     * holding one lock and waiting on the other's are stuck for good, with neither timing out.
     * Where several locks are held at once:
     * <ul>
     *   <li>either use {@link #acquire(long, TimeUnit)} throughout, releasing everything
     *       acquired so far on timeout and starting again</li>
     *   <li>or agree on a fixed acquisition order globally, and have everyone follow it</li>
     * </ul>
     */
    boolean acquire();

    /** Releases it. Holding nothing, it does nothing, so it is safe in a finally block. */
    void release();

    /**
     * Releases it and makes the lock <b>unobtainable by anyone</b>, this node included, for a
     * while afterwards.
     *
     * <p>Built for "once per period" requirements. Take a scheduled task on a five-second
     * cycle whose body runs for two milliseconds: with an ordinary release, another instance
     * takes the lock the moment it is free and the same cycle runs several times. Holding it
     * down for five seconds guarantees one run per cycle.
     *
     * @param cooldownMs the cooldown; 0 or negative is equivalent to {@link #release()}
     */
    void release(long cooldownMs);

    /**
     * Whether this node still holds the lock.
     *
     * <p><b>Decided locally, with no network request.</b> It becomes false immediately after a
     * change of leader, because the register lived in the old leader's memory and is gone.
     */
    boolean isHeld();

    /**
     * Asks for the identifier the current holder wrote.
     *
     * <p>It queries the leader rather than deciding locally.
     *
     * @return the holder's value, or null when nobody holds it or it cannot be found
     */
    String currentOccupied();
}
