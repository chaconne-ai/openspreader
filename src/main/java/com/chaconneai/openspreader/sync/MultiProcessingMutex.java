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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link ProcessingMutex} that achieves "exactly one thread cluster-wide holds it" with
 * two layers of locking.
 *
 * <ul>
 *   <li><b>Within the process</b> -- a local {@link ReentrantLock}. When several threads in
 *       one process contend for the same lock, they queue locally first and only one goes on
 *       to contend with the other processes. It solves reentrancy along the way.</li>
 *   <li><b>Between processes</b> -- a request to the leader. Every process's request
 *       converges on one place to be decided, so two processes can never both hold it.</li>
 * </ul>
 *
 * <p>Without the local layer, ten threads in one process would each send the leader a
 * request, the leader would turn nine of them away for nothing, and each would have cost a
 * round trip.
 *
 * <p><b>One lock name must reuse one instance</b>, or the local layer achieves nothing --
 * which is why they are obtained through {@link ProcessingSyncService}, which caches them.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MultiProcessingMutex implements ProcessingMutex {

    private static final Logger log = LoggerFactory.getLogger(MultiProcessingMutex.class);

    private final MutexService service;
    private final String lockName;
    private final String value;

    /** In-process mutual exclusion, plus the reentrancy count. */
    private final ReentrantLock localLock = new ReentrantLock();

    public MultiProcessingMutex(MutexService service, String lockName, String value) {
        this.service = service;
        this.lockName = lockName;
        this.value = value;
    }

    @Override
    public String name() {
        return lockName;
    }

    @Override
    public boolean tryAcquire() {
        if (!localLock.tryLock()) {
            // Another thread in this process already holds it, or is asking for it
            return false;
        }
        if (localLock.getHoldCount() > 1) {
            // Reentrant: it is already ours, so there is no need to ask the leader
            return true;
        }
        boolean acquired = false;
        try {
            acquired = service.tryAcquire(lockName, value);
            return acquired;
        } finally {
            // The local lock must be returned when the acquisition failed. Miss any one path
            // and this process holds it forever: the lock becomes unobtainable, with no way
            // back
            if (!acquired) {
                localLock.unlock();
            }
        }
    }

    @Override
    public boolean acquire(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);

        if (!localLock.tryLock(timeout, unit)) {
            return false;
        }
        if (localLock.getHoldCount() > 1) {
            return true;
        }
        boolean acquired = false;
        try {
            // Queueing locally has already consumed part of the budget; whatever is left goes
            // to the remote attempt
            long remainingMs = Math.max(0, (deadline - System.nanoTime()) / 1_000_000L);
            acquired = service.acquire(lockName, value, remainingMs);
            return acquired;
        } finally {
            if (!acquired) {
                localLock.unlock();
            }
        }
    }

    /**
     * Waits until the lock is acquired.
     *
     * <p><b>Think before using it</b>: the wait is unbounded and there is no deadlock
     * detection. Two processes each holding one lock and waiting on the other's will be stuck
     * for good -- for that, use {@link #acquire(long, TimeUnit)}, leave on timeout, and
     * retry.
     *
     * <p>The wait can be broken by {@code interrupt()}: the local queueing uses an
     * interruptible acquisition, and the remote retry checks the interrupt flag.
     */
    @Override
    public boolean acquire() {
        try {
            // The interruptible form is required. lock() does not respond to interrupts, so
            // once a thread ahead becomes stuck, those behind it park here forever and not
            // even interrupt() can retrieve them
            localLock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        if (localLock.getHoldCount() > 1) {
            return true;
        }
        boolean acquired = false;
        try {
            // -1 means untimed: while leadership is vacant it keeps retrying, waiting for a
            // new leader to take over
            acquired = service.acquire(lockName, value, -1);
            return acquired;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            if (!acquired) {
                localLock.unlock();
            }
        }
    }

    @Override
    public void release() {
        release(0L);
    }

    @Override
    public void release(long cooldownMs) {
        if (!localLock.isHeldByCurrentThread()) {
            // Being called unconditionally from a finally block is the common shape, so this
            // does not throw and interrupt the application
            log.debug("This thread does not hold lock {}; the release is ignored", lockName);
            return;
        }
        try {
            if (localLock.getHoldCount() == 1) {
                // Only when the outermost reentrant level exits is it really returned to the cluster
                service.release(lockName, cooldownMs);
            }
        } finally {
            localLock.unlock();
        }
    }

    @Override
    public boolean isHeld() {
        return service.isHeld(lockName);
    }

    @Override
    public String currentOccupied() {
        return service.currentOccupied(lockName);
    }

    @Override
    public String toString() {
        return "ProcessingMutex{" + lockName + ", held=" + isHeld() + '}';
    }
}
