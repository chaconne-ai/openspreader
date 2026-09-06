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
package com.chaconneai.openspreader.example.semaphore;

import com.chaconneai.openspreader.sync.ProcessingSemaphore;
import com.chaconneai.openspreader.sync.ProcessingSyncService;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A cross-process semaphore: N holders at a time across the whole cluster.
 *
 * <h2>One number separates it from a lock</h2>
 * A lock is a semaphore with {@code permits=1}. The real question is "how many of these are
 * safe at once": one means a lock, N means a semaphore.
 *
 * <h2>The typical use: limiting load on something downstream</h2>
 * Scale the application to ten replicas, each limiting itself to five concurrent calls, and
 * what downstream sees is fifty -- a single-machine
 * {@code java.util.concurrent.Semaphore} cannot stop that. A cross-process semaphore can:
 * however many replicas there are, the cluster total is the number configured.
 *
 * <ul>
 *   <li>Calling a third-party API with a concurrency limit</li>
 *   <li>Limiting how many heavy tasks run at once -- exports, transcoding, model
 *       inference</li>
 *   <li>Protecting an old system with a limited connection count</li>
 * </ul>
 *
 * <h2>The two granularities</h2>
 * <ul>
 *   <li>{@code application(key, n)} -- the N permits are shared <b>among instances of the same
 *       application</b>. "At most five concurrent exports in this application" wants this</li>
 *   <li>{@code cluster(key, n)} -- shared across the whole cluster, without regard to
 *       application. "At most twenty connections from all services to that old machine" wants
 *       this</li>
 * </ul>
 *
 * <h2>The same ceiling as a lock</h2>
 * Its safety does not exceed the strength of "there is one leader": under a network partition
 * each side may have a leader, and the total permit count doubles at that moment. So it suits
 * <b>rate limiting</b>, where letting a few extra through only makes things slower, and
 * <b>does not suit</b> a correctness constraint of "never more than N".
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class SemaphoreBestPractice {

    private final ProcessingSyncService semaphores;

    public SemaphoreBestPractice(ProcessingSyncService semaphores) {
        this.semaphores = semaphores;
    }

    // ==================================================================
    // 1. The common form
    // ==================================================================

    /**
     * <b>The recommended basic form</b>: acquire with a timeout, work if it succeeds, and fail
     * plainly if it does not.
     *
     * <p>Three points:
     * <ol>
     *   <li>{@code release()} must go in a finally -- an exception in between must still return
     *       the permit, or permits leak one by one until nobody can acquire any</li>
     *   <li>{@code release()} goes <b>inside</b> the {@code if} -- do not return what was never
     *       acquired, or someone else's permit is returned instead</li>
     *   <li>Use the version with a timeout. The unbounded
     *       {@link ProcessingSemaphore#acquire()} waits for ever once permits have leaked</li>
     * </ol>
     */
    public <T> T callWithLimitedConcurrency(Supplier<T> call) throws InterruptedException {
        // At most five concurrent calls across the application, whatever the replica count
        ProcessingSemaphore sem = semaphores.applicationSemaphore("third-party-api", 5);

        if (!sem.tryAcquire(3, TimeUnit.SECONDS)) {
            // Failing to acquire means downstream is busy. Failing outright beats queueing
            // indefinitely -- the caller has a timeout of its own, and waiting here only drags
            // it down too
            throw new IllegalStateException("downstream concurrency is full; retry shortly");
        }
        try {
            return call.get();
        } finally {
            sem.release();
        }
    }

    /**
     * The non-waiting form: take another path when it fails.
     *
     * <p>For where a fallback exists -- failing to acquire, serve from cache, take a simplified
     * path, or simply queue it for later.
     */
    public String renderReportOrFallback() {
        ProcessingSemaphore sem = semaphores.applicationSemaphore("heavy-render", 2);
        if (!sem.tryAcquire()) {
            return cachedReport();          // degrade rather than queue
        }
        try {
            return renderReport();
        } finally {
            sem.release();
        }
    }

    // ==================================================================
    // 2. Choosing a granularity
    // ==================================================================

    /**
     * Cluster granularity: <b>every application</b> shares these permits between them.
     *
     * <p>For protecting a shared external resource -- that old Oracle takes twenty connections
     * and does not care whether the order service or the reporting service is connecting.
     * Cluster granularity is required here; application granularity would become "twenty each,
     * per application".
     */
    public void queryLegacySystem() throws InterruptedException {
        ProcessingSemaphore sem = semaphores.clusterSemaphore("legacy-db-conn", 20);
        if (!sem.tryAcquire(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("the legacy system's connections are full");
        }
        try {
            doQuery();
        } finally {
            sem.release();
        }
    }

    // ==================================================================
    // 3. Observing it
    // ==================================================================

    /**
     * The available permit count is for monitoring, <b>not for making decisions</b>.
     *
     * <p>It is queried from the leader and true at that instant; by the time it is read,
     * someone else may already have taken it. Writing
     * {@code if (availablePermits() > 0) { doTheWorkDirectly(); }} is wrong -- the only correct
     * way to acquire a permit is {@code tryAcquire()} itself, which is atomic.
     *
     * <p>It returns -1 while leadership is vacant or the query fails; filter that out before
     * charting.
     */
    public void reportMetrics() {
        ProcessingSemaphore sem = semaphores.applicationSemaphore("third-party-api", 5);
        int available = sem.availablePermits();
        if (available >= 0) {
            metrics("semaphore.available", available);
            metrics("semaphore.used", sem.permits() - available);
        }
        // How many this node holds is decided locally with no network request, so it may be
        // called freely
        metrics("semaphore.held.local", sem.heldPermits());
    }

    // ==================================================================
    // 4. Easy mistakes
    // ==================================================================

    /**
     * <b>Do not acquire one semaphore while holding another</b> -- two callers in opposite
     * orders deadlock, and <b>there is no deadlock detection</b>.
     *
     * <p>Where it is genuinely unavoidable, take one of two routes:
     * <ol>
     *   <li>Agree a fixed acquisition order globally -- always A then B -- and have everyone
     *       follow it</li>
     *   <li>Use the version with a timeout throughout -- a deadlock unwinds itself, at the cost
     *       of some failed requests</li>
     * </ol>
     *
     * <p>The second is written below. Note that <b>a failed acquisition must return what has
     * already been acquired</b>.
     */
    public void needsTwoResources() throws InterruptedException {
        ProcessingSemaphore a = semaphores.applicationSemaphore("resource-a", 3);
        ProcessingSemaphore b = semaphores.applicationSemaphore("resource-b", 3);

        if (!a.tryAcquire(2, TimeUnit.SECONDS)) {
            throw new IllegalStateException("resource A is busy");
        }
        try {
            if (!b.tryAcquire(2, TimeUnit.SECONDS)) {
                throw new IllegalStateException("resource B is busy");
            }
            try {
                doWork();
            } finally {
                b.release();
            }
        } finally {
            a.release();
        }
    }

    /**
     * Permits <b>are not lost for good when a process crashes</b>.
     *
     * <p>A permit carries a lease, which this node renews while it is held. Once the process
     * dies the renewals stop, and the leader reclaims the permit when the lease expires; a node
     * judged to have departed has every permit it held reclaimed at once. So there is no need
     * to fear "a power cut leaves fewer and fewer permits".
     *
     * <p>There is exactly one way permits really leak: <b>a forgotten release in the code</b>,
     * with the process still alive and still renewing. Which is why the finally is not
     * optional.
     */
    public void leaseIsAutomatic() {
        // Nothing to do -- this method exists only to carry the note above
    }

    // ==================================================================

    private String renderReport() {
        return "report";
    }

    private String cachedReport() {
        return "cached-report";
    }

    private void doQuery() {
    }

    private void doWork() {
    }

    private void metrics(String name, long value) {
    }
}
