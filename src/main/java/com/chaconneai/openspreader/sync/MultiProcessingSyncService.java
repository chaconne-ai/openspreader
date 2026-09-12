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

import com.chaconneai.openspreader.Scope;
import com.chaconneai.spreader.GossipCluster;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link ProcessingSyncService}: it caches the synchronisers by name, with the rules for
 * obtaining them gathered here.
 *
 * <p>Granularity, caching and argument validation -- everything about <i>how</i> to obtain
 * one -- are documented on the interface. This class adds two implementation trade-offs.
 *
 * <h2>Five tables, not one</h2>
 * Locks, latches, barriers, semaphores and exchange points each get a cache table. Combined
 * into one, a lock and a latch under the same business name would displace each other -- and
 * using one name for different synchronisations of the same thing is a perfectly natural way
 * to write it ({@code import-batch} having both a latch and a lock for the closing work,
 * say).
 *
 * <h2>The five synchronisers have independent switches</h2>
 * Locks, latches, barriers, semaphores and exchange points are <b>five independent
 * settings</b>, and enabling only one of them is ordinary usage. So a service is allowed to
 * be {@code null} here, and the error comes only when that kind of synchroniser is actually
 * asked for -- naming the setting to switch on -- rather than failing the entire
 * application's configuration at startup over a missing bean.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
public class MultiProcessingSyncService implements ProcessingSyncService {

    private final MutexService mutexService;
    private final LatchService latchService;
    private final BarrierService barrierService;
    private final SemaphoreService semaphoreService;
    private final ExchangerService exchangerService;
    private final GossipCluster cluster;

    /** Qualified name -> instance. One table per kind, so the name spaces cannot interfere. */
    private final Map<String, ProcessingMutex> mutexes = new ConcurrentHashMap<>();
    private final Map<String, ProcessingCountDownLatch> latches = new ConcurrentHashMap<>();
    private final Map<String, ProcessingCyclicBarrier> barriers = new ConcurrentHashMap<>();
    private final Map<String, ProcessingSemaphore> semaphores = new ConcurrentHashMap<>();
    private final Map<String, ProcessingExchanger<?>> exchangers = new ConcurrentHashMap<>();

    /**
     * @param mutexService     the lock service; {@code null} when that feature is off
     * @param latchService     the latch service; {@code null} when that feature is off
     * @param barrierService   the barrier service; {@code null} when that feature is off
     * @param semaphoreService the semaphore service; {@code null} when that feature is off
     * @param exchangerService the exchanger service; {@code null} when that feature is off
     */
    public MultiProcessingSyncService(MutexService mutexService, LatchService latchService,
                               BarrierService barrierService, SemaphoreService semaphoreService,
                               ExchangerService exchangerService, GossipCluster cluster) {
        this.mutexService = mutexService;
        this.latchService = latchService;
        this.barrierService = barrierService;
        this.semaphoreService = semaphoreService;
        this.exchangerService = exchangerService;
        this.cluster = cluster;
    }

    // ------------------------------------------------------------------
    // Locks
    // ------------------------------------------------------------------

    @Override
    public ProcessingMutex mutex(Scope scope, String key) {
        String name = qualify(scope, key, "lock name");
        MutexService svc = require(mutexService, "the cross-process lock",
                "spring.spreader.multiprocessing.mutex.enabled");
        return mutexes.computeIfAbsent(name,
                n -> new MultiProcessingMutex(svc, n, holderLabel()));
    }

    // ------------------------------------------------------------------
    // Latches
    // ------------------------------------------------------------------

    @Override
    public ProcessingCountDownLatch latch(Scope scope, String key, long count) {
        if (count < 0) {
            throw new IllegalArgumentException("a latch's initial count must not be negative: " + count);
        }
        String name = qualify(scope, key, "latch name");
        LatchService svc = require(latchService, "the cross-process latch",
                "spring.spreader.multiprocessing.latch.enabled");
        ProcessingCountDownLatch existing = latches.computeIfAbsent(name,
                n -> new MultiProcessingCountDownLatch(svc, n, count));
        if (existing.initialCount() != count) {
            // Caught in this process first: the message is far clearer than "the leader says
            // the count does not match"
            throw new IllegalArgumentException("latch " + name + " was already created in this "
                    + "process with a count of " + existing.initialCount()
                    + " and cannot be changed to " + count);
        }
        return existing;
    }

    @Override
    public void forgetLatch(Scope scope, String key) {
        latches.remove(qualifyRaw(scope, key));
    }

    // ------------------------------------------------------------------
    // Barriers
    // ------------------------------------------------------------------

    @Override
    public ProcessingCyclicBarrier barrier(Scope scope, String key, int parties) {
        if (parties <= 0) {
            throw new IllegalArgumentException("a barrier's party count must be greater than 0: " + parties);
        }
        String name = qualify(scope, key, "barrier name");
        BarrierService svc = require(barrierService, "the cross-process barrier",
                "spring.spreader.multiprocessing.barrier.enabled");
        ProcessingCyclicBarrier existing = barriers.computeIfAbsent(name,
                n -> new MultiProcessingCyclicBarrier(svc, n, parties));
        if (existing.parties() != parties) {
            // Caught in this process first: the message is far clearer than "the parties never
            // add up"
            throw new IllegalArgumentException("barrier " + name + " was already created in "
                    + "this process with " + existing.parties()
                    + " parties and cannot be changed to " + parties);
        }
        return existing;
    }

    @Override
    public void forgetBarrier(Scope scope, String key) {
        barriers.remove(qualifyRaw(scope, key));
    }

    // ------------------------------------------------------------------
    // Semaphores
    // ------------------------------------------------------------------

    @Override
    public ProcessingSemaphore semaphore(Scope scope, String key, int permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("the permit count must be greater than 0, but was " + permits);
        }
        String name = qualify(scope, key, "semaphore name");
        SemaphoreService svc = require(semaphoreService, "the cross-process semaphore",
                "spring.spreader.multiprocessing.semaphore.enabled");
        ProcessingSemaphore existing = semaphores.computeIfAbsent(name,
                n -> new MultiProcessingSemaphore(svc, n, permits, holderLabel()));
        if (existing.permits() != permits) {
            // IllegalArgumentException rather than IllegalStateException: the barrier above
            // throws the same for "same name, different parameters", and the two must agree,
            // or a caller catching one would miss the other
            throw new IllegalArgumentException("semaphore " + name + " was created with "
                    + existing.permits() + " permit(s) but " + permits
                    + " were asked for this time; one name must keep one permit count");
        }
        return existing;
    }

    // ------------------------------------------------------------------
    // Exchange points
    // ------------------------------------------------------------------

    /**
     * <p>The cast is safe in the only sense available: {@code V} is erased before the item
     * ever reaches the network, so this table cannot check it and nor could a table keyed by
     * type. One name to one type is the caller's discipline; see
     * {@link MultiProcessingExchanger}.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <V> ProcessingExchanger<V> exchanger(Scope scope, String key) {
        String name = qualify(scope, key, "exchange point name");
        ExchangerService svc = require(exchangerService, "the cross-process exchanger",
                "spring.spreader.multiprocessing.exchanger.enabled");
        return (ProcessingExchanger<V>) exchangers.computeIfAbsent(name,
                n -> new MultiProcessingExchanger<V>(svc, n));
    }

    // ------------------------------------------------------------------
    // Troubleshooting
    // ------------------------------------------------------------------

    @Override
    public Map<String, ProcessingSemaphore> knownSemaphores() {
        return Map.copyOf(semaphores);
    }

    @Override
    public Map<String, ProcessingMutex> knownMutexes() {
        return Map.copyOf(mutexes);
    }

    @Override
    public Map<String, ProcessingCountDownLatch> knownLatches() {
        return Map.copyOf(latches);
    }

    @Override
    public Map<String, ProcessingCyclicBarrier> knownBarriers() {
        return Map.copyOf(barriers);
    }

    @Override
    public Map<String, ProcessingExchanger<?>> knownExchangers() {
        return Map.copyOf(exchangers);
    }

    // ------------------------------------------------------------------

    /**
     * Confirms the corresponding service is switched on before it is used.
     *
     * <p>The message names the property to enable outright -- a "NullPointerException" or a
     * "no such bean" would leave the user to dig through the documentation guessing which
     * switch it was.
     */
    private static <T> T require(T service, String what, String property) {
        if (service == null) {
            throw new IllegalStateException(what + " is not enabled; set "
                    + property + "=true");
        }
        return service;
    }

    /**
     * Qualifies the key the application gave to the requested granularity.
     *
     * @param what names the kind of synchroniser for error messages, such as "lock name"
     */
    private String qualify(Scope scope, String key, String what) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException(what + " must not be blank");
        }
        return qualifyRaw(scope, key);
    }

    private String qualifyRaw(Scope scope, String key) {
        return scope.qualify(cluster.clusterName(), cluster.self().name(), key.trim());
    }

    /**
     * The default holder identifier: application name @ address.
     *
     * <p>It takes node-level information rather than a thread name, because
     * {@code currentOccupied()} is read by other <b>processes</b>, which care about which
     * application on which machine is holding it.
     */
    private String holderLabel() {
        return cluster.self().label();
    }
}
