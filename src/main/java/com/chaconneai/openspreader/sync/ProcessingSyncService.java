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

import java.util.Map;

/**
 * The <b>single entry point</b> for cross-process synchronisers: locks, latches and barriers
 * all come from here.
 *
 * <pre>{@code
 * // One holder across the whole cluster, without regard to application
 * ProcessingMutex m = sync.clusterMutex("db-migration");
 *
 * // One holder among this application's instances; another application using the same name
 * // is unaffected
 * ProcessingMutex m2 = sync.applicationMutex("daily-report");
 *
 * ProcessingCountDownLatch latch = sync.applicationLatch("batch-done", 3);
 * ProcessingCyclicBarrier barrier = sync.applicationBarrier("phase", 4);
 * }</pre>
 *
 * <h2>Why all three share one entry point</h2>
 * The rules for obtaining them are <b>identical</b> -- the same two granularities, the same
 * caching by name, the same "same name, different parameters, error". Split across three
 * factories, those rules would be written out three times, and the risk of changing one and
 * missing two is real: the granularity-qualifying logic did once exist as three separate
 * copies.
 *
 * <p>The method names carry the synchroniser type -- {@code applicationMutex} rather than
 * {@code application} -- because once they share an entry point, {@code application("x")} no
 * longer says what is being obtained.
 *
 * <h2>The two granularities</h2>
 * <ul>
 *   <li>{@code cluster*} -- one across the whole cluster, <b>without regard to
 *       application</b>. For global exclusion across applications, such as a data
 *       migration</li>
 *   <li>{@code application*} -- in effect only <b>among instances of the same application</b>.
 *       Another application using the same key is unaffected, and this is what the great
 *       majority of applications want</li>
 * </ul>
 *
 * <p>Both groups forward to the method taking a {@link Scope}, so they are default methods
 * here -- an implementation need only handle the three that do the work, and granularity is
 * not something each implementation should write for itself.
 *
 * @see MultiProcessingSyncService the only implementation
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
public interface ProcessingSyncService {

    // ------------------------------------------------------------------
    // Locks
    // ------------------------------------------------------------------

    /**
     * Obtains a lock at the given granularity.
     *
     * <p><b>One name returns one instance.</b> This is not to save objects --
     * {@link MultiProcessingMutex} relies on an internal local lock for in-process exclusion
     * and reentrancy counting, and creating one each time <b>voids that layer entirely</b>:
     * several threads in the same process would hold the lock at once, and nothing would report
     * it.
     *
     * @param key the application's own lock name, any string
     */
    ProcessingMutex mutex(Scope scope, String key);

    /**
     * A cluster lock: one holder across the whole cluster at a time, without regard to
     * application.
     *
     * @param key the application's own lock name, any string
     */
    default ProcessingMutex clusterMutex(String key) {
        return mutex(Scope.CLUSTER, key);
    }

    /**
     * An application lock: exclusive only among instances of the same application.
     *
     * @param key the application's own lock name, any string
     */
    default ProcessingMutex applicationMutex(String key) {
        return mutex(Scope.APPLICATION, key);
    }

    // ------------------------------------------------------------------
    // Latches
    // ------------------------------------------------------------------

    /**
     * Obtains a latch at the given granularity.
     *
     * @param key   the application's own name, any string
     * @param count the initial count -- how many to wait for
     * @throws IllegalArgumentException when the same name was already created in this process
     *                                  with <b>a different count</b>
     */
    ProcessingCountDownLatch latch(Scope scope, String key, long count);

    /**
     * A cluster-level latch: one across the whole cluster, <b>without regard to
     * application</b>.
     *
     * @param key   the application's own name, any string
     * @param count the initial count -- how many to wait for
     */
    default ProcessingCountDownLatch clusterLatch(String key, long count) {
        return latch(Scope.CLUSTER, key, count);
    }

    /**
     * An application-level latch: in effect only <b>among instances of the same
     * application</b>.
     *
     * @param key   the application's own name, any string
     * @param count the initial count -- how many to wait for
     */
    default ProcessingCountDownLatch applicationLatch(String key, long count) {
        return latch(Scope.APPLICATION, key, count);
    }

    /**
     * Drops this process's cached latch.
     *
     * <p>A latch is single-use, so starting a new round under the same name means dropping the
     * old instance first, or the "same name, different count" check refuses it. The leader's
     * own record is reclaimed once it falls idle.
     */
    void forgetLatch(Scope scope, String key);

    // ------------------------------------------------------------------
    // Barriers
    // ------------------------------------------------------------------

    /**
     * Obtains a barrier at the given granularity.
     *
     * @param key     the application's own name, any string
     * @param parties how many parties make it complete
     * @throws IllegalArgumentException when the same name was already created in this process
     *                                  with <b>a different party count</b>
     */
    ProcessingCyclicBarrier barrier(Scope scope, String key, int parties);

    /**
     * A cluster-level barrier: one across the whole cluster, <b>without regard to
     * application</b>.
     *
     * @param key     the application's own name, any string
     * @param parties how many parties make it complete
     */
    default ProcessingCyclicBarrier clusterBarrier(String key, int parties) {
        return barrier(Scope.CLUSTER, key, parties);
    }

    /**
     * An application-level barrier: in effect only <b>among instances of the same
     * application</b>.
     *
     * @param key     the application's own name, any string
     * @param parties how many parties make it complete
     */
    default ProcessingCyclicBarrier applicationBarrier(String key, int parties) {
        return barrier(Scope.APPLICATION, key, parties);
    }

    /** Drops this process's cached barrier, so it can be reopened with a different party
     *  count. */
    void forgetBarrier(Scope scope, String key);

    // ------------------------------------------------------------------
    // Semaphores
    // ------------------------------------------------------------------

    /**
     * Obtains a semaphore at the given granularity.
     *
     * <p><b>One name must carry the same permit count everywhere.</b> Where it does not, the
     * leader refuses and says so in the log -- running silently on one side's number would only
     * make "how many actually get through" a mystery.
     *
     * <p>One name returns one instance. Obtaining it again with a different {@code permits}
     * throws, since one name with two permit counts is a configuration error.
     *
     * @param permits the total permits; must be greater than 0
     */
    ProcessingSemaphore semaphore(Scope scope, String key, int permits);

    /**
     * A cluster-level semaphore: at most {@code permits} holders across the whole cluster at
     * a time, without regard to application.
     *
     * <pre>{@code
     * // At most three callers of this third-party API across the whole cluster
     * ProcessingSemaphore s = syncs.clusterSemaphore("third-party-api", 3);
     * }</pre>
     */
    default ProcessingSemaphore clusterSemaphore(String key, int permits) {
        return semaphore(Scope.CLUSTER, key, permits);
    }

    /**
     * An application-level semaphore: the quota is shared among instances of the same
     * application.
     *
     * <pre>{@code
     * // At most five concurrent consumers across all of this application's instances
     * ProcessingSemaphore s = syncs.applicationSemaphore("queue-consumer", 5);
     * }</pre>
     */
    default ProcessingSemaphore applicationSemaphore(String key, int permits) {
        return semaphore(Scope.APPLICATION, key, permits);
    }

    // ------------------------------------------------------------------
    // Troubleshooting
    // ------------------------------------------------------------------

    /** The locks this process has created. */
    Map<String, ProcessingMutex> knownMutexes();

    /** The latches this process has created. */
    Map<String, ProcessingCountDownLatch> knownLatches();

    /** The barriers this process has created. */
    Map<String, ProcessingCyclicBarrier> knownBarriers();

    /** The semaphores this process has created. */
    Map<String, ProcessingSemaphore> knownSemaphores();
}
