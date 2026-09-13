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
package com.chaconneai.openspreader.dag;

import com.chaconneai.openspreader.pooling.ProcessingPool;

import java.util.concurrent.ExecutorService;

/**
 * Everything the engine borrows in order to run. Which is, deliberately, one thing.
 *
 * <h2>Why only the pool</h2>
 * This project weaves graphs; it does not build infrastructure. Every capability a
 * distributed DAG needs turns out to be something openspreader already does:
 *
 * <ul>
 *   <li><b>Running a step somewhere in the cluster</b>, picking a replica, falling back to a
 *       local call when there is no other replica, serialising the arguments, timing out,
 *       wrapping failures: {@link ProcessingPool}</li>
 *   <li><b>Carrying the state to the step</b>: the pool serialises call arguments already, so
 *       the slice travels as an argument. Putting the state in the replicated cache instead
 *       was considered and dropped: it would mean inventing a key layout, a TTL and a cleanup
 *       pass, which is building a component to replace one that already works</li>
 *   <li><b>Waiting for the steps</b>: the pool hands back a {@code CompletableFuture}. The
 *       JDK's own primitive, nothing added</li>
 * </ul>
 *
 * <p>The second and third fields are for nodes declared {@code local(...)}: work that happens
 * outside the application, which there is no reason to ship to another replica. They are the
 * node registry that already exists on this instance and a pool of threads to block on, both
 * borrowed as well.
 *
 * <h2>What is not borrowed, and why</h2>
 * <ul>
 *   <li><b>The distributed latch</b> would suit "wait for N branches", but the counting
 *       happens on the coordinator, which already holds every future. A cross-process latch
 *       would add a round trip to the leader per count to buy nothing</li>
 *   <li><b>The barrier</b> aligns parties round after round. A DAG has no rounds</li>
 *   <li><b>The exchanger</b> swaps items between two parties. A DAG edge carries one way</li>
 *   <li><b>The distributed lock</b> would keep one run to one owner, which matters when runs
 *       arrive on a shared queue. Here a run starts with a method call, so its owner is
 *       whoever called</li>
 * </ul>
 * Each of these is a good component used for the wrong shape. Reaching for one because it is
 * there is how a dependency becomes a liability.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public record DagRuntime(ProcessingPool pool, NodeDispatcher local,
                         ExecutorService localExecutor, GraphRenderer renderer) {

    public DagRuntime {
        if (pool == null) {
            throw new IllegalArgumentException("a DagRuntime needs a ProcessingPool");
        }
    }

    /**
     * A runtime that can only dispatch.
     *
     * <p>Graphs with no {@code local(...)} node need nothing else. One that has them fails
     * with a message saying which bean is missing, rather than a null pointer.
     */
    public DagRuntime(ProcessingPool pool) {
        this(pool, null, null, null);
    }

    /** A runtime that can dispatch and run in place, rendering as Mermaid. */
    public DagRuntime(ProcessingPool pool, NodeDispatcher local, ExecutorService localExecutor) {
        this(pool, local, localExecutor, null);
    }

    /** Whether this runtime can run a node in place. */
    public boolean canRunLocally() {
        return local != null && localExecutor != null;
    }
}
