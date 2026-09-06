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
package com.chaconneai.openspreader.example.pooling;

import com.chaconneai.openspreader.pooling.RecursiveTask;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fibonacci, used to load-test fork/join across processes.
 *
 * <p>It was chosen because its shape suits the test: <b>the computation grows exponentially
 * in n while the data to transmit is a single integer</b>. That is, the computational
 * benefit far outweighs the network cost, so dispatching to another process pays -- which is
 * the only situation in which cross-process fork/join makes sense at all.
 *
 * <p>Which also states the boundary: a task carrying a lot of data for little computation --
 * "sort this List", say -- has its benefit eaten entirely by serialisation and the network,
 * and should not be sent out.
 *
 * <h2>Two thresholds; do not confuse them</h2>
 * <ul>
 *   <li><b>The split threshold</b> ({@code n - 8}, adapting with n) -- below it, the value is
 *       computed by plain single-threaded recursion. A fixed absolute value will not do: the
 *       leaf count changes by orders of magnitude as n changes.</li>
 *   <li><b>The dispatch depth limit</b> (the {@code maxDepth} setting, 3 by default) --
 *       beyond it, splitting continues only in the local fork/join pool and nothing more is
 *       sent to other processes.</li>
 * </ul>
 *
 * <h2>Two things to watch when measuring</h2>
 * <ol>
 *   <li><b>n must be large enough.</b> fib(34) takes twenty-odd milliseconds
 *       single-threaded, so dispatch costs far more than the computation and the measurement
 *       shows nothing but "distributed is slower". To see the value, the single-machine
 *       baseline needs to reach seconds -- take n above 40.</li>
 *   <li><b>The first number does not count.</b> The peer process's JIT is cold and the first
 *       run is often an order of magnitude slower than those after it, so warm up before
 *       measuring.</li>
 * </ol>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class FibTask extends RecursiveTask<Long> {

    private static final long serialVersionUID = 1L;

    /**
     * How far below n to stop splitting.
     *
     * <p>Fibonacci's recursion tree is unbalanced, and splitting k levels produces roughly
     * fib(k+1) leaves. A value of 8 means about 34 leaf tasks -- enough to spread across
     * several processes, and not so fragmented that scheduling overhead swamps the
     * computation. A fixed absolute threshold will not do: the leaf count changes by orders
     * of magnitude as n changes.
     */
    private static final int SPLIT_LEVELS = 8;

    /** Below this there is no point splitting: a round trip costs more than computing it. */
    private static final int MIN_THRESHOLD = 20;

    /** How many leaf tasks this process computed, for observing the load distribution. */
    private static final AtomicLong LEAVES = new AtomicLong();

    /**
     * Milliseconds this process spent on leaf computation, <b>accumulated across
     * threads</b>.
     *
     * <p>It exceeds wall-clock time -- eight threads running 100ms each accumulate to 800ms
     * here. Use it to compare the load across processes, not against the total elapsed
     * time.
     */
    private static final AtomicLong LEAF_MILLIS = new AtomicLong();

    private final int n;

    /** Below this it is computed by single-threaded recursion, with no further splitting. */
    private final int threshold;

    public FibTask(int n) {
        this(n, Math.max(MIN_THRESHOLD, n - SPLIT_LEVELS));
    }

    public FibTask(int n, int threshold) {
        this.n = n;
        this.threshold = threshold;
    }

    @Override
    protected Long compute() {
        if (n <= threshold) {
            long t0 = System.currentTimeMillis();
            long result = fibSequential(n);
            LEAVES.incrementAndGet();
            LEAF_MILLIS.addAndGet(System.currentTimeMillis() - t0);
            return result;
        }
        // The threshold has to be passed down, or subtasks would each recompute it from their
        // own n and split ever more finely
        CompletableFuture<Long> f1 = fork(new FibTask(n - 1, threshold));
        CompletableFuture<Long> f2 = fork(new FibTask(n - 2, threshold));
        return join(f1) + join(f2);
    }

    /** Plain recursion, deliberately without memoisation -- what is wanted here is a
     *  controllable CPU load. */
    public static long fibSequential(int n) {
        return n <= 1 ? n : fibSequential(n - 1) + fibSequential(n - 2);
    }

    public static long leaves() {
        return LEAVES.get();
    }

    public static long leafMillis() {
        return LEAF_MILLIS.get();
    }

    public static void reset() {
        LEAVES.set(0);
        LEAF_MILLIS.set(0);
    }
}
