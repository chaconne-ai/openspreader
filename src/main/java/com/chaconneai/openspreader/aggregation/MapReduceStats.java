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
package com.chaconneai.openspreader.aggregation;

import java.io.Serializable;

/**
 * How much work one job did.
 *
 * <p>These numbers are <b>aggregated cluster-wide</b>: {@code mapCalls} is how many times map
 * ran across every node, and {@code mapNanos} is the sum of every node's map time -- so it
 * may well be <b>greater than</b> {@link #elapsedMs}, which is exactly the point of running
 * in parallel.
 *
 * <h2>The ratio of {@link #shuffled()} to {@link #emitted()} is what most deserves watching</h2>
 * It is the combiner's effect: {@code emitted} is how many intermediate records {@code map}
 * produced, and {@code shuffled} how many were actually <b>put on the network</b>. In word
 * counting the former can be dozens of times the latter.
 *
 * <p>Without a combiner the two are equal. Having enabled one and seeing almost no reduction
 * means the keys repeat very little, so the combiner is merely computing a second time for
 * nothing -- it can be turned off.
 *
 * <h2>Why the times are in nanoseconds</h2>
 * Each node's time is summed, and one shard's map may take only a few hundred microseconds.
 * In milliseconds, the rounding error accumulated over dozens of shards would exceed the
 * number itself.
 *
 * @param shards       how many shards it was cut into
 * @param reducers     how many nodes took part in reducing
 * @param mapCalls     how many times {@code map} was called. <b>Equal to the shard count</b>,
 *                     one per shard
 * @param emitted      how many intermediate records {@code map} emitted in total
 * @param shuffled     how many were actually sent. With a combiner, the count <b>after</b>
 *                     local combining
 * @param combineCalls how many times local combining was called; 0 without a combiner
 * @param reduceCalls  how many times {@code reduce} was called. <b>Equal to the number of
 *                     keys in the final result</b>
 * @param keys         how many distinct keys the final result holds
 * @param mapNanos     the sum of every node's {@code map} time, combining included
 * @param reduceNanos  the sum of every node's {@code reduce} time
 * @param elapsedMs    <b>wall-clock</b> time from submission to the complete result
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
public record MapReduceStats(
        int shards,
        int reducers,
        long mapCalls,
        long emitted,
        long shuffled,
        long combineCalls,
        long reduceCalls,
        long keys,
        long mapNanos,
        long reduceNanos,
        long elapsedMs) implements Serializable {

    /**
     * How much the combiner collapsed the intermediate records: {@code 1 - shuffled/emitted}.
     *
     * @return between 0 and 1; 0 without a combiner, or when nothing was collapsed
     */
    public double combineRatio() {
        if (emitted <= 0 || shuffled >= emitted) {
            return 0.0;
        }
        return 1.0 - (double) shuffled / emitted;
    }

    /**
     * Concurrency: the sum of every node's computation time divided by the wall clock.
     *
     * <h2>How to read it</h2>
     * <ul>
     *   <li><b>Well below 1</b> -- the time did not go into computing. The shards are too
     *       small and round trips and scheduling overhead outweigh the actual work, so a task
     *       like this <b>is faster computed locally than spread out</b>. Measured at 0.86 when
     *       each record did a single floating-point iteration</li>
     *   <li><b>Above 1</b> -- it genuinely is running in parallel. 4.75 with two thousand
     *       iterations per record</li>
     * </ul>
     *
     * <h2>It is not a speedup figure, and it inflates when the CPU is oversubscribed</h2>
     * The numerator sums each node's <b>wall-clock</b> time, not CPU time. With more threads
     * than available cores, most of each thread's wall clock is spent waiting to be
     * scheduled, and summing that inflates the figure -- <b>this number can exceed the
     * physical core count, which is physically impossible</b>.
     *
     * <p>Measured once: three nodes with twelve worker threads between them in a 4-core
     * container reported 10.52, while the real speedup was 1.13x.
     *
     * <p>So it answers "did the time go into computing" (compare it against 1) and
     * <b>does not answer</b> "how many times faster". Only measuring against a single-node
     * baseline answers that.
     */
    public double speedup() {
        if (elapsedMs <= 0) {
            return 0.0;
        }
        return (mapNanos + reduceNanos) / 1_000_000.0 / elapsedMs;
    }

    @Override
    public String toString() {
        return String.format(
                "MapReduceStats{%d shards / %d reducers, map ran %d times emitting %d -> %d sent "
                        + "(%.1f%% collapsed), reduce ran %d times yielding %d keys, "
                        + "compute %.1fms (map %.1f + reduce %.1f), wall clock %dms, "
                        + "concurrency %.2f}",
                shards, reducers, mapCalls, emitted, shuffled, combineRatio() * 100,
                reduceCalls, keys, (mapNanos + reduceNanos) / 1e6,
                mapNanos / 1e6, reduceNanos / 1e6, elapsedMs, speedup());
    }
}
