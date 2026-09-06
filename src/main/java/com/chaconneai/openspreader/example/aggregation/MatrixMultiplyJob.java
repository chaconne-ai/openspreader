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
package com.chaconneai.openspreader.example.aggregation;

import com.chaconneai.openspreader.aggregation.Emitter;
import com.chaconneai.openspreader.aggregation.MapReduceJob;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Matrix multiplication, {@code C = A x B}, divided by rows and spread across the cluster.
 *
 * <pre>{@code
 * Map<Integer, double[]> rows = mapReduce
 *         .<MatrixMultiplyJob.Block, Integer, double[], double[]>submit(
 *                 "matrixMultiply", MatrixMultiplyJob.Block.of(a, b))
 *         .get(5, TimeUnit.MINUTES);
 *
 * double[][] c = MatrixMultiplyJob.assemble(rows);
 * }</pre>
 *
 * <h2>What this example shows is the shape where reduce has nothing to do</h2>
 * Each row of {@code C} depends only on the corresponding row of {@code A} and the whole of
 * {@code B}, so <b>one row's result is finished on one mapper</b>. Each row index has a single
 * intermediate value, and the {@code values} {@link #reduce} receives always holds one element
 * -- all it does is take the result out.
 *
 * <p>This is not a design mistake. Among MapReduce's three phases, reduce is often mere
 * <b>collection</b>: the computation is all in map, and shuffle puts the results back in place
 * by key. {@link WordCountJob} is the other extreme, where reduce is the point, and
 * {@link MonteCarloPiJob} sits between them -- a heavy map, and a reduce that only sums.
 *
 * <h2>B is copied onto every shard</h2>
 * Every shard carries a complete {@code B}, because any row of {@code C} needs every column of
 * {@code B}. Eight shards means <b>eight copies of B travelling the network</b>.
 *
 * <p>A 1000x1000 matrix of doubles is 8MB, so eight shards is 64MB. This example therefore has
 * a hard ceiling on scale, and <b>the finer it is divided the more is transferred</b> -- in
 * direct conflict with "divide finely so the quicker nodes take more shards".
 *
 * <p>Genuinely large matrices call for an algorithm that divides by blocks rather than rows, so
 * that each block needs only the corresponding strips of {@code A} and {@code B}. That is
 * another thing entirely, and it says plainly that <b>this framework is not for dense linear
 * algebra</b>.
 *
 * <h2>On GPUs</h2>
 * Matrix multiplication is the classic GPU workload, but <b>this is a pure CPU
 * implementation</b>, because reaching a GPU from Java needs a JNI binding -- JCuda,
 * TornadoVM, aparapi -- and {@code spreader} has no dependencies, so it should not bind such a
 * library in for the sake of one example.
 *
 * <p>To put a GPU behind it, <b>{@link #map} is the single point of entry</b>: replace the
 * triple loop inside with one kernel invocation, and the rest -- dividing, dispatching,
 * placing results, handling failures -- does not change by a line. That is precisely what this
 * interface is for: <b>separating what is computed from where it runs, how it divides, and how
 * it combines</b>.
 *
 * <p>Two warnings: shards too large for GPU memory must be divided down in {@link #split}; and
 * a GPU context <b>is not thread-safe</b>, while map is called concurrently by the inbound
 * thread pool, so the job implementation has to isolate them itself -- one context per thread,
 * or serialised access.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
public class MatrixMultiplyJob
        implements MapReduceJob<MatrixMultiplyJob.Block, Integer, double[], double[]> {

    /**
     * One shard: a run of consecutive rows of {@code A}, plus the whole of {@code B}.
     *
     * <p>The input and a shard are <b>the same type</b>, as the interface defines it, so the
     * entire input is the one shard holding every row from row 0.
     *
     * @param startRow where these rows begin in the original {@code A}, used to put the results
     *                 back in place
     */
    public record Block(int startRow, double[][] a, double[][] b) implements Serializable {

        public static Block of(double[][] a, double[][] b) {
            return new Block(0, a, b);
        }
    }

    @Override
    public List<Block> split(Block input, int suggestedShards) {
        double[][] a = input.a();
        double[][] b = input.b();
        if (a == null || b == null || a.length == 0 || b.length == 0) {
            return List.of();
        }
        if (a[0].length != b.length) {
            throw new IllegalArgumentException("the dimensions do not agree: A is " + a.length
                    + "x" + a[0].length + " and B is " + b.length + "x" + b[0].length
                    + "; A's column count must equal B's row count");
        }

        // Not divided more finely than the node count: each extra shard transfers another
        // complete B. "Divide finely so the quicker nodes take more shards" is right in other
        // jobs; here it is exactly backwards
        int shards = Math.max(1, Math.min(suggestedShards, a.length));
        int rowsPerShard = (a.length + shards - 1) / shards;

        List<Block> out = new ArrayList<>(shards);
        for (int start = 0; start < a.length; start += rowsPerShard) {
            int end = Math.min(start + rowsPerShard, a.length);
            double[][] slice = new double[end - start][];
            // Row references are copied, not the rows themselves -- each row is really copied
            // once, at serialisation
            System.arraycopy(a, start, slice, 0, end - start);
            out.add(new Block(start, slice, b));
        }
        return out;
    }

    /**
     * Computes the rows of {@code C} this shard corresponds to.
     *
     * <p>The loop order is <b>i-k-j</b> rather than the textbook i-j-k: the inner loop walks
     * {@code b[k]} and {@code row} contiguously, which is far kinder to the CPU cache. For the
     * same number of floating-point operations, large matrices can differ severalfold.
     *
     * <p><b>Putting a GPU behind it means replacing this stretch</b>, and nothing else.
     */
    @Override
    public void map(Block shard, Emitter<Integer, double[]> emitter) {
        double[][] a = shard.a();
        double[][] b = shard.b();
        int cols = b[0].length;

        for (int i = 0; i < a.length; i++) {
            double[] row = new double[cols];
            double[] ai = a[i];
            for (int k = 0; k < ai.length; k++) {
                double aik = ai[k];
                if (aik == 0.0) {
                    // On a sparse matrix this saves an entire pass of the inner loop
                    continue;
                }
                double[] bk = b[k];
                for (int j = 0; j < cols; j++) {
                    row[j] += aik * bk[j];
                }
            }
            // The key is the global row index, not the index within the shard, or row 0 of
            // different shards would collide
            emitter.emit(shard.startRow() + i, row);
        }
    }

    /**
     * Each row index has one result, and this takes it out.
     *
     * <p>{@code values} <b>must have size 1</b>: one row is computed by one mapper. A second
     * one means the shards overlapped -- some rows computed twice, and others possibly not at
     * all. Returning either of them would be wrong, so it fails outright.
     */
    @Override
    public double[] reduce(Integer rowIndex, List<double[]> values) {
        if (values.size() != 1) {
            throw new IllegalStateException("row " + rowIndex + " received " + values.size()
                    + " results where there should be exactly 1, which means split produced "
                    + "overlapping shards");
        }
        return values.get(0);
    }

    /**
     * <b>Off</b> -- and deliberately off, not forgotten.
     *
     * <p>Combining locally first would destroy the premise {@link #reduce} rests on: it detects
     * overlapping shards through "{@code values} must hold exactly 1", and after a combine each
     * key holds one anyway, so that assertion could never find anything again.
     *
     * <p>There is nothing to gain here either -- each row index is emitted once, so there are
     * no duplicate keys to merge.
     */
    @Override
    public boolean combinable() {
        return false;
    }

    /** Assembles results indexed by row back into a two-dimensional array. */
    public static double[][] assemble(Map<Integer, double[]> rows) {
        double[][] c = new double[rows.size()][];
        for (Map.Entry<Integer, double[]> e : rows.entrySet()) {
            int index = e.getKey();
            if (index < 0 || index >= c.length) {
                throw new IllegalStateException("row index " + index + " is outside the range "
                        + "0.." + (c.length - 1) + ", so the result is incomplete");
            }
            c[index] = e.getValue();
        }
        return c;
    }
}
