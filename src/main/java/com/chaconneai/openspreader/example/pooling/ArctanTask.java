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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.concurrent.CompletableFuture;

/**
 * Computes one stretch of the Taylor series for {@code arctan(1/x)}, used to assemble pi to
 * high precision.
 *
 * <pre>
 * arctan(1/x) = 1/x - 1/(3x³) + 1/(5x⁵) - 1/(7x⁷) + ...
 * </pre>
 *
 * <h2>Why each term can be computed on its own</h2>
 * Term {@code k} is {@code (-1)^k / ((2k+1)*x^(2k+1))} -- it <b>depends on k alone</b> and
 * needs to know nothing about where the previous term got to.
 *
 * <p>That is the entire reason it can be cut up and sent to other processes. Any series with
 * a recurrence -- Chudnovsky's, where each term follows from the last -- cannot be cut up at
 * all, however fast it converges.
 *
 * <h2>The computation here is real work</h2>
 * {@link BigDecimal} keeps the precision, and every term costs a division and an
 * exponentiation over hundreds of digits. At 100 decimal places the {@code x=5} branch takes
 * over 70 terms, each of them genuine big-number arithmetic -- not the sort of toy load that
 * finishes instantly and shows nothing about whether dispatching helped.
 *
 * @see PiCalculationBestPractice for how two arctan branches are assembled into pi
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
public class ArctanTask extends RecursiveTask<BigDecimal> {

    private static final long serialVersionUID = 1L;

    /**
     * How few terms before it stops splitting.
     *
     * <p>Not too small: one dispatch serialises the task, makes a network trip and serialises
     * the result back, costing a few hundred microseconds. With only two or three terms per
     * subtask, all the time goes into moving things about and splitting further makes it
     * slower.
     *
     * <p>Not too large either: what cannot be split cannot use the other replicas. Eight terms
     * at 100-digit precision is a few milliseconds of computation, comfortably heavier than a
     * round trip, which makes it about right.
     */
    private static final int THRESHOLD = 8;

    /** Guard digits. Intermediate results carry a few extra places and are truncated to the
     *  target precision at the end, so rounding error cannot climb into the significant digits. */
    private static final int GUARD_DIGITS = 10;

    private final int x;
    private final int from;
    private final int to;
    private final int scale;

    public ArctanTask(int x, int from, int to, int scale) {
        this.x = x;
        this.from = from;
        this.to = to;
        this.scale = scale;
    }

    /**
     * Computes {@code arctan(1/x)} to {@code scale} decimal places.
     *
     * <p>{@link #termsFor} works out the term count; the caller need not think about it.
     */
    public static ArctanTask of(int x, int scale) {
        return new ArctanTask(x, 0, termsFor(x, scale), scale);
    }

    /**
     * How many terms are needed for the target precision.
     *
     * <p>Term k is of order {@code 1/x^(2k+1)}, and requiring that to be below
     * {@code 10^-scale} solves to {@code k > scale / (2*log10(x))}. Five extra terms are kept
     * as margin.
     *
     * <p>So the larger {@code x}, the faster the convergence -- Machin's formula uses a 239
     * branch that needs a third of the terms the 5 branch does.
     */
    public static int termsFor(int x, int scale) {
        double perTerm = 2 * Math.log10(x);
        return (int) Math.ceil(scale / perTerm) + 5;
    }

    @Override
    protected BigDecimal compute() {
        int count = to - from;
        if (count <= 0) {
            return BigDecimal.ZERO;
        }
        if (count <= THRESHOLD) {
            return partialSum();
        }

        int mid = from + count / 2;
        // A forked subtask may land in another process; past the dispatch depth limit it stays
        // local automatically
        CompletableFuture<BigDecimal> left = fork(new ArctanTask(x, from, mid, scale));
        CompletableFuture<BigDecimal> right = fork(new ArctanTask(x, mid, to, scale));
        return join(left).add(join(right));
    }

    /**
     * The partial sum of this stretch.
     *
     * <p>Note it <b>does not</b> carry the previous term forward as a recurrence -- each term
     * is computed independently. A recurrence would be somewhat faster, but it would make this
     * stretch depend on the one before and therefore unsplittable. <b>Parallelisability
     * outweighs single-point efficiency</b>, a trade made over and over in distributed
     * computation.
     */
    private BigDecimal partialSum() {
        MathContext mc = new MathContext(scale + GUARD_DIGITS, RoundingMode.HALF_EVEN);
        BigDecimal base = BigDecimal.valueOf(x);
        BigDecimal sum = BigDecimal.ZERO;

        for (int k = from; k < to; k++) {
            int exponent = 2 * k + 1;
            // 1 / ((2k+1) · x^(2k+1))
            BigDecimal denominator = base.pow(exponent, mc)
                    .multiply(BigDecimal.valueOf(exponent), mc);
            BigDecimal term = BigDecimal.ONE.divide(denominator, mc);
            // Added for even k, subtracted for odd
            sum = (k & 1) == 0 ? sum.add(term, mc) : sum.subtract(term, mc);
        }
        return sum;
    }

    /**
     * Computes the whole stretch on the current thread, without the pool.
     *
     * <p>Use it as the <b>baseline</b> when comparing performance: no serialisation, no
     * network, no thread switching -- purely how long this computation itself takes. Without
     * the single-machine number, "340 milliseconds once dispatched" means nothing at all.
     *
     * <p>It differs from {@link #compute()} only in not forking: below the threshold, the two
     * run exactly the same code.
     */
    public BigDecimal computeDirectly() {
        int count = to - from;
        if (count <= 0) {
            return BigDecimal.ZERO;
        }
        if (count <= THRESHOLD) {
            return partialSum();
        }
        int mid = from + count / 2;
        return new ArctanTask(x, from, mid, scale).computeDirectly()
                .add(new ArctanTask(x, mid, to, scale).computeDirectly());
    }

    public int x() {
        return x;
    }

    public int from() {
        return from;
    }

    public int to() {
        return to;
    }

    @Override
    public String toString() {
        return "ArctanTask[1/" + x + ", terms " + from + "-" + to + ")";
    }
}
