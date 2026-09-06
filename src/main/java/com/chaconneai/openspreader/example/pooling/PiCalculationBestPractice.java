package com.chaconneai.openspreader.example.pooling;

import com.chaconneai.openspreader.pooling.ForkJoinMultiProcessingPool;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.concurrent.CompletableFuture;

/**
 * Computing pi to 100 decimal places across the whole cluster -- a complete example of a
 * recursive divide-and-conquer task.
 *
 * <h2>In one sentence</h2>
 * A large computation is divided into pieces, spread across every replica of the same
 * application, and combined again. Going from one replica to three changes <b>not a word</b>
 * of the code.
 *
 * <pre>{@code
 * @Autowired
 * private ForkJoinMultiProcessingPool pool;
 *
 * BigDecimal pi = new PiCalculationBestPractice(pool).computePi(100);
 * // 3.1415926535897932384626433832795028841971693993751058209749445923...
 * }</pre>
 *
 * <h2>It uses Machin's formula</h2>
 * <pre>
 * pi = 16*arctan(1/5) - 4*arctan(1/239)
 * </pre>
 *
 * <p>Why not the textbook {@code pi/4 = 1 - 1/3 + 1/5 - ...}: it <b>converges far too
 * slowly</b>, needing 10^100 terms for 100 digits -- more than the age of the universe allows.
 * Machin's formula needs some 70, because {@code 1/5} and {@code 1/239} are both small and
 * decay very fast once raised to a power.
 *
 * <h2>Why it has to be BigDecimal</h2>
 * A {@code double} carries 15 to 17 significant digits and <b>cannot hold even 20 decimal
 * places</b>, let alone 100. So the whole path is {@link BigDecimal}, and every term is a
 * hundred-digit division and exponentiation -- real work, not a toy load finished in an
 * instant that says nothing about whether dispatch helped.
 *
 * <h2>Why this computation suits dispatch</h2>
 * <ol>
 *   <li><b>The terms are independent</b> -- term k depends on k alone, so it divides any way
 *       you like. A formula like Chudnovsky's, where each term follows from the last,
 *       converges far faster but <b>does not divide</b>, and one node is no quicker for
 *       it</li>
 *   <li><b>Pure computation</b> -- no IO, no side effects. This is a <b>hard requirement</b>
 *       for a recursive task: a subtask that fails at the far end is recomputed locally, so a
 *       database write or a message sent inside compute() would happen twice</li>
 *   <li><b>Enough work to be worth it</b> -- enough to pay for serialisation and a network
 *       round trip</li>
 * </ol>
 *
 * @see ArctanTask how one arctan branch divides
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
public class PiCalculationBestPractice {

    /** The two branches of Machin's formula. */
    private static final int MACHIN_A = 5;
    private static final int MACHIN_B = 239;

    private static final BigDecimal SIXTEEN = BigDecimal.valueOf(16);
    private static final BigDecimal FOUR = BigDecimal.valueOf(4);

    /** Intermediate results carry a few extra digits and are truncated at the end, so rounding
     *  error cannot creep into the significant ones. */
    private static final int GUARD_DIGITS = 10;

    private final ForkJoinMultiProcessingPool pool;

    public PiCalculationBestPractice(ForkJoinMultiProcessingPool pool) {
        this.pool = pool;
    }

    /**
     * Computes pi to {@code scale} decimal places.
     *
     * <h2>The two arctan branches run <b>in parallel</b></h2>
     * Two futures from {@code submit}, awaited together, rather than {@code invoke} twice --
     * the latter is serial and wastes half the time for nothing.
     *
     * <p>And each branch divides in two internally, so the subtasks finally spread out number
     * in the dozens, and every replica in the cluster gets work.
     */
    public BigDecimal computePi(int scale) {
        CompletableFuture<BigDecimal> a = pool.submit(ArctanTask.of(MACHIN_A, scale));
        CompletableFuture<BigDecimal> b = pool.submit(ArctanTask.of(MACHIN_B, scale));

        MathContext mc = new MathContext(scale + GUARD_DIGITS, RoundingMode.HALF_EVEN);
        BigDecimal pi = a.join().multiply(SIXTEEN, mc)
                .subtract(b.join().multiply(FOUR, mc), mc);

        // The guard digits end here; truncate to the precision the caller asked for
        return pi.setScale(scale, RoundingMode.HALF_EVEN);
    }

    /** 100 places by default. */
    public BigDecimal computePi() {
        return computePi(100);
    }

    /**
     * Computes pi, bringing back <b>how long it took</b> and <b>how many replicas took
     * part</b>.
     *
     * <h2>Why measure</h2>
     * Dispatch is not free: a task is serialised, crosses the network, and its result is
     * combined again. Where the computation is small, that overhead exceeds the time saved and
     * <b>dispatching is slower</b>.
     *
     * <p>So "should this be dispatched" is a question about numbers, not a slogan. This method
     * is how you measure it for yourself:
     *
     * <pre>{@code
     * // Starting with a single replica
     * TimedResult<BigDecimal> r1 = practice.computePiTimed(1000);
     * // computed locally on one replica, 820 ms
     *
     * // Start two more replicas, without changing a word of the code
     * TimedResult<BigDecimal> r3 = practice.computePiTimed(1000);
     * // three replicas took part, 340 ms
     * }</pre>
     *
     * <p>Try it again with a scale of 50 and the result reverses -- that little computation
     * does not pay for the network. <b>Where the crossover lies depends on your machines and
     * your network, and can only be measured.</b>
     */
    public TimedResult<BigDecimal> computePiTimed(int scale) {
        return TimedResult.measure(() -> computePi(scale), pool.peerCount());
    }

    /**
     * Runs the same computation <b>locally by force</b>, to compare against the dispatched
     * result.
     *
     * <p>A performance comparison needs this baseline: without knowing what one machine takes,
     * "340 milliseconds once dispatched" means nothing at all.
     *
     * <p>It simply calls {@code compute()} directly, bypassing the pool entirely -- no
     * serialisation, no network, and no thread switch.
     */
    public TimedResult<BigDecimal> computePiLocally(int scale) {
        return TimedResult.measure(() -> {
            BigDecimal a = ArctanTask.of(MACHIN_A, scale).computeDirectly();
            BigDecimal b = ArctanTask.of(MACHIN_B, scale).computeDirectly();
            MathContext mc = new MathContext(scale + GUARD_DIGITS, RoundingMode.HALF_EVEN);
            return a.multiply(SIXTEEN, mc)
                    .subtract(b.multiply(FOUR, mc), mc)
                    .setScale(scale, RoundingMode.HALF_EVEN);
        }, 0);
    }

    /**
     * The usage that does not block the calling thread.
     *
     * <p>This is what a web request wants: the computation runs in the pool and the request
     * thread returns to other work at once.
     *
     * <p><b>Always give {@code get()} a timeout</b> when taking the result. When a node dies
     * partway through, the service has timeouts of its own, but a network partition does not
     * guarantee they fire -- and a {@code get()} without one hangs the calling thread for
     * good.
     */
    public CompletableFuture<BigDecimal> computePiAsync(int scale) {
        return CompletableFuture.supplyAsync(() -> computePi(scale));
    }

    /**
     * What to do when the replica count changes -- <b>nothing at all</b>.
     *
     * <p>Target nodes are <b>looked up afresh on every dispatch</b>, and nowhere caches "how
     * many nodes the cluster has":
     *
     * <ul>
     *   <li>One replica: computed locally, without the network and without serialisation</li>
     *   <li>A second replica joins: the <b>next</b> fork starts dispatching outward, with no
     *       restart</li>
     *   <li>Every replica leaves: it returns to computing locally by itself</li>
     * </ul>
     *
     * <p>So local development, one process, and production, a crowd of replicas, run the same
     * code, with no switch and no test on the environment.
     */
    public BigDecimal sameCodeAtAnyScale(int scale) {
        return computePi(scale);
    }

    /**
     * When checking the result, <b>do not compare for equality against a literal pi</b>.
     *
     * <p>A series is an <b>approximation</b>: computed to {@code scale} places, the last digit
     * or two may differ from the true value through truncation and rounding. The right way is
     * to compare a leading prefix, or to allow a margin.
     *
     * @param scale  how many places to compute
     * @param digits how many leading places to check; it should be below scale, leaving room
     *               for rounding
     */
    public boolean matchesKnownPi(int scale, int digits) {
        String computed = computePi(scale).toPlainString();
        return computed.startsWith(PI_100.substring(0, digits + 2));   // "3." takes two
    }

    /**
     * Pi's first 100 decimal places, for checking the answer.
     *
     * <p>Hard-coding it here is only a reference for examples and tests, and production code
     * should not do this -- where a fixed value is what is wanted, use the constant and do not
     * compute anything.
     */
    public static final String PI_100 =
            "3.1415926535897932384626433832795028841971693993751"
                    + "0582097494459230781640628620899862803482534211706";
}
