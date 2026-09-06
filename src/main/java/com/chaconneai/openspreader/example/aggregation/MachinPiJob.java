package com.chaconneai.openspreader.example.aggregation;

import com.chaconneai.openspreader.aggregation.Emitter;
import com.chaconneai.openspreader.aggregation.MapReduceJob;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Computes pi to <b>arbitrary precision</b> with Machin's formula:
 * {@code pi = 16*arctan(1/5) - 4*arctan(1/239)}.
 *
 * <pre>{@code
 * var r = mapReduce.<MachinPiJob.Shard, String, BigDecimal, BigDecimal>submit(
 *         "machinPi", MachinPiJob.request(100));
 * BigDecimal pi = MachinPiJob.assemble(r, 100);
 * // 3.1415926535897932384626433832795028841971693993751058209749445923078164062862089986280348253421170679
 * }</pre>
 *
 * <h2>The fundamental difference from {@link MonteCarloPiJob}: this one is deterministic</h2>
 * Monte Carlo approximates by scattering points, with an error converging as
 * {@code 1.64/sqrt(n)} -- <b>each additional decimal place needs 100 times the samples</b>.
 * A hundred places would need {@code 10^200} points, while the universe holds some
 * {@code 10^80} atoms. And a {@code double} carries only 15 to 17 significant digits, so it
 * <b>cannot even hold the answer</b>.
 *
 * <p>This is a series summation over {@link BigDecimal}, giving as many digits as are asked
 * for. The cost is that every term does a hundred-digit exponentiation and division --
 * <b>real, adjustable work</b>, which is exactly what a load test wants.
 *
 * <p>There is another benefit: the result can be compared with known pi <b>digit by digit</b>,
 * where Monte Carlo can only assert that the error is below some value.
 *
 * <h2>Why this series divides</h2>
 * Term k is {@code (-1)^k / ((2k+1)*x^(2k+1))} -- it <b>depends on k alone</b> and needs to
 * know nothing about where the previous term got to. So any stretch of terms can be sent out
 * to be computed on its own.
 *
 * <p>A formula like Chudnovsky's, where each term follows from the last, converges far faster
 * at 14 digits a term, but <b>does not divide</b> -- and when parallelising, that property
 * matters more than the rate of convergence.
 *
 * <h2>Two keys only, with all the work in map</h2>
 * Every shard emits one of the two keys {@code arctan5} and {@code arctan239}, and reducing
 * merely adds a pile of BigDecimals. As in {@link MonteCarloPiJob}, shuffle here is only a
 * channel for gathering -- <b>MapReduce's value does not always lie in the parallelism of the
 * shuffle</b>.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
public class MachinPiJob implements MapReduceJob<MachinPiJob.Shard, String, BigDecimal, BigDecimal> {

    /** The two branches of Machin's formula. */
    public static final int BASE_5 = 5;
    public static final int BASE_239 = 239;

    /** Intermediate computation keeps a few extra digits and truncates at the end -- without
     *  them, rounding error eats the last few digits. */
    private static final int GUARD_DIGITS = 10;

    /**
     * One shard: a stretch of terms from one arctan branch.
     *
     * @param scale    how many decimal places to compute
     * @param base     5 or 239
     * @param fromTerm the first term, inclusive
     * @param toTerm   the last term, exclusive
     */
    public record Shard(int scale, int base, int fromTerm, int toTerm) implements Serializable {
    }

    /** The entire input is the one shard holding every term from term 0, with a {@code base}
     *  of 0 meaning both branches. */
    public static Shard request(int scale) {
        return new Shard(scale, 0, 0, 0);
    }

    /**
     * How many terms {@code scale} digits needs.
     *
     * <p>Term k of {@code arctan(1/x)} is roughly {@code x^-(2k+1)}, so each term contributes
     * {@code 2*log10(x)} digits: about 1.4 a term at x=5, and about 4.8 at x=239 -- which is
     * why the 239 branch needs far fewer terms.
     */
    static int termsFor(int base, int scale) {
        double digitsPerTerm = 2 * Math.log10(base);
        return (int) Math.ceil((scale + GUARD_DIGITS) / digitsPerTerm) + 1;
    }

    @Override
    public List<Shard> split(Shard input, int suggestedShards) {
        int scale = input.scale();
        if (scale <= 0) {
            return List.of();
        }
        // Divided a little more finely than the node count: the two branches differ greatly in
        // term count -- for 100 digits, x=5 needs 72 terms and x=239 only 21 -- so too coarse a
        // granularity leaves the quicker nodes idle, waiting on the slower ones
        int perBranch = Math.max(1, suggestedShards * 2);
        List<Shard> out = new ArrayList<>(perBranch * 2);
        appendShards(out, scale, BASE_5, perBranch);
        appendShards(out, scale, BASE_239, perBranch);
        return out;
    }

    private static void appendShards(List<Shard> out, int scale, int base, int pieces) {
        int terms = termsFor(base, scale);
        int chunk = Math.max(1, (terms + pieces - 1) / pieces);
        for (int from = 0; from < terms; from += chunk) {
            out.add(new Shard(scale, base, from, Math.min(from + chunk, terms)));
        }
    }

    @Override
    public void map(Shard shard, Emitter<String, BigDecimal> emitter) {
        MathContext mc = new MathContext(shard.scale() + GUARD_DIGITS, RoundingMode.HALF_EVEN);
        BigDecimal base = BigDecimal.valueOf(shard.base());
        BigDecimal sum = BigDecimal.ZERO;

        for (int k = shard.fromTerm(); k < shard.toTerm(); k++) {
            int exponent = 2 * k + 1;
            // 1 / ((2k+1) * x^(2k+1)), a hundred-digit computation every time
            BigDecimal denominator = base.pow(exponent, mc)
                    .multiply(BigDecimal.valueOf(exponent), mc);
            BigDecimal term = BigDecimal.ONE.divide(denominator, mc);
            sum = (k & 1) == 0 ? sum.add(term, mc) : sum.subtract(term, mc);
        }
        emitter.emit(keyOf(shard.base()), sum);
    }

    @Override
    public BigDecimal reduce(String key, List<BigDecimal> values) {
        // Added exactly, with no MathContext.
        //
        // Truncating here on its own initiative would be wrong: the key says nothing about how
        // many digits are wanted, and the partial sums already carry the guard digits map left
        // on them. Addition produces no non-terminating decimal, so exact arithmetic cannot
        // throw, and the real truncation is left to assemble, where it happens once.
        BigDecimal sum = BigDecimal.ZERO;
        for (BigDecimal v : values) {
            sum = sum.add(v);
        }
        return sum;
    }

    /** Addition is associative, so adding some first and the rest afterwards gives the same
     *  answer. */
    @Override
    public boolean combinable() {
        return true;
    }

    static String keyOf(int base) {
        return "arctan" + base;
    }

    /**
     * Assembles the two branches into pi: {@code 16*arctan(1/5) - 4*arctan(1/239)}.
     *
     * <h2>The last step <b>truncates</b> rather than rounds, and the difference is real</h2>
     * Pi's 101st decimal place is an <b>8</b>:
     *
     * <pre>
     * ...3421170679 | 8214808651...
     *               ^ the 101st place
     * </pre>
     *
     * So "pi to 100 decimal places" has two answers, both correct and not equal:
     * <ul>
     *   <li><b>Truncated</b>, {@code ...170679} -- what every published table of pi's digits
     *       uses</li>
     *   <li><b>Rounded</b>, {@code ...170680} -- because the 101st digit, 8, is at least 5 and
     *       carries</li>
     * </ul>
     *
     * <p>This truncates ({@link RoundingMode#DOWN}), so the result compares directly, digit by
     * digit, against a published table such as {@link #PI_100}. The first version used
     * {@code HALF_EVEN}, and the {@code ...680} it produced is mathematically sound but does
     * not match the tables -- <b>it looks like a miscalculation, when in fact "accurate to 100
     * places" is itself ambiguous</b>.
     *
     * <p>Truncating is only usable here because <b>the internal precision is high enough</b>:
     * the intermediate computation keeps {@value #GUARD_DIGITS} guard digits, and the true
     * value only diverges beyond the 101st place, so what is cut away is certainly surplus and
     * never a digit that should have been kept.
     *
     * @param scale how many decimal places to keep
     */
    public static BigDecimal assemble(Map<String, BigDecimal> parts, int scale) {
        BigDecimal a = parts.get(keyOf(BASE_5));
        BigDecimal b = parts.get(keyOf(BASE_239));
        if (a == null || b == null) {
            throw new IllegalStateException("the result is incomplete: "
                    + (a == null ? "arctan5" : "arctan239") + " is missing. Keys received: "
                    + parts.keySet());
        }
        MathContext mc = new MathContext(scale + GUARD_DIGITS, RoundingMode.HALF_EVEN);
        return a.multiply(BigDecimal.valueOf(16), mc)
                .subtract(b.multiply(BigDecimal.valueOf(4), mc), mc)
                .setScale(scale, RoundingMode.DOWN);
    }

    /**
     * Pi's first 100 decimal places, for <b>digit-by-digit comparison</b>.
     *
     * <p>This is this job's greatest advantage over Monte Carlo: the result is determined, so a
     * test can assert "not one digit out" rather than "the error is below some value".
     */
    public static final String PI_100 =
            "3.1415926535897932384626433832795028841971693993751"
                    + "058209749445923078164062862089986280348253421170679";
}
