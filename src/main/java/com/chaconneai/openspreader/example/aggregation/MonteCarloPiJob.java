package com.chaconneai.openspreader.example.aggregation;

import com.chaconneai.openspreader.aggregation.Emitter;
import com.chaconneai.openspreader.aggregation.MapReduceJob;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Estimating pi by Monte Carlo -- <b>the input is a single number and the computation is
 * arbitrarily large</b>.
 *
 * <pre>{@code
 * Map<String, Long> r = mapReduce
 *         .<Long, String, Long, Long>submit("montePi", 1_000_000_000L)
 *         .get(5, TimeUnit.MINUTES);
 *
 * double pi = 4.0 * r.get(HITS) / r.get(TOTAL);
 * }</pre>
 *
 * <h2>Why this example is worth looking at separately</h2>
 * {@link WordCountJob} is much data computed quickly; this is the opposite. The
 * <b>entire shard the submitter sends is one long</b> -- how many points this shard should
 * scatter -- while the computation runs to billions of operations.
 *
 * <p>This is the shape the toolkit is <b>genuinely suited to</b>. Shards are serialised and
 * sent as cluster messages, so the data volume has a ceiling; the computation does not, and
 * spread across five nodes it is five times the CPU.
 *
 * <p>Conversely, where the bottleneck is moving the data rather than finishing the
 * computation, use a real big-data stack instead of this.
 *
 * <h2>How it works</h2>
 * Scatter points at random into a unit square, and the proportion falling inside the
 * inscribed quarter circle approaches {@code pi/4}. More points give more accuracy, with the
 * error converging as {@code 1/sqrt(n)} -- one more digit of precision costs a hundred times
 * the points -- which makes it a natural example of "give it as much compute as you have".
 *
 * <h2>Only two keys, so the shuffle has almost no parallelism -- and that is fine</h2>
 * Every shard emits the same two keys, {@link #HITS} and {@link #TOTAL}, so the whole
 * cluster's intermediate results land on <b>at most two nodes</b> to be reduced.
 *
 * <p>It looks wasteful and is not: <b>all the work is in the map phase</b>, and reducing is
 * adding a few numbers together. MapReduce's value does not always lie in shuffle
 * parallelism -- here the shuffle is merely a channel for gathering totals.
 *
 * <h2>Random numbers come from {@code ThreadLocalRandom}</h2>
 * Not for speed but for <b>correctness</b>. Sharing one {@code Random} has threads contending
 * on a CAS over the same seed, which is not only slow but interleaves the sequences each
 * thread receives -- and Monte Carlo rests on the samples being independent.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
public class MonteCarloPiJob implements MapReduceJob<Long, String, Long, Long> {

    /** Points that fell inside the circle. */
    public static final String HITS = "hits";

    /** How many points were scattered in total. It is reported back rather than trusted from
     *  the submitter's input, so that a shard failing to finish does not skew the ratio. */
    public static final String TOTAL = "total";

    /**
     * Divides the total sample count evenly into shards.
     *
     * <p>More shards than nodes -- four times as many here. The nodes do not have equal
     * compute, and with shards smaller and more numerous the fast ones naturally take more,
     * rather than everything being held back by the slowest. This is the right way to have a
     * stronger machine do more work: control it at the sharding step, not afterwards with a
     * weighted strategy for choosing nodes.
     */
    @Override
    public List<Long> split(Long totalSamples, int suggestedShards) {
        if (totalSamples == null || totalSamples <= 0) {
            return List.of();
        }
        int shards = Math.max(1, suggestedShards * 4);
        shards = (int) Math.min(shards, totalSamples);

        List<Long> out = new ArrayList<>(shards);
        long base = totalSamples / shards;
        long remainder = totalSamples % shards;
        for (int i = 0; i < shards; i++) {
            // The remainder is spread over the first few shards rather than dropped: dropping
            // it would make the total wrong and skew the estimate of pi
            out.add(base + (i < remainder ? 1 : 0));
        }
        return out;
    }

    @Override
    public void map(Long samples, Emitter<String, Long> emitter) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long hits = 0;
        for (long i = 0; i < samples; i++) {
            double x = random.nextDouble();
            double y = random.nextDouble();
            // Compare x^2 + y^2 against 1 without a square root: it would be an expensive
            // operation per point and would not change the comparison
            if (x * x + y * y <= 1.0) {
                hits++;
            }
        }
        // Two records per shard, not one per point. The volume of intermediate results is
        // independent of the sample count, so the sample count can grow arbitrarily without
        // overwhelming the network
        emitter.emit(HITS, hits);
        emitter.emit(TOTAL, samples);
    }

    @Override
    public Long reduce(String key, List<Long> values) {
        long sum = 0;
        for (Long v : values) {
            sum += v;
        }
        return sum;
    }

    /**
     * On, though it gains almost nothing here -- {@code map} emits two records per shard, so
     * there is next to nothing to combine.
     *
     * <p>It is declared because the operation <b>genuinely is</b> associative, and because
     * should this later emit per thread rather than per shard, the combiner would take effect
     * immediately.
     */
    @Override
    public boolean combinable() {
        return true;
    }

    /** Converts the result into pi. */
    public static double toPi(Map<String, Long> result) {
        Long hits = result.get(HITS);
        Long total = result.get(TOTAL);
        if (hits == null || total == null || total == 0) {
            throw new IllegalStateException("the result is incomplete: " + result);
        }
        return 4.0 * hits / total;
    }
}
