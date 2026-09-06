package com.chaconneai.openspreader.bloom;

import com.chaconneai.openspreader.cache.ProcessingCache;

/**
 * A Bloom filter built on the cluster cache: one process {@code put}s a value in, and every
 * other process sees it in {@code mightContain} at once.
 *
 * <pre>{@code
 * ProcessingBloomFilter seen = ProcessingBloomFilter.create(cache, "seen:orders", 1_000_000, 0.01);
 * if (!seen.mightContain(orderId)) {
 *     // Certainly never seen -- safe to go to the database
 * }
 * seen.put(orderId);
 * }</pre>
 *
 * <h2>Why it can be built on the cluster cache</h2>
 * {@link ProcessingCache#setbit} is what makes it possible: the cache replicates
 * <b>operations</b> rather than data, so broadcasting "set bit N" is a few dozen bytes,
 * <b>entirely independent of how large the bitmap is</b>. One {@code put} is k setbits, and k
 * is usually a single digit.
 *
 * <p>Without a bit operation -- with only "read the whole bitmap, change a few bits, write it
 * back" -- a filter for a hundred million entries would be 120MB, and every put would
 * broadcast 120MB. The approach simply would not stand.
 *
 * <h2>A partition can produce false negatives</h2>
 * <b>This determines where it can be used; read it through.</b>
 *
 * <p>A Bloom filter's entire value rests on "<b>when it says no, the answer is certainly
 * no</b>". In this cluster, leader uniqueness is <b>an agreement about timing</b> -- whoever
 * takes the cluster port is the leader -- and <b>not consensus</b>. Under a network partition
 * each side may have a leader accepting setbits of its own, and merging afterwards <b>loses
 * bits</b> -- and a filter that has lost bits answers "not present" for a key that really was
 * put in.
 *
 * <table border="1">
 *   <caption>Whether it can be used</caption>
 *   <tr><th>Use</th><th>Cost of missing one</th><th>Usable</th></tr>
 *   <tr><td>Guarding against cache penetration</td><td>One extra database query</td>
 *       <td><b>Yes</b></td></tr>
 *   <tr><td>De-duplicating crawler URLs</td><td>One page fetched twice</td>
 *       <td><b>Yes</b></td></tr>
 *   <tr><td>De-duplicating recommendations</td><td>One item recommended twice</td>
 *       <td><b>Yes</b></td></tr>
 *   <tr><td>De-duplicating order charges</td><td><b>Money debited twice</b></td>
 *       <td><b>No</b></td></tr>
 *   <tr><td>Guaranteeing idempotence</td><td><b>A business operation running twice</b></td>
 *       <td><b>No</b></td></tr>
 * </table>
 *
 * <p>The test is simple: <b>does missing one merely cost a little extra, or does it cause
 * harm?</b> Where it causes harm, use {@code IdempotentRequestCache} or a unique index in the
 * database, which are deterministic.
 *
 * <h2>Choosing the parameters</h2>
 * Two numbers are all that is needed: how many entries are expected
 * ({@code expectedInsertions}), and what false-positive rate is acceptable ({@code fpp}). The
 * bit count and hash count follow from the standard formulas, with nothing to guess at.
 *
 * <p><b>What happens when it overfills</b>: nothing fails, but the false-positive rate climbs
 * quickly -- at twice the expected count, 1% becomes roughly 5%. So err on the high side with
 * the estimate; the bit count grows linearly and is not expensive.
 *
 * @see MultiProcessingBloomFilter the only implementation
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 22/08/2026
 */
public interface ProcessingBloomFilter {

    /**
     * The bit ceiling for one bitmap, matching {@code CacheStore}'s 64MB.
     *
     * <p>Past it, the approach needs rethinking -- shard across several keys, or use another
     * structure entirely.
     */
    long MAX_BITS = 64L * 1024 * 1024 * 8;

    /**
     * Builds one from an expected count and a false-positive rate.
     *
     * @param key                the key in the cache. <b>One key is one filter</b>, so every
     *                           process in the cluster sharing a key name shares the filter
     * @param expectedInsertions how many entries are expected; must be positive
     * @param fpp                the acceptable false-positive rate, in {@code (0, 1)}. 0.01 is
     *                           the usual value
     */
    static ProcessingBloomFilter create(ProcessingCache cache, String key,
                                        long expectedInsertions, double fpp) {
        if (cache == null) {
            throw new IllegalArgumentException("cache must not be null");
        }
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        if (expectedInsertions <= 0) {
            throw new IllegalArgumentException("the expected count must be positive: "
                    + expectedInsertions);
        }
        if (fpp <= 0 || fpp >= 1) {
            throw new IllegalArgumentException("the false-positive rate must lie between 0 and "
                    + "1: " + fpp);
        }
        long bits = optimalNumOfBits(expectedInsertions, fpp);
        if (bits > MAX_BITS) {
            throw new IllegalArgumentException(
                    "these parameters need " + bits + " bits, beyond one bitmap's ceiling of "
                            + MAX_BITS + ". Raise the false-positive rate, or shard the data "
                            + "across several filters");
        }
        return new MultiProcessingBloomFilter(cache, key, bits,
                optimalNumOfHashFunctions(expectedInsertions, bits));
    }

    /**
     * How many bits are needed: {@code m = -n*ln(p) / (ln2)^2}.
     *
     * <p>It is public so that a caller can <b>work out the memory before building one</b>: a
     * hundred million entries at 0.1% is 1.44 billion bits, about 180MB, and that is a number
     * better known before going live than after it has eaten the heap.
     *
     * <p><b>It returns a {@code long}, not an {@code int}.</b> The boundary sits around 150
     * million entries at a 0.1% rate: beyond that the count passes an {@code int}'s 2.14
     * billion, and once it overflows negative every bit offset is negative too. The {@code int}
     * in the reference implementation is a real trap.
     */
    static long optimalNumOfBits(long n, double p) {
        double fpp = p == 0 ? Double.MIN_VALUE : p;
        return (long) (-n * Math.log(fpp) / (Math.log(2) * Math.log(2)));
    }

    /** How many hashes are needed: {@code k = m/n * ln2}, at least 1. */
    static int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    /**
     * Puts a value in.
     *
     * @return {@code true} when this call <b>really changed a bit</b>, meaning the value was
     *         certainly not there before; {@code false} when every bit was already 1, so it
     *         <b>may</b> already be there -- or it may be a false positive
     */
    boolean put(String value);

    /**
     * Asks whether a value is present.
     *
     * @return {@code false} -- <b>certainly not present</b>, unless a partition has lost bits;
     *         see the class javadoc. {@code true} -- possibly present, possibly a false
     *         positive
     */
    boolean mightContain(String value);

    /** Clears the whole filter. */
    void clear();

    /** How many bits this filter occupies. */
    long bitSize();

    /** How many positions each put or query computes. */
    int hashCount();

    /** Its key in the cache. One key name is one filter. */
    String key();
}
