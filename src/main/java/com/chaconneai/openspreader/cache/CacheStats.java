package com.chaconneai.openspreader.cache;

/**
 * A snapshot of one statistical aggregate.
 *
 * <p>Accumulated by {@link ProcessingCache#max}, {@link ProcessingCache#min} and
 * {@link ProcessingCache#sum}, and read out through {@link ProcessingCache#stats}.
 *
 * <p><b>It holds no raw samples</b>, so it can answer four questions and no more: max, min,
 * sum, mean. Percentiles such as p95 and p99 need distribution information, which is not
 * here -- do not expect them.
 *
 * @param max   the maximum. {@link Double#NaN} with no samples at all, <b>not 0</b>
 * @param min   the minimum; likewise
 * @param sum   the total
 * @param count the sample count, accumulated implicitly by {@link ProcessingCache#sum}
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public record CacheStats(double max, double min, double sum, long count) {

    /** An empty aggregate, with no samples at all. */
    public static final CacheStats EMPTY = new CacheStats(Double.NaN, Double.NaN, 0d, 0L);

    /**
     * The mean.
     *
     * <p>Returns {@link Double#NaN} rather than 0 when {@code count} is 0: "no samples yet"
     * and "the samples average to 0" are different things, and using 0 for the former puts
     * a line along the floor of the dashboard that looks like a healthy service when in
     * fact there is no data at all.
     */
    public double avg() {
        return count == 0 ? Double.NaN : sum / count;
    }

    /** Whether any sample has ever arrived. */
    public boolean isEmpty() {
        return count == 0 && Double.isNaN(max) && Double.isNaN(min);
    }

    @Override
    public String toString() {
        return "CacheStats{max=" + max + ", min=" + min + ", sum=" + sum
                + ", count=" + count + ", avg=" + avg() + '}';
    }
}
