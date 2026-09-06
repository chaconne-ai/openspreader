package com.chaconneai.openspreader.cache;

/**
 * Which entry goes first when capacity is reached.
 *
 * <h2>Why it samples rather than being exact</h2>
 * Exact LRU needs a list ordered by access, with <b>every read moving a node to the head</b>
 * -- which needs a global lock, and this cache's reads take no global lock at all (see
 * {@link CacheStore}). Adding one would cut read throughput from tens of millions per second
 * to a few hundred thousand.
 *
 * <p>So it follows Redis's well-trodden path: <b>sample N at random and evict the most
 * deserving of them</b>. The read path is left costing one long field write. The bias
 * sampling introduces does not matter for a cache -- evicting the wrong key occasionally
 * costs one trip to the source, not lost data.
 *
 * <h2>Where the access information comes from</h2>
 * This cache's reads all complete locally on each node, <b>invisible to the leader</b>. So
 * followers report "the keys read this round" to the leader in batches (see
 * {@code spring.spreader.multiprocessing.cache.access-report-interval-ms}）。
 * The reporting is sampled and capped rather than complete, which is ample for a heuristic
 * like eviction.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum EvictionPolicy {

    /**
     * No eviction. The capacity setting becomes decorative and the data only accumulates.
     *
     * <p>Use it only where the total number of keys is inherently bounded -- caching by a
     * business primary key whose count is fixed, for instance.
     */
    NONE,

    /** The least recently accessed goes first. The default. */
    LRU,

    /**
     * The least frequently accessed goes first.
     *
     * <p>More resistant than LRU to a one-off scan flushing the hot data out, but slow to
     * react to a key that was once hot and is no longer used -- its count stays high.
     */
    LFU,

    /**
     * Evicts at random.
     *
     * <p>It looks crude, but where the access distribution is already even its hit rate is
     * much the same as LRU's -- and it maintains no access information whatsoever.
     */
    RANDOM
}
