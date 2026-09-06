package com.chaconneai.openspreader.aggregation;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

/**
 * The result of one job: the data itself, plus how it was arrived at.
 *
 * <pre>{@code
 * MapReduceResult<String, Integer> r = mapReduce
 *         .<String, String, Integer, Integer>submit("wordCount", text)
 *         .get(60, TimeUnit.SECONDS);
 *
 * // It <b>is</b> a Map; use it directly
 * int the = r.get("the");
 * for (var e : r.entrySet()) { ... }
 *
 * // The statistics hang alongside
 * log.info("{}", r.stats());
 * // MapReduceStats{12 shards / 3 reducers, map ran 12 times emitting 400000 -> 6000 sent
 * //   (98.5% collapsed), reduce ran 2000 times yielding 2000 keys,
 * //   compute 182.3ms (map 175.1 + reduce 7.2), wall clock 75ms, speedup 2.43}
 * }</pre>
 *
 * <h2>Why it is a Map itself, rather than {@code result.data()}</h2>
 * Because the overwhelming majority of calls care only about the data. Designed as a box
 * holding a Map, every call site would have to write {@code .data()} -- and the statistics
 * are looked at <b>occasionally</b>, so they should not tax the common path.
 *
 * <p>Use {@link #data()} when a plain Map has to be handed on, into another API, say.
 *
 * <h2>The statistics are best-effort; the data is not</h2>
 * The numbers in {@link #stats()} come from what each node reports, and in extreme cases an
 * individual field may come out low -- a node's report and its results having travelled
 * separate paths, for instance. <b>The data itself has no such problem</b>: an incomplete
 * result fails the job outright rather than quietly going short.
 *
 * <p>So: tune with {@code stats}, but do not reconcile accounts with it.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
public final class MapReduceResult<K, R> extends AbstractMap<K, R> {

    private final Map<K, R> data;
    private final MapReduceStats stats;

    public MapReduceResult(Map<K, R> data, MapReduceStats stats) {
        this.data = data == null ? Map.of() : data;
        this.stats = stats;
    }

    /** How much work this job did: shard count, map/reduce/combine invocations, and the
     *  time each phase took. */
    public MapReduceStats stats() {
        return stats;
    }

    /** The plain result map, without the statistics. */
    public Map<K, R> data() {
        return data;
    }

    @Override
    public Set<Entry<K, R>> entrySet() {
        return data.entrySet();
    }

    // AbstractMap's default implementations of these walk the entrySet, which is O(n).
    // A result routinely holds tens of thousands of keys, so all of them delegate to the
    // underlying Map's own implementations

    @Override
    public R get(Object key) {
        return data.get(key);
    }

    @Override
    public boolean containsKey(Object key) {
        return data.containsKey(key);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }
}
