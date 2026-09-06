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
package com.chaconneai.openspreader.cache;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * What a key holds.
 *
 * <p>Five kinds, and as in Redis a key is exactly one of them -- {@code set} on a key that is
 * already a list replaces it wholesale with a string, and {@code lpush} to a string key fails
 * outright. The constraint is not fastidiousness: with types mixed, replaying the same
 * sequence of operations gives different results on different nodes.
 *
 * <h2>Every change goes through a method here</h2>
 * {@code list()} and {@code hash()} are not handed out for the caller to change at will,
 * because {@link #approxBytes()} is maintained incrementally alongside. Recomputing the byte
 * count after each change would make every lpush on a list of a hundred thousand elements
 * O(n), which would be unusable.
 *
 * <p><b>It is not thread-safe.</b> Writes are serialised globally by {@link CacheStore}'s
 * caller, and reads and writes exclude each other through the value object itself.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class CacheValue {

    public enum Kind {
        STRING, LIST, HASH,
        /**
         * A sorted set, behaving as Redis's ZSET does.
         *
         * <p>Members are unique, each carries a score, and they are ordered by score ascending,
         * ties broken by the member's bytes ascending. The direction is chosen <b>at query
         * time</b> ({@code zrange} or {@code zrevrange}); storage is always ascending -- as in
         * Redis, there is no such thing as a direction on the key.
         */
        ZSET,

        /**
         * Statistical aggregation: max, min, sum and count only, with <b>no samples kept</b>.
         *
         * <p>This is its fundamental difference from ZSET: a ZSET stores every member and its
         * memory grows linearly with the sample count, whereas this stays the size of four
         * numbers however many are fed in. It is for statistics like "what is this endpoint's
         * maximum, minimum and mean latency", where millions of samples still cost no memory.
         *
         * <p>The cost is that <b>it cannot compute a percentile</b> -- a p99 needs the
         * distribution, and the distribution is exactly what is thrown away. A real p99 needs
         * another structure, a TDigest or the like; do not expect it here.
         */
        STATS
    }

    /** The JVM overhead of one list element, hash field or set member -- object header,
     *  reference, array header -- to an order of magnitude. */
    private static final int ENTRY_OVERHEAD = 48;

    /** The size of one statistical aggregate: three doubles, a long, and the object
     *  header. */
    private static final int STATS_BYTES = 3 * Double.BYTES + Long.BYTES + 16;

    private final Kind kind;

    private byte[] str;

    /** The aggregate's four numbers, meaningful only under {@link Kind#STATS}. */
    private double statMax;
    private double statMin;
    private double statSum;
    private long statCount;

    private Deque<byte[]> list;
    private Map<String, byte[]> hash;

    /**
     * The sorted set as member to score.
     *
     * <p>It and {@link #sorted} are <b>two indexes over the same data</b>, exactly as Redis
     * does it with a dict plus a skip list. Both are needed, because each does something the
     * other cannot:
     * <ul>
     *   <li>Without this map, {@code zscore} would scan the whole ordered structure -- that is
     *       ordered by (score, member), and a member alone locates nothing</li>
     *   <li>Without the ordered structure there is no order, and a range query would have to
     *       sort afresh every time</li>
     * </ul>
     */
    private Map<ByteKey, Double> scores;

    /**
     * The sorted set's ordering.
     *
     * <p>A {@link TreeSet} -- a red-black tree -- rather than Redis's skip list: insertion,
     * deletion and location are all O(log n), and a range walk goes straight along.
     * <b>Locating by index is where they differ</b>: a skip list carries a span at each level
     * and sums them to find a rank in O(log n), while a red-black tree has no such information,
     * so a range by index such as {@code zrange(10000, 10100)} is O(n) here. A range by score
     * is unaffected and remains O(log n + m).
     */
    private NavigableSet<ScoredMember> sorted;

    /** The absolute expiry instant, on this machine's {@code System.currentTimeMillis()}; 0
     *  means it does not expire. */
    private long expireAt;

    /** The approximate bytes the content occupies, maintained incrementally. */
    private long approxBytes;

    /**
     * When it was last accessed.
     *
     * <p>Not volatile: this is a heuristic for eviction, and reading a slightly stale value
     * only delays one key's removal by a round. A memory barrier for it would cost something on
     * every read, which is not a good bargain.
     */
    private long lastAccessAt;

    /** The nanosecond timestamp LRU compares on; see {@link #accessOrder()}. */
    private long accessOrder;

    /** How many times it has been accessed, for LFU. Approximate as well. */
    private long hits;

    private CacheValue(Kind kind) {
        this.kind = kind;
        this.lastAccessAt = System.currentTimeMillis();
        this.accessOrder = System.nanoTime();
    }

    static CacheValue ofString(byte[] data) {
        CacheValue v = new CacheValue(Kind.STRING);
        v.str = data == null ? new byte[0] : data;
        v.approxBytes = v.str.length;
        return v;
    }

    static CacheValue ofStats() {
        CacheValue v = new CacheValue(Kind.STATS);
        // An empty aggregate must start at the opposite infinity, or the first sample can
        // never beat the initial value. Starting from 0 is the commonest mistake in code like
        // this: fed nothing but negative numbers, max would sit at 0 for ever
        v.statMax = Double.NEGATIVE_INFINITY;
        v.statMin = Double.POSITIVE_INFINITY;
        v.approxBytes = STATS_BYTES;
        return v;
    }

    static CacheValue ofList() {
        CacheValue v = new CacheValue(Kind.LIST);
        v.list = new ArrayDeque<>();
        return v;
    }

    static CacheValue ofHash() {
        CacheValue v = new CacheValue(Kind.HASH);
        // LinkedHashMap rather than HashMap: hgetAll's field order must agree across nodes, or
        // the same data prints differently in different processes and troubleshooting takes it
        // for a forked replica
        v.hash = new LinkedHashMap<>();
        return v;
    }

    static CacheValue ofZSet() {
        CacheValue v = new CacheValue(Kind.ZSET);
        // This map only looks a score up by member and is never walked, so the order does not
        // matter and a HashMap will do; anything needing order goes through sorted, whose order
        // is fixed by the comparator and agrees across nodes
        v.scores = new HashMap<>();
        v.sorted = new TreeSet<>(ScoredMember.ORDER);
        return v;
    }

    public Kind kind() {
        return kind;
    }

    public long approxBytes() {
        return approxBytes;
    }

    public long lastAccessAt() {
        return lastAccessAt;
    }

    public long hits() {
        return hits;
    }

    /** Records an access, for eviction. On the read path it is a few ordinary field writes,
     *  at negligible cost. */
    void touch(long now) {
        lastAccessAt = now;
        accessOrder = System.nanoTime();
        hits++;
    }

    /**
     * The timestamp LRU compares on, in <b>nanoseconds</b>.
     *
     * <h2>Why {@link #lastAccessAt} will not do</h2>
     * It is in milliseconds. Tens of thousands of keys can be written within one millisecond,
     * and their timestamps are then <b>exactly equal</b>, so {@code <} never holds -- LRU
     * degenerates into "take the first from the sample pool", which may be the key just
     * written.
     *
     * <p>The symptom is "write it, read it back at once, and it is gone", appearing only under
     * write-heavy load and behaving perfectly the rest of the time.
     *
     * <h2>The nanosecond clock is not wall time, and need not be</h2>
     * {@link System#nanoTime()} guarantees only that it is <b>monotonic within this
     * process</b>, and is not comparable between machines. Eviction is a purely local decision
     * -- each node decides which keys to keep -- so nothing has to be compared across
     * nodes.
     */
    long accessOrder() {
        return accessOrder;
    }

    byte[] str() {
        return str;
    }

    void str(byte[] data) {
        byte[] next = data == null ? new byte[0] : data;
        approxBytes += next.length - str.length;
        this.str = next;
    }

    /** Records a sample against max and returns the updated max. */
    double statMax(double sample) {
        statMax = Math.max(statMax, sample);
        return statMax;
    }

    /** Records a sample against min and returns the updated min. */
    double statMin(double sample) {
        statMin = Math.min(statMin, sample);
        return statMin;
    }

    /**
     * Records a sample against sum, <b>incrementing count along with it</b>, and returns the
     * updated sum.
     *
     * <p>The count is maintained implicitly, with no second command for the caller to send --
     * losing or failing either of two commands would skew the mean permanently, and invisibly.
     * Bound together, that window does not exist.
     */
    double statSum(double sample) {
        statSum += sample;
        statCount++;
        return statSum;
    }

    double statMaxValue() {
        return statMax;
    }

    double statMinValue() {
        return statMin;
    }

    double statSumValue() {
        return statSum;
    }

    long statCountValue() {
        return statCount;
    }

    void restoreStats(double max, double min, double sum, long count) {
        this.statMax = max;
        this.statMin = min;
        this.statSum = sum;
        this.statCount = count;
    }

    /**
     * Keeps only {@code [start, stop]}, trimming both ends, as Redis's LTRIM does.
     *
     * <p>Indexes may be negative, with {@code -1} the last element. Going out of range does not
     * fail but <b>clamps to the boundary</b>: {@code ltrim(0, 999)} on a list of ten elements
     * is legitimate and trims nothing. That matters, because it is exactly how "keep only the
     * most recent N" is written, and the list is short until it has gathered N.
     *
     * <p>{@code start > stop}, or a range entirely outside the list, empties it -- after which
     * the caller deletes the key; see {@link #isEmpty()}.
     */
    void ltrim(int start, int stop) {
        int n = list.size();
        int from = start < 0 ? n + start : start;
        int to = stop < 0 ? n + stop : stop;
        if (from < 0) {
            from = 0;
        }
        if (to >= n) {
            to = n - 1;
        }
        if (from > to || from >= n) {
            list.clear();
            return;
        }
        // Popped from both ends, without rebuilding the whole Deque
        for (int i = 0; i < from; i++) {
            list.pollFirst();
        }
        for (int i = to; i < n - 1; i++) {
            list.pollLast();
        }
    }

    /** For reading only; changes go through the methods above, or the byte count goes
     *  wrong. */
    Deque<byte[]> list() {
        return list;
    }

    Map<String, byte[]> hash() {
        return hash;
    }

    NavigableSet<ScoredMember> sorted() {
        return sorted;
    }

    void listAdd(byte[] item, boolean first) {
        if (first) {
            list.addFirst(item);
        } else {
            list.addLast(item);
        }
        approxBytes += lengthOf(item) + ENTRY_OVERHEAD;
    }

    byte[] listPoll(boolean first) {
        byte[] b = first ? list.pollFirst() : list.pollLast();
        if (b != null) {
            approxBytes -= lengthOf(b) + ENTRY_OVERHEAD;
        }
        return b;
    }

    /** @return whether the field is new */
    boolean hashPut(String field, byte[] value) {
        byte[] old = hash.put(field, value);
        approxBytes += lengthOf(value) - lengthOf(old);
        if (old == null) {
            approxBytes += field.length() * 2L + ENTRY_OVERHEAD;
            return true;
        }
        return false;
    }

    boolean hashRemove(String field) {
        byte[] old = hash.remove(field);
        if (old == null) {
            return false;
        }
        approxBytes -= lengthOf(old) + field.length() * 2L + ENTRY_OVERHEAD;
        return true;
    }

    // ------------------------------------------------------------------
    // Sorted sets
    // ------------------------------------------------------------------

    /**
     * Adds a member, or changes its score.
     *
     * <p>An existing member means <b>changing the score</b>: the old entry must come out of the
     * ordered structure before the new one goes in, because its position in the tree follows
     * the <b>old</b> score, and adding the new one directly would leave a ghost entry in the
     * wrong place that can never be removed.
     *
     * @return whether it was added; changing an existing member's score returns false, matching
     *         Redis's ZADD
     */
    boolean zadd(byte[] member, double score) {
        if (Double.isNaN(score)) {
            // Caught before anything is touched: NaN compares false against every number, and
            // an entry placed in the ordered structure could never be located or removed again
            throw new ProcessingCacheException("a sorted set's score must not be NaN");
        }
        ByteKey key = new ByteKey(member);
        Double old = scores.put(key, score);
        if (old != null) {
            sorted.remove(new ScoredMember(member, old));
            sorted.add(new ScoredMember(member, score));
            return false;
        }
        sorted.add(new ScoredMember(member, score));
        // One member is referenced once in each structure, so the overhead counts twice
        approxBytes += lengthOf(member) + 2L * ENTRY_OVERHEAD + Double.BYTES;
        return true;
    }

    boolean zrem(byte[] member) {
        Double old = scores.remove(new ByteKey(member));
        if (old == null) {
            return false;
        }
        sorted.remove(new ScoredMember(member, old));
        approxBytes -= lengthOf(member) + 2L * ENTRY_OVERHEAD + Double.BYTES;
        return true;
    }

    /** @return the member's score, or null when it is absent */
    Double zscore(byte[] member) {
        return scores.get(new ByteKey(member));
    }

    /**
     * Pops the first or the last member.
     *
     * <p>Storage is always ascending, so first is the lowest score and last the highest --
     * matching Redis's {@code ZPOPMIN} and {@code ZPOPMAX}.
     */
    ScoredMember zpoll(boolean first) {
        ScoredMember m = first ? sorted.pollFirst() : sorted.pollLast();
        if (m != null) {
            scores.remove(new ByteKey(m.member()));
            approxBytes -= lengthOf(m.member()) + 2L * ENTRY_OVERHEAD + Double.BYTES;
        }
        return m;
    }

    /** Looks without removing. The leader uses it to decide which member to pop, then
     *  broadcasts the removal of that one. */
    ScoredMember zpeek(boolean first) {
        return sorted.isEmpty() ? null : (first ? sorted.first() : sorted.last());
    }

    /** Used when loading from a snapshot: the content is already in place, and this computes
     *  the byte count once. */
    void recomputeBytes() {
        approxBytes = switch (kind) {
            case STRING -> str.length;
            case LIST -> {
                long n = 0;
                for (byte[] b : list) {
                    n += lengthOf(b) + ENTRY_OVERHEAD;
                }
                yield n;
            }
            case HASH -> {
                long n = 0;
                for (Map.Entry<String, byte[]> e : hash.entrySet()) {
                    n += lengthOf(e.getValue()) + e.getKey().length() * 2L + ENTRY_OVERHEAD;
                }
                yield n;
            }
            case ZSET -> {
                long n = 0;
                for (ScoredMember m : sorted) {
                    n += lengthOf(m.member()) + 2L * ENTRY_OVERHEAD + Double.BYTES;
                }
                yield n;
            }
            // Fixed in size, however many samples are fed in
            case STATS -> STATS_BYTES;
        };
    }

    public long expireAt() {
        return expireAt;
    }

    void expireAt(long at) {
        this.expireAt = at;
    }

    boolean expired(long now) {
        return expireAt > 0 && now >= expireAt;
    }

    /** The element count: list length, field count, member count, or string byte length. */
    int size() {
        return switch (kind) {
            case STRING -> str.length;
            case LIST -> list.size();
            case HASH -> hash.size();
            case ZSET -> sorted.size();
            // How many samples there are, that is how many times sum has accumulated
            case STATS -> (int) Math.min(Integer.MAX_VALUE, statCount);
        };
    }

    /** Whether the content is empty -- a list popped dry, a hash with every field removed, a
     *  set with every member removed -- in which case the key goes with it, as in Redis. */
    boolean isEmpty() {
        // STATS is excluded: a count of 0 does not mean the key should go -- calling max and
        // min without sum leaves count at 0 while max and min hold values
        return kind != Kind.STRING && kind != Kind.STATS && size() == 0;
    }

    /** Requires a particular kind, and fails otherwise. */
    void require(Kind expected, String key, String operation) {
        if (kind != expected) {
            throw new ProcessingCacheException(
                    "key " + key + " holds a " + kind + ", so " + operation
                            + " cannot be performed on it");
        }
    }

    private static int lengthOf(byte[] b) {
        return b == null ? 0 : b.length;
    }

    /**
     * A wrapper so a byte[] can serve as a map key.
     *
     * <p>A byte[]'s own equals and hashCode compare <b>by reference</b>, so using one directly
     * as a key makes two arrays with the same contents two different keys -- the kind of
     * mistake that never turns up in a search.
     */
    private record ByteKey(byte[] bytes) {

        @Override
        public boolean equals(Object o) {
            return o instanceof ByteKey other && Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }
}
