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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A process-level cache: one body of data, with a complete copy in every process in the
 * cluster.
 *
 * <h2>Written on the leader and broadcast incrementally</h2>
 * Every write converges on the leader and executes serially there; the leader numbers each one
 * with an increasing version and broadcasts the operation along with it, and every other node
 * replays them in version order, so the copies agree. What is broadcast is <b>the
 * operation</b> rather than the whole body of data -- that is what makes it incremental.
 *
 * <p>Where this process is the leader, a write executes locally and saves the round trip.
 *
 * <h2>Reads are local</h2>
 * {@link #get}, {@link #exists}, {@link #size} and the other reads <b>consult this process's
 * memory only</b>, sending no network request. They are therefore quick, and may return data a
 * fraction of a millisecond out of date. "Write, then read your own write" works -- the reply
 * to a write carries the operation, so it has been applied locally by the time the reply
 * arrives -- but <b>another process</b> waits for the broadcast.
 *
 * <p>In other words this is an eventually consistent cache, not a database. It suits
 * configuration, dictionaries, counters, rate-limit windows: things where reading a briefly
 * stale value does no harm.
 *
 * <h2>Expiry</h2>
 * TTLs are adjudicated by <b>the leader</b>: it sweeps expired keys periodically and
 * broadcasts each as an ordinary delete. Other nodes do not delete on their own before that
 * arrives, which would fork the state machines, but a read treats an expired key as absent --
 * so the read semantics are exact.
 *
 * <h2>When the leader changes</h2>
 * Every node pulls a full snapshot from the new leader and continues incrementally afterwards.
 * A newly joined node does the same, pulling a full snapshot as the first thing it does. So
 * nodes may be added and removed freely while running.
 *
 * <h2>What it must not be used for</h2>
 * Under a network partition each side may have a leader, each writing its own, and they cannot
 * be merged back. Do not use it for anything needing strong consistency -- deducting stock,
 * for instance.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface ProcessingCache {

    // ------------------------------------------------------------------
    // Strings
    // ------------------------------------------------------------------

    /** Writes without an expiry. An existing key is overwritten and its TTL cleared. */
    void set(String key, byte[] data);

    /** Writes with a lifetime. {@code ttl <= 0} is equivalent to no expiry. */
    void set(String key, byte[] data, long ttl, TimeUnit ttlUnit);

    /**
     * Writes only when the key is absent, as Redis's SETNX does.
     *
     * <p>The decision resolves serially on the leader, so among several processes calling at
     * once, exactly one succeeds.
     *
     * @return whether the write succeeded
     */
    boolean setIfAbsent(String key, byte[] data, long ttl, TimeUnit ttlUnit);

    /** @return null when the key is absent or has expired */
    byte[] get(String key);

    /**
     * Adds atomically, as Redis's INCRBY does. An absent key starts from 0, and any existing
     * TTL is kept.
     *
     * @param delta may be negative
     * @return the value after adding
     * @throws ProcessingCacheException when the key does not hold a decimal integer
     */
    long incr(String key, long delta);

    // ------------------------------------------------------------------
    // Bitmaps
    // ------------------------------------------------------------------

    /**
     * Sets one bit to 0 or 1, matching Redis's {@code SETBIT}.
     *
     * <p>The bit order matches Redis's: within each byte, <b>the most significant bit is bit
     * 0</b>. Underneath it is a byte array that grows on demand, and bits never written are 0.
     *
     * <p>What is replicated is the operation "set bit N", <b>independent of how large the
     * bitmap is</b> -- which is the premise on which a Bloom filter can be built on the cluster
     * cache.
     *
     * <h2>Where the bitmap's final size is known, set the highest bit once first</h2>
     * Growth is <b>exact</b>: each step extends to precisely what is needed and not one byte
     * more, because {@link #get} returns the whole underlying array and its length is part of
     * the public semantics.
     *
     * <p>So writing at increasing offsets triggers a resize and a full copy again and again --
     * measurably jittery tail latency. One {@code setbit(key, maxOffset, ...)} takes the
     * capacity to its final size in a single step, after which every write changes a bit in
     * place.
     *
     * <p>A Bloom filter has that shape naturally -- its bit count is fixed at creation and the
     * first put touches a high bit -- so it needs nothing extra.
     *
     * @param offset the bit offset, from 0. It is a {@code long} because a Bloom filter for 150
     *               million entries at a 0.1% rate already exceeds an {@code int}'s 2.14
     *               billion bits
     * @return the bit's <b>previous</b> value
     * @throws ProcessingCacheException when offset is negative, exceeds one bitmap's ceiling,
     *                                  or the key does not hold a string
     */
    boolean setbit(String key, long offset, boolean value);

    /**
     * Reads one bit, matching Redis's {@code GETBIT}.
     *
     * <p>A local read, not going through the leader. An absent key, or a bit beyond the array,
     * returns false.
     */
    boolean getbit(String key, long offset);

    // ------------------------------------------------------------------
    // Statistical aggregates
    // ------------------------------------------------------------------

    /**
     * Records a sample and updates the maximum.
     *
     * <p>This group ({@link #max}, {@link #min}, {@link #sum}) <b>keeps four numbers and no
     * samples</b>: a million values cost the same memory as one. It is for statistics like
     * "what is this endpoint's slowest and mean latency", without storing every sample.
     *
     * <p>The cost is that <b>no percentile can be computed</b> -- a p99 needs the distribution,
     * and the distribution is what is thrown away here.
     *
     * @return the updated maximum
     */
    double max(String key, double sample);

    /** Records a sample and updates the minimum. See {@link #max} for the details. */
    double min(String key, double sample);

    /**
     * Adds a sample to the running sum, <b>incrementing the count along with it</b>.
     *
     * <p>The count is maintained implicitly, with no command of its own: as two commands,
     * losing either would skew the mean permanently, and nothing in the result would show
     * it.
     *
     * @return the updated sum
     */
    double sum(String key, double sample);

    /**
     * Reads the statistical aggregate on a key. A local read, not going through the leader.
     *
     * @return an empty aggregate when the key is absent, with count and avg both 0
     * @throws ProcessingCacheException when the key does not hold a statistical aggregate
     */
    CacheStats stats(String key);

    // ------------------------------------------------------------------
    // Lists
    // ------------------------------------------------------------------

    /**
     * Inserts at the head, matching Redis's {@code LPUSH}.
     *
     * @return the list's length after the insert, matching Redis's return value
     */
    int lpush(String key, byte[] data);

    /**
     * Keeps only {@code [start, stop]}, matching Redis's {@code LTRIM}.
     *
     * <p>Indexes may be negative, {@code -1} being the last element. <b>Going out of range does
     * not fail but clamps to the boundary</b> -- which is exactly how "keep only the most
     * recent N" works: {@code ltrim(-N, -1)} after an {@code rpush} trims nothing while the
     * list is still shorter than N.
     *
     * <p>Trimming to empty deletes the key along with it, as popping it dry does.
     */
    void ltrim(String key, int start, int stop);

    /**
     * Inserts at the tail, matching {@code RPUSH}.
     *
     * @return the list's length after the insert
     */
    int rpush(String key, byte[] data);

    /** @return the element popped, or null when the list is empty. Emptying the list deletes
     *          the key along with it */
    byte[] lpop(String key);

    byte[] rpop(String key);

    /**
     * Takes a stretch, both ends inclusive. Indexes may be negative, -1 being the last.
     *
     * @return an empty list when the range is out of bounds or the key is absent
     */
    List<byte[]> lrange(String key, int start, int stop);

    // ------------------------------------------------------------------
    // Hashes
    // ------------------------------------------------------------------

    /**
     * Sets a hash field, matching {@code HSET}.
     *
     * @return whether the field is new; overwriting an existing one returns false, matching
     *         Redis's 0
     */
    boolean hset(String key, String field, byte[] value);

    /**
     * Adds atomically to a hash field, matching Redis's {@code HINCRBY}. An absent field starts
     * from 0.
     *
     * @return the value after adding
     * @throws ProcessingCacheException when the field does not hold a decimal integer
     */
    long hincrby(String key, String field, long delta);

    byte[] hget(String key, String field);

    /** Whether the field exists, matching {@code HEXISTS}. */
    boolean hexists(String key, String field);

    boolean hdel(String key, String field);

    /** @return an empty map when the key is absent. What is returned is a copy, and changing
     *          it does not affect the cache */
    Map<String, byte[]> hgetAll(String key);

    // ------------------------------------------------------------------
    // Sorted sets (ZSET), behaving as Redis does
    // ------------------------------------------------------------------

    /**
     * Adds a member or changes its score, matching Redis's {@code ZADD}.
     *
     * <p>An existing member has its <b>score updated</b> rather than a second entry added. The
     * ordering is fixed: score ascending, ties broken by the member's bytes ascending. As in
     * Redis, storage is always ascending and the direction is chosen <b>at query time</b>,
     * through {@link #zrange} or {@link #zrevrange}.
     *
     * @return whether the member is new; changing an existing member's score returns false
     * @throws ProcessingCacheException when the score is NaN, or the key does not hold a sorted
     *                                  set
     */
    boolean zadd(String key, byte[] member, double score);

    /**
     * Removes a member, matching {@code ZREM}. Emptying the set deletes the key along with it,
     * as in Redis.
     *
     * @return true only when the member existed and was removed
     */
    boolean zrem(String key, byte[] member);

    /**
     * Adds an increment to a member's score, matching {@code ZINCRBY}.
     *
     * <p>An absent member starts from 0, which is equivalent to
     * {@code zadd(key, member, delta)}.
     *
     * @return the score after adding
     */
    double zincrby(String key, byte[] member, double delta);

    /**
     * Looks a member's score up, matching {@code ZSCORE}. This step is O(1).
     *
     * @return the score, or null when the key or the member is absent
     */
    Double zscore(String key, byte[] member);

    /** The member count, matching {@code ZCARD}. */
    int zcard(String key);

    /** How many members have a score within {@code [min, max]}, matching {@code ZCOUNT}. */
    int zcount(String key, double min, double max);

    /**
     * Takes a stretch by index, both ends inclusive, indexes possibly negative with -1 the
     * last, matching {@code ZRANGE}.
     *
     * <p><b>This is an O(n) walk</b>: underneath is a red-black tree, which has no span
     * information as Redis's skip list does, so locating by index means counting through one by
     * one. Where the set is large and the range lies far along, use {@link #zrangeByScore}
     * instead, which is O(log n + m).
     */
    List<ScoredMember> zrange(String key, int start, int stop);

    /** As {@link #zrange}, but counting from the high-score end. Matches
     *  {@code ZREVRANGE}. */
    List<ScoredMember> zrevrange(String key, int start, int stop);

    /**
     * Takes a stretch by score, both ends inclusive, ordered by score ascending. Matches
     * {@code ZRANGEBYSCORE}.
     *
     * <p>This is O(log n + m): it locates the start and walks on from there. Use it for a band
     * of scores from a leaderboard.
     */
    List<ScoredMember> zrangeByScore(String key, double min, double max);

    /** As above, ordered by score descending. Matches {@code ZREVRANGEBYSCORE}. */
    List<ScoredMember> zrevrangeByScore(String key, double min, double max);

    /**
     * A member's rank, from 0, matching {@code ZRANK}. This too is O(n).
     *
     * @return the rank, or -1 when the key or the member is absent
     */
    long zrank(String key, byte[] member);

    /** The rank counting from the highest score down. Matches {@code ZREVRANK}. */
    long zrevrank(String key, byte[] member);

    /**
     * Pops the lowest-scoring member, matching {@code ZPOPMIN}.
     *
     * <p>Exactly one caller in the cluster receives any given member -- the decision resolves
     * serially on the leader. This makes it a good priority queue.
     *
     * @return the member popped, or null when the set is empty
     */
    ScoredMember zpopmin(String key);

    /** Pops the highest-scoring member, matching {@code ZPOPMAX}. */
    ScoredMember zpopmax(String key);

    // ------------------------------------------------------------------
    // General
    // ------------------------------------------------------------------

    /** @return true only when the key existed and was deleted */
    boolean delete(String key);

    /**
     * The element count: a list's length, a hash's field count, a string's byte length, and 0
     * for an absent key.
     */
    int size(String key);

    boolean exists(String key);

    /**
     * What kind of value the key holds.
     *
     * <p>Ask before choosing a group of operations -- calling {@link #get} or {@link #lpush} on
     * a key of the wrong kind throws outright, as Redis's WRONGTYPE does.
     *
     * @return {@code none} / {@code string} / {@code list} / {@code hash} / {@code zset}
     */
    String type(String key);

    /**
     * Sets a lifetime on an existing key, matching Redis's {@code EXPIRE}.
     *
     * <p><b>{@code ttl <= 0} deletes the key immediately</b>, as in Redis. To cancel an expiry
     * and let it stay indefinitely, use {@link #persist} -- the two mean opposite things.
     *
     * @return true only when the key existed and was acted on
     */
    boolean expire(String key, long ttl, TimeUnit ttlUnit);

    /**
     * Removes the lifetime so the key no longer expires, matching {@code PERSIST}.
     *
     * @return true only when a TTL was set and has been removed
     */
    boolean persist(String key);

    /**
     * The remaining lifetime, <b>in milliseconds</b> -- matching Redis's {@code PTTL} rather
     * than {@code TTL}, which returns seconds.
     *
     * @return the milliseconds remaining; -2 when the key is absent and -1 when it never
     *         expires, as in Redis
     */
    long ttl(String key);

    /**
     * Lists key names by pattern, understanding only the {@code *} and {@code ?} wildcards.
     *
     * <p>It consults the local copy, and with many keys it is a full scan. Do not put it on a
     * hot path.
     */
    Set<String> keys(String pattern);

    /** Clears the whole cache, across the cluster. */
    int clear();

    /** How many keys the local copy holds. For troubleshooting. */
    int keyCount();
}
