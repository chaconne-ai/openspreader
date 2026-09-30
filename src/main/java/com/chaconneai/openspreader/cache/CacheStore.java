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

/**
 * Where the data actually sits. {@link LocalCacheStore} is the in-memory one every node
 * carries; another implementation can put it somewhere with more room.
 *
 * <h2>Writes all arrive through {@link #apply}</h2>
 * One entry point for all of {@link CacheOp}, so replaying the replication stream is one call
 * whatever the operation. Everything else here reads.
 *
 * <h2>What is deliberately not here</h2>
 * Snapshots, eviction sampling, byte accounting and access tracking stay on
 * {@link LocalCacheStore}. They exist to keep <b>this process's</b> memory in hand and to
 * bring another node's replica into step, and neither is an external store's concern: it has
 * its own capacity and its own expiry, and it takes no part in node-to-node synchronisation.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 30/09/2026
 */
public interface CacheStore {

    /**
     * Executes one write.
     *
     * <p><b>It must be deterministic</b>: the same input on any node must give the same
     * result, or the replicas fork on replay. So nothing here may use a random number or read
     * the clock to decide anything.
     *
     * @param arg its meaning varies with op: a TTL in milliseconds, or incr's increment
     * @throws ProcessingCacheException on a type mismatch, or when incr's value is not an
     *                                  integer
     */
    Result apply(CacheOp op, String key, String field, byte[] value, long arg);

    byte[] get(String key);

    boolean exists(String key);

    /** The type name, or null when the key is absent. */
    String type(String key);

    /** Element count for a collection, byte length for a string, 0 when absent. */
    int size(String key);

    /** Milliseconds left, -1 with no expiry set, -2 when the key is absent. */
    long ttl(String key);

    /** Keys matching a glob pattern. */
    Set<String> keys(String pattern);

    /** How many keys are held. Across a composed store, the total of its parts. */
    int keyCount();

    /** What this store is, for diagnostics. */
    Map<String, String> describe();

    boolean getbit(String key, long offset);

    CacheStats stats(String key);

    byte[] hget(String key, String field);

    Map<String, byte[]> hgetAll(String key);

    List<byte[]> lrange(String key, int start, int stop);

    Double zscore(String key, byte[] member);

    List<ScoredMember> zrange(String key, int start, int stop, boolean reverse);

    /**
     * Members in a score range, at most {@code count} of them starting at {@code offset}.
     *
     * <p>A range query without a ceiling is how a caller ends up holding a million members it
     * never asked for. Time series work in particular wants a page at a time.
     *
     * @param count how many at most; negative means no limit
     */
    List<ScoredMember> zrangeByScore(String key, double min, double max, boolean reverse,
                                     int offset, int count);

    /** The whole range, however large it turns out to be. */
    default List<ScoredMember> zrangeByScore(String key, double min, double max,
                                             boolean reverse) {
        return zrangeByScore(key, min, max, reverse, 0, -1);
    }

    int zcount(String key, double min, double max);

    long zrank(String key, byte[] member, boolean reverse);

    /** The lowest or highest scoring member, without removing it. */
    ScoredMember zpeek(String key, boolean min);

    /**
     * Writes a whole statistical aggregate, replacing whatever is there.
     *
     * <p>Sampling cannot express this: feeding max, min and sum back in as three samples would
     * leave a count of three. It exists so that a STATS key can be moved between stores
     * unchanged, and nothing on the normal write path calls it.
     *
     * @param ttlMillis milliseconds until expiry, or 0 for none
     */
    void putStats(String key, CacheStats stats, long ttlMillis);

    /**
     * One write's result. Each operation uses whichever of the three fields it needs:
     *
     * <ul>
     *   <li>{@code bytes} -- the element lpop or rpop removed</li>
     *   <li>{@code number} -- the value after incr, the length after a push, the keys cleared
     *       by clear</li>
     *   <li>{@code flag} -- whether setIfAbsent wrote, whether delete really removed
     *       something</li>
     * </ul>
     */
    record Result(byte[] bytes, long number, boolean flag) {

        static final Result NONE = new Result(null, 0L, false);

        static Result of(boolean flag) {
            return new Result(null, 0L, flag);
        }

        static Result of(long number) {
            return new Result(null, number, true);
        }

        static Result of(byte[] bytes) {
            return new Result(bytes, 0L, bytes != null);
        }
    }
}
