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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Two stores as one: memory in front, somewhere roomier behind.
 *
 * <p><b>A key lives in exactly one of them.</b> Every call finds out which and goes there, so
 * there is no merging to do and no question of which copy is current. A key starts in memory
 * and moves out when memory runs short; see {@code CacheService}'s eviction, which spills
 * rather than deletes once a store is configured here.
 *
 * <p>This class only exists when an external store is configured. Without one, the cache holds
 * its {@link LocalCacheStore} directly and not a call goes through here.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 30/09/2026
 */
public class CompositeCacheStore implements CacheStore {

    private final LocalCacheStore local;
    private final CacheStore external;

    public CompositeCacheStore(LocalCacheStore local, CacheStore external) {
        this.local = local;
        this.external = external;
    }

    /**
     * Which store holds this key.
     *
     * <p>Memory is asked first because it answers without leaving the process, and because a
     * key that is there is the common case. An absent key routes to the external store, which
     * is also where a brand new key would be created, so writes have to be routed separately;
     * see {@link #apply}.
     */
    private CacheStore storeOf(String key) {
        return local.exists(key) ? local : external;
    }

    /**
     * Writes go where the key already is, and a new key is created <b>in memory</b>.
     *
     * <p>Creating it externally instead would defeat the point: the cache would answer from
     * over there for data that has never been anywhere near the size limit.
     *
     * <p>The cost to know: a key <b>absent from memory</b> is asked about outside, which for a
     * networked store is a round trip. So the first write of a new key pays one, and every
     * write after it pays nothing, the key being in memory by then. Writes to keys already
     * moved out pay it as well, which is the price of them still being reachable at all.
     */
    @Override
    public Result apply(CacheOp op, String key, String field, byte[] value, long arg) {
        if (op == CacheOp.CLEAR) {
            // Clear means clear, so both halves go
            Result inMemory = local.apply(op, key, field, value, arg);
            Result outside = external.apply(op, key, field, value, arg);
            return CacheStore.Result.of(inMemory.number() + outside.number());
        }
        if (local.exists(key)) {
            return local.apply(op, key, field, value, arg);
        }
        if (external.exists(key)) {
            return external.apply(op, key, field, value, arg);
        }
        return local.apply(op, key, field, value, arg);
    }

    @Override
    public byte[] get(String key) {
        return storeOf(key).get(key);
    }

    @Override
    public boolean exists(String key) {
        return local.exists(key) || external.exists(key);
    }

    @Override
    public String type(String key) {
        return storeOf(key).type(key);
    }

    @Override
    public int size(String key) {
        return storeOf(key).size(key);
    }

    @Override
    public long ttl(String key) {
        return storeOf(key).ttl(key);
    }

    @Override
    public Set<String> keys(String pattern) {
        Set<String> all = new LinkedHashSet<>(local.keys(pattern));
        all.addAll(external.keys(pattern));
        return all;
    }

    @Override
    public int keyCount() {
        return local.keyCount() + external.keyCount();
    }

    @Override
    public Map<String, String> describe() {
        Map<String, String> out = new LinkedHashMap<>(local.describe());
        external.describe().forEach((k, v) -> out.put("external." + k, v));
        return out;
    }

    @Override
    public boolean getbit(String key, long offset) {
        return storeOf(key).getbit(key, offset);
    }

    @Override
    public CacheStats stats(String key) {
        return storeOf(key).stats(key);
    }

    @Override
    public byte[] hget(String key, String field) {
        return storeOf(key).hget(key, field);
    }

    @Override
    public Map<String, byte[]> hgetAll(String key) {
        return storeOf(key).hgetAll(key);
    }

    @Override
    public List<byte[]> lrange(String key, int start, int stop) {
        return storeOf(key).lrange(key, start, stop);
    }

    @Override
    public Double zscore(String key, byte[] member) {
        return storeOf(key).zscore(key, member);
    }

    @Override
    public List<ScoredMember> zrange(String key, int start, int stop, boolean reverse) {
        return storeOf(key).zrange(key, start, stop, reverse);
    }

    @Override
    public List<ScoredMember> zrangeByScore(String key, double min, double max, boolean reverse,
                                            int offset, int count) {
        return storeOf(key).zrangeByScore(key, min, max, reverse, offset, count);
    }

    @Override
    public int zcount(String key, double min, double max) {
        return storeOf(key).zcount(key, min, max);
    }

    @Override
    public long zrank(String key, byte[] member, boolean reverse) {
        return storeOf(key).zrank(key, member, reverse);
    }

    @Override
    public ScoredMember zpeek(String key, boolean min) {
        return storeOf(key).zpeek(key, min);
    }

    @Override
    public void putStats(String key, CacheStats stats, long ttlMillis) {
        // Routed like any write: where the key already is, or memory when it is new
        if (local.exists(key) || !external.exists(key)) {
            local.putStats(key, stats, ttlMillis);
        } else {
            external.putStats(key, stats, ttlMillis);
        }
    }

    /**
     * Moves one key out of memory and into the external store.
     *
     * <p>Called by eviction instead of deleting: the same decision about which key has to go,
     * but the data survives it. The caller deletes the memory copy afterwards, which is what
     * keeps a key in exactly one place.
     *
     * <p>Everything is rebuilt through the ordinary write operations, so an external store
     * needs nothing beyond {@link CacheStore} to receive a key of any type.
     *
     * @return whether the key was moved; false leaves the caller to evict it as before
     */
    public boolean spill(String key) {
        long ttl = local.ttl(key);
        if (ttl == -2) {
            // Gone between being picked and being moved
            return false;
        }
        if (local.isBitmap(key)) {
            // A bitmap stays in memory. A bit test is one memory read here and a network round
            // trip out there, and a Bloom filter lookup does seven of them, so moving one out
            // would not save memory so much as make it unusable
            return false;
        }
        long expiry = ttl > 0 ? ttl : 0L;
        String kind = local.type(key);
        switch (kind) {
            case "string" -> external.apply(CacheOp.SET, key, "", local.get(key), expiry);
            case "hash" -> {
                local.hgetAll(key).forEach(
                        (field, value) -> external.apply(CacheOp.HSET, key, field, value, 0L));
                expireOutside(key, expiry);
            }
            case "list" -> {
                for (byte[] element : local.lrange(key, 0, -1)) {
                    external.apply(CacheOp.RPUSH, key, "", element, 0L);
                }
                expireOutside(key, expiry);
            }
            case "zset" -> {
                for (ScoredMember member : local.zrange(key, 0, -1, false)) {
                    external.apply(CacheOp.ZADD, key, "", member.member(),
                            Double.doubleToRawLongBits(member.score()));
                }
                expireOutside(key, expiry);
            }
            // Sampling cannot rebuild an aggregate, so it goes across whole
            case "stats" -> external.putStats(key, local.stats(key), expiry);
            default -> {
                return false;
            }
        }
        return true;
    }

    /**
     * The expiry is set last, because the operations that rebuilt a collection each created the
     * key afresh and would have cleared it.
     */
    private void expireOutside(String key, long expiry) {
        if (expiry > 0) {
            external.apply(CacheOp.EXPIRE, key, "", null, expiry);
        }
    }

    /**
     * Whether this key has been moved out, and so lives only in the external store.
     *
     * <p>{@code CacheService} asks before writing: a key out there is changed in place and
     * <b>does not enter the replication stream</b>. Broadcasting it would have every follower
     * rebuild the key in its own memory, which hands back the memory the move just saved and
     * leaves the nodes disagreeing about which side the key is on.
     */
    public boolean isExternal(String key) {
        return !local.exists(key) && external.exists(key);
    }

    /** The memory half, which is the only one that takes part in snapshots and eviction. */
    public LocalCacheStore local() {
        return local;
    }

    /** The external half. */
    public CacheStore external() {
        return external;
    }
}
