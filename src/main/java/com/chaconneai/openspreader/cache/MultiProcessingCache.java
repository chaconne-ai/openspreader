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
 * A thin {@link ProcessingCache}.
 *
 * <p>The real work lives in {@link CacheService} (write routing and replication) and
 * {@link CacheStore} (the local data). This does two things: convert arguments into the
 * protocol's form, with TTLs normalised to milliseconds, and wire reads straight to the
 * local replica.
 *
 * <p><b>Reads never touch the network</b>, deliberately: every process already holds a
 * complete replica, and asking the leader anyway would leave this cache meaning nothing more
 * than "Redis with an extra hop".
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MultiProcessingCache implements ProcessingCache {

    private final CacheService service;

    public MultiProcessingCache(CacheService service) {
        this.service = service;
    }

    // ------------------------------------------------------------------
    // Writes: all routed through the leader
    // ------------------------------------------------------------------

    @Override
    public void set(String key, byte[] data) {
        service.write(CacheOp.SET, requireKey(key), "", data, 0L);
    }

    @Override
    public void set(String key, byte[] data, long ttl, TimeUnit ttlUnit) {
        service.write(CacheOp.SET, requireKey(key), "", data, toMillis(ttl, ttlUnit));
    }

    @Override
    public boolean setIfAbsent(String key, byte[] data, long ttl, TimeUnit ttlUnit) {
        return service.write(CacheOp.SET_IF_ABSENT, requireKey(key), "", data,
                toMillis(ttl, ttlUnit)).flag();
    }

    @Override
    public long incr(String key, long delta) {
        return service.write(CacheOp.INCR, requireKey(key), "", null, delta).number();
    }

    @Override
    public boolean setbit(String key, long offset, boolean value) {
        // The value uses its first byte as the flag, rather than carrying a full payload for one bit
        byte[] flag = { (byte) (value ? 1 : 0) };
        return service.write(CacheOp.SETBIT, requireKey(key), "", flag, offset).flag();
    }

    @Override
    public boolean getbit(String key, long offset) {
        String k = requireKey(key);
        return service.store().getbit(k, offset);
    }

    @Override
    public double max(String key, double sample) {
        return bitsToDouble(CacheOp.MAX, key, sample);
    }

    @Override
    public double min(String key, double sample) {
        return bitsToDouble(CacheOp.MIN, key, sample);
    }

    @Override
    public double sum(String key, double sample) {
        return bitsToDouble(CacheOp.SUM, key, sample);
    }

    /** All three aggregate commands encode their sample into arg by ZADD's convention, and
     *  decode the return value back the same way. */
    private double bitsToDouble(CacheOp op, String key, double sample) {
        long bits = service.write(op, requireKey(key), "", null,
                Double.doubleToRawLongBits(sample)).number();
        return Double.longBitsToDouble(bits);
    }

    @Override
    public CacheStats stats(String key) {
        return service.store().stats(requireKey(key));
    }

    @Override
    public void ltrim(String key, int start, int stop) {
        // Two ints packed into one long: the message format has a single arg field, and
        // adding another just for ltrim would not be worth it
        long packed = ((long) start << 32) | (stop & 0xFFFFFFFFL);
        service.write(CacheOp.LTRIM, requireKey(key), "", null, packed);
    }

    @Override
    public long hincrby(String key, String field, long delta) {
        return service.write(CacheOp.HINCRBY, requireKey(key),
                requireField(field), null, delta).number();
    }

    @Override
    public int lpush(String key, byte[] data) {
        return (int) service.write(CacheOp.LPUSH, requireKey(key), "", data, 0L).number();
    }

    @Override
    public int rpush(String key, byte[] data) {
        return (int) service.write(CacheOp.RPUSH, requireKey(key), "", data, 0L).number();
    }

    @Override
    public byte[] lpop(String key) {
        return service.write(CacheOp.LPOP, requireKey(key), "", null, 0L).bytes();
    }

    @Override
    public byte[] rpop(String key) {
        return service.write(CacheOp.RPOP, requireKey(key), "", null, 0L).bytes();
    }

    @Override
    public boolean hset(String key, String field, byte[] value) {
        return service.write(CacheOp.HSET, requireKey(key), requireField(field), value, 0L).flag();
    }

    @Override
    public boolean hdel(String key, String field) {
        return service.write(CacheOp.HDEL, requireKey(key), requireField(field), null, 0L).flag();
    }

    @Override
    public boolean delete(String key) {
        return service.write(CacheOp.DEL, requireKey(key), "", null, 0L).flag();
    }

    @Override
    public boolean expire(String key, long ttl, TimeUnit ttlUnit) {
        // toMillis cannot be used here: it collapses every non-positive value to 0, meaning
        // "no TTL", whereas a non-positive value to EXPIRE means "delete it now". Collapsing
        // it inverts the meaning
        long ms = ttl <= 0 ? -1L : (ttlUnit == null ? ttl : ttlUnit.toMillis(ttl));
        return service.write(CacheOp.EXPIRE, requireKey(key), "", null, ms).flag();
    }

    @Override
    public boolean persist(String key) {
        return service.write(CacheOp.PERSIST, requireKey(key), "", null, 0L).flag();
    }

    @Override
    public int clear() {
        return (int) service.write(CacheOp.CLEAR, "", "", null, 0L).number();
    }

    // ------------------------------------------------------------------
    // Sorted sets: writes through the leader, reads from the local replica
    // ------------------------------------------------------------------

    @Override
    public boolean zadd(String key, byte[] member, double score) {
        // A ScoredMember is constructed purely to borrow its argument validation for NaN and
        // null; nothing extra is stored
        new ScoredMember(requireMember(member), score);
        return service.write(CacheOp.ZADD, requireKey(key), "", member,
                Double.doubleToRawLongBits(score)).flag();
    }

    @Override
    public boolean zrem(String key, byte[] member) {
        return service.write(CacheOp.ZREM, requireKey(key), "", requireMember(member), 0L).flag();
    }

    @Override
    public double zincrby(String key, byte[] member, double delta) {
        if (Double.isNaN(delta)) {
            throw new ProcessingCacheException("the increment to zincrby must not be NaN");
        }
        long bits = service.write(CacheOp.ZINCRBY, requireKey(key), "", requireMember(member),
                Double.doubleToRawLongBits(delta)).number();
        return Double.longBitsToDouble(bits);
    }

    @Override
    public ScoredMember zpopmin(String key) {
        return popped(service.write(CacheOp.ZPOPMIN, requireKey(key), "", null, 0L));
    }

    @Override
    public ScoredMember zpopmax(String key) {
        return popped(service.write(CacheOp.ZPOPMAX, requireKey(key), "", null, 0L));
    }

    /** A popped result: the member is in bytes, and the score sits in number as bits. */
    private static ScoredMember popped(CacheStore.Result r) {
        return r.bytes() == null ? null
                : new ScoredMember(r.bytes(), Double.longBitsToDouble(r.number()));
    }

    @Override
    public Double zscore(String key, byte[] member) {
        return service.store().zscore(requireKey(key), requireMember(member));
    }

    @Override
    public int zcard(String key) {
        return service.store().size(requireKey(key));
    }

    @Override
    public int zcount(String key, double min, double max) {
        return service.store().zcount(requireKey(key), min, max);
    }

    @Override
    public List<ScoredMember> zrange(String key, int start, int stop) {
        return service.store().zrange(requireKey(key), start, stop, false);
    }

    @Override
    public List<ScoredMember> zrevrange(String key, int start, int stop) {
        return service.store().zrange(requireKey(key), start, stop, true);
    }

    @Override
    public List<ScoredMember> zrangeByScore(String key, double min, double max) {
        return service.store().zrangeByScore(requireKey(key), min, max, false);
    }

    @Override
    public List<ScoredMember> zrevrangeByScore(String key, double min, double max) {
        return service.store().zrangeByScore(requireKey(key), min, max, true);
    }

    @Override
    public long zrank(String key, byte[] member) {
        return service.store().zrank(requireKey(key), requireMember(member), false);
    }

    @Override
    public long zrevrank(String key, byte[] member) {
        return service.store().zrank(requireKey(key), requireMember(member), true);
    }

    // ------------------------------------------------------------------
    // Reads: the local replica
    // ------------------------------------------------------------------

    @Override
    public byte[] get(String key) {
        return service.store().get(requireKey(key));
    }

    @Override
    public byte[] hget(String key, String field) {
        return service.store().hget(requireKey(key), requireField(field));
    }

    @Override
    public boolean hexists(String key, String field) {
        return service.store().hget(requireKey(key), requireField(field)) != null;
    }

    @Override
    public Map<String, byte[]> hgetAll(String key) {
        return service.store().hgetAll(requireKey(key));
    }

    @Override
    public List<byte[]> lrange(String key, int start, int stop) {
        return service.store().lrange(requireKey(key), start, stop);
    }

    @Override
    public int size(String key) {
        return service.store().size(requireKey(key));
    }

    @Override
    public boolean exists(String key) {
        return service.store().exists(requireKey(key));
    }

    @Override
    public String type(String key) {
        return service.store().type(requireKey(key));
    }

    @Override
    public long ttl(String key) {
        return service.store().ttl(requireKey(key));
    }

    @Override
    public Set<String> keys(String pattern) {
        return service.store().keys(pattern);
    }

    @Override
    public int keyCount() {
        return service.store().keyCount();
    }

    // ------------------------------------------------------------------

    /**
     * Validates a key.
     *
     */
    private static String requireKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new ProcessingCacheException("a key must not be empty");
        }
        return key;
    }

    private static byte[] requireMember(byte[] member) {
        if (member == null) {
            throw new ProcessingCacheException("a sorted set member must not be null");
        }
        return member;
    }

    private static String requireField(String field) {
        if (field == null || field.isEmpty()) {
            throw new ProcessingCacheException("a hash field name must not be empty");
        }
        return field;
    }

    /** Normalises to milliseconds. Any non-positive value means "no expiry", matching
     *  {@link CacheOp#SET}'s convention. */
    private static long toMillis(long ttl, TimeUnit unit) {
        if (ttl <= 0) {
            return 0L;
        }
        return unit == null ? ttl : unit.toMillis(ttl);
    }
}
