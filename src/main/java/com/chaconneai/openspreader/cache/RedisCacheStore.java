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

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.SetCondition;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Locale;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A {@link CacheStore} on Redis, for keys memory has no room for.
 *
 * <p>Every operation maps to its native Redis command, so a hash stays a hash over there and a
 * sorted set a sorted set. Nothing is serialised into an opaque blob, which is what keeps a
 * field update a field update rather than a read, change and write of the whole value.
 *
 * <h2>Everything is namespaced</h2>
 * Keys carry a prefix, {@code spreader:cache:} by default. Redis is usually shared with the
 * rest of an application, and without one {@link #keys}, {@link #keyCount} and a
 * {@link CacheOp#CLEAR} would reach data that is none of this cache's business. For the same
 * reason CLEAR scans its own prefix rather than calling {@code FLUSHDB}.
 *
 * <h2>Aggregates are a hash with a marker</h2>
 * Redis has no equivalent of {@link CacheValue.Kind#STATS}, so one is kept as a hash of four
 * numbers under {@link #STAT_KIND}, which is also what tells {@link #type} a hash of ours from
 * a hash of yours.
 *
 * <h2>Read, change, write is safe here</h2>
 * {@code MAX}, {@code MIN} and {@code SUM} read the aggregate, change it and write it back
 * without a transaction. That is sound because <b>only the leader ever writes an external
 * store</b>, and its writes are serialised by the cache's own state lock, so no second writer
 * exists to race with.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 30/09/2026
 */
public class RedisCacheStore implements CacheStore {

    /** Marks a hash as one of ours holding an aggregate. */
    private static final byte[] STAT_KIND = utf8("__kind");

    private static final byte[] STAT_MAX = utf8("__max");
    private static final byte[] STAT_MIN = utf8("__min");
    private static final byte[] STAT_SUM = utf8("__sum");
    private static final byte[] STAT_COUNT = utf8("__count");
    private static final byte[] STAT_MARKER = utf8("stats");

    public static final String DEFAULT_KEY_PREFIX = "spreader:cache:";

    private final RedisConnectionFactory factory;
    private final String keyPrefix;
    private final byte[] prefixBytes;

    public RedisCacheStore(RedisConnectionFactory factory) {
        this(factory, DEFAULT_KEY_PREFIX);
    }

    public RedisCacheStore(RedisConnectionFactory factory, String keyPrefix) {
        this.factory = factory;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
        this.prefixBytes = utf8(this.keyPrefix);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] k(String key) {
        return utf8(keyPrefix + key);
    }

    private String unprefixed(byte[] raw) {
        String full = new String(raw, StandardCharsets.UTF_8);
        return full.startsWith(keyPrefix) ? full.substring(keyPrefix.length()) : full;
    }

    private <T> T call(Function<RedisConnection, T> action) {
        try (RedisConnection conn = factory.getConnection()) {
            return action.apply(conn);
        }
    }

    // ------------------------------------------------------------------
    // Writes
    // ------------------------------------------------------------------

    @Override
    public Result apply(CacheOp op, String key, String field, byte[] value, long arg) {
        return call(conn -> write(conn, op, key, field, value, arg));
    }

    private Result write(RedisConnection conn, CacheOp op, String key, String field,
                         byte[] value, long arg) {
        byte[] k = k(key);
        return switch (op) {
            case SET -> {
                conn.set(k, value == null ? new byte[0] : value);
                if (arg > 0) {
                    conn.pExpire(k, arg);
                }
                yield Result.of(true);
            }
            case SET_IF_ABSENT -> {
                Boolean written = conn.set(k, value == null ? new byte[0] : value,
                        SetCondition.ifAbsent(),
                        arg > 0 ? Expiration.milliseconds(arg) : Expiration.persistent());
                yield Result.of(Boolean.TRUE.equals(written));
            }
            case DEL -> Result.of(count(conn.del(k)) > 0);
            case EXPIRE -> {
                // A non-positive lifetime deletes, which is what the local store does
                if (arg <= 0) {
                    yield Result.of(count(conn.del(k)) > 0);
                }
                yield Result.of(Boolean.TRUE.equals(conn.pExpire(k, arg)));
            }
            case PERSIST -> Result.of(Boolean.TRUE.equals(conn.persist(k)));
            case INCR -> Result.of(count(conn.incrBy(k, arg)));
            case SETBIT -> {
                boolean on = value != null && value.length > 0 && value[0] != 0;
                Boolean was = conn.setBit(k, arg, on);
                yield Result.of(Boolean.TRUE.equals(was));
            }
            case LPUSH -> Result.of(count(conn.lPush(k, value)));
            case RPUSH -> Result.of(count(conn.rPush(k, value)));
            case LPOP -> {
                byte[] popped = conn.lPop(k);
                yield popped == null ? Result.NONE : Result.of(popped);
            }
            case RPOP -> {
                byte[] popped = conn.rPop(k);
                yield popped == null ? Result.NONE : Result.of(popped);
            }
            case LTRIM -> {
                // Two ints packed into one long, exactly as the local store unpacks them
                if (!Boolean.TRUE.equals(conn.exists(k))) {
                    yield Result.of(false);
                }
                conn.lTrim(k, (int) (arg >> 32), (int) arg);
                yield Result.of(true);
            }
            case HSET -> Result.of(Boolean.TRUE.equals(conn.hSet(k, utf8(field), value)));
            case HDEL -> Result.of(count(conn.hDel(k, utf8(field))) > 0);
            case HINCRBY -> Result.of(count(conn.hIncrBy(k, utf8(field), arg)));
            case ZADD -> {
                double score = Double.longBitsToDouble(arg);
                Boolean added = conn.zAdd(k, score, value, RedisZSetCommands.ZAddArgs.empty());
                yield Result.of(Boolean.TRUE.equals(added));
            }
            case ZREM -> Result.of(count(conn.zRem(k, value)) > 0);
            case ZREMRANGEBYSCORE -> {
                ByteBuffer bounds = ByteBuffer.wrap(value);
                double low = bounds.getDouble();
                yield Result.of(count(conn.zRemRangeByScore(k, low, bounds.getDouble())));
            }
            case ZINCRBY -> {
                Double updated = conn.zIncrBy(k, Double.longBitsToDouble(arg), value);
                yield Result.of(Double.doubleToRawLongBits(updated == null ? 0d : updated));
            }
            case ZPOPMIN -> pop(conn.zPopMin(k));
            case ZPOPMAX -> pop(conn.zPopMax(k));
            case MAX -> aggregate(conn, k, STAT_MAX, Double.longBitsToDouble(arg), false);
            case MIN -> aggregate(conn, k, STAT_MIN, Double.longBitsToDouble(arg), false);
            case SUM -> aggregate(conn, k, STAT_SUM, Double.longBitsToDouble(arg), true);
            case CLEAR -> Result.of(clearNamespace(conn));
        };
    }

    private static Result pop(Tuple tuple) {
        if (tuple == null) {
            return Result.NONE;
        }
        double score = tuple.getScore() == null ? 0d : tuple.getScore();
        return new Result(tuple.getValue(), Double.doubleToRawLongBits(score), true);
    }

    /**
     * One sample into an aggregate. Max and min keep the extreme and leave the count alone;
     * only sum advances it, which is exactly what the local store does.
     */
    private Result aggregate(RedisConnection conn, byte[] k, byte[] field,
                             double sample, boolean counts) {
        conn.hSet(k, STAT_KIND, STAT_MARKER);
        double current = readDouble(conn, k, field,
                field == STAT_MAX ? Double.NEGATIVE_INFINITY
                        : field == STAT_MIN ? Double.POSITIVE_INFINITY : 0d);
        double updated;
        if (field == STAT_MAX) {
            updated = Math.max(current, sample);
        } else if (field == STAT_MIN) {
            updated = Math.min(current, sample);
        } else {
            updated = current + sample;
        }
        conn.hSet(k, field, utf8(Double.toString(updated)));
        if (counts) {
            conn.hIncrBy(k, STAT_COUNT, 1L);
        }
        return Result.of(Double.doubleToRawLongBits(updated));
    }

    @Override
    public void putStats(String key, CacheStats stats, long ttlMillis) {
        byte[] k = k(key);
        call(conn -> {
            conn.del(k);
            conn.hSet(k, STAT_KIND, STAT_MARKER);
            // NaN is what an untouched aggregate publishes; the infinities are what it holds
            conn.hSet(k, STAT_MAX, utf8(Double.toString(
                    Double.isNaN(stats.max()) ? Double.NEGATIVE_INFINITY : stats.max())));
            conn.hSet(k, STAT_MIN, utf8(Double.toString(
                    Double.isNaN(stats.min()) ? Double.POSITIVE_INFINITY : stats.min())));
            conn.hSet(k, STAT_SUM, utf8(Double.toString(stats.sum())));
            conn.hSet(k, STAT_COUNT, utf8(Long.toString(stats.count())));
            if (ttlMillis > 0) {
                conn.pExpire(k, ttlMillis);
            }
            return null;
        });
    }

    /** Deletes only what carries this store's prefix, never the whole database. */
    private long clearNamespace(RedisConnection conn) {
        List<byte[]> doomed = scanKeys(conn, "*");
        if (doomed.isEmpty()) {
            return 0L;
        }
        return count(conn.del(doomed.toArray(new byte[0][])));
    }

    // ------------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------------

    @Override
    public byte[] get(String key) {
        return call(conn -> conn.get(k(key)));
    }

    @Override
    public boolean exists(String key) {
        return call(conn -> Boolean.TRUE.equals(conn.exists(k(key))));
    }

    @Override
    public String type(String key) {
        return call(conn -> {
            byte[] k = k(key);
            DataType type = conn.type(k);
            if (type == null || type == DataType.NONE) {
                return "none";
            }
            return switch (type) {
                case STRING -> "string";
                case LIST -> "list";
                case ZSET -> "zset";
                // Ours or the application's: the marker is what separates them
                case HASH -> conn.hGet(k, STAT_KIND) != null ? "stats" : "hash";
                default -> "none";
            };
        });
    }

    @Override
    public int size(String key) {
        return call(conn -> {
            byte[] k = k(key);
            DataType type = conn.type(k);
            if (type == null) {
                return 0;
            }
            return (int) switch (type) {
                case STRING -> count(conn.strLen(k));
                case LIST -> count(conn.lLen(k));
                case HASH -> count(conn.hLen(k));
                case ZSET -> count(conn.zCard(k));
                default -> 0L;
            };
        });
    }

    @Override
    public long ttl(String key) {
        return call(conn -> {
            Long remaining = conn.pTtl(k(key));
            // Redis answers -1 with no expiry and -2 when absent, which is this contract too
            return remaining == null ? -2L : remaining;
        });
    }

    @Override
    public Set<String> keys(String pattern) {
        return call(conn -> {
            Set<String> out = new LinkedHashSet<>();
            for (byte[] raw : scanKeys(conn, pattern)) {
                out.add(unprefixed(raw));
            }
            return out;
        });
    }

    /**
     * {@code SCAN} rather than {@code KEYS}: the latter walks the whole keyspace in one go and
     * blocks every other client while it does, which on a shared Redis is an outage.
     */
    private List<byte[]> scanKeys(RedisConnection conn, String pattern) {
        List<byte[]> out = new ArrayList<>();
        ScanOptions options = ScanOptions.scanOptions()
                .match(keyPrefix + (pattern == null || pattern.isBlank() ? "*" : pattern))
                .count(500)
                .build();
        try (Cursor<byte[]> cursor = conn.scan(options)) {
            while (cursor.hasNext()) {
                out.add(cursor.next());
            }
        }
        return out;
    }

    @Override
    public int keyCount() {
        // O(n) over this prefix: Redis has no count for a subset of the keyspace, and DBSIZE
        // would include everything the application keeps beside this cache
        return call(conn -> scanKeys(conn, "*").size());
    }

    @Override
    public Map<String, String> describe() {
        Map<String, String> out = new LinkedHashMap<>();
        out.put("type", "redis");
        out.put("keyPrefix", keyPrefix);
        out.put("factory", factory.getClass().getSimpleName());
        return out;
    }

    @Override
    public boolean getbit(String key, long offset) {
        return call(conn -> Boolean.TRUE.equals(conn.getBit(k(key), offset)));
    }

    @Override
    public CacheStats stats(String key) {
        return call(conn -> {
            byte[] k = k(key);
            if (conn.hGet(k, STAT_KIND) == null) {
                DataType type = conn.type(k);
                if (type != null && type != DataType.NONE) {
                    throw new ProcessingCacheException("key " + key + " holds a "
                            + type.code().toUpperCase(Locale.ROOT)
                            + ", so stats cannot be performed on it");
                }
                return CacheStats.EMPTY;
            }
            long n = (long) readDouble(conn, k, STAT_COUNT, 0d);
            double max = readDouble(conn, k, STAT_MAX, Double.NEGATIVE_INFINITY);
            double min = readDouble(conn, k, STAT_MIN, Double.POSITIVE_INFINITY);
            // An aggregate with no samples publishes NaN rather than an infinity, or a chart
            // draws a line at the edge of the universe
            return new CacheStats(
                    n == 0 && Double.isInfinite(max) ? Double.NaN : max,
                    n == 0 && Double.isInfinite(min) ? Double.NaN : min,
                    readDouble(conn, k, STAT_SUM, 0d), n);
        });
    }

    private static double readDouble(RedisConnection conn, byte[] k, byte[] field,
                                     double fallback) {
        byte[] raw = conn.hGet(k, field);
        if (raw == null) {
            return fallback;
        }
        try {
            return Double.parseDouble(new String(raw, StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    @Override
    public byte[] hget(String key, String field) {
        return call(conn -> {
            byte[] k = k(key);
            rejectAggregate(conn, k, key, "hget");
            return conn.hGet(k, utf8(field));
        });
    }

    @Override
    public Map<String, byte[]> hgetAll(String key) {
        return call(conn -> {
            byte[] k = k(key);
            rejectAggregate(conn, k, key, "hgetAll");
            Map<byte[], byte[]> raw = conn.hGetAll(k);
            Map<String, byte[]> out = new LinkedHashMap<>();
            if (raw != null) {
                raw.forEach((f, v) -> out.put(new String(f, StandardCharsets.UTF_8), v));
            }
            return out;
        });
    }

    /**
     * An aggregate is a hash here, so the hash operations would happily read its four numbers
     * and hand back {@code __max} as though it were a field somebody stored. The local store
     * refuses by type, and so does this.
     */
    private void rejectAggregate(RedisConnection conn, byte[] k, String key, String operation) {
        if (conn.hGet(k, STAT_KIND) != null) {
            throw new ProcessingCacheException("key " + key + " holds a STATS, so " + operation
                    + " cannot be performed on it");
        }
    }

    @Override
    public List<byte[]> lrange(String key, int start, int stop) {
        return call(conn -> {
            List<byte[]> out = conn.lRange(k(key), start, stop);
            return out == null ? List.of() : out;
        });
    }

    @Override
    public Double zscore(String key, byte[] member) {
        return call(conn -> conn.zScore(k(key), member));
    }

    @Override
    public List<ScoredMember> zrange(String key, int start, int stop, boolean reverse) {
        return call(conn -> {
            byte[] k = k(key);
            Set<Tuple> tuples = reverse
                    ? conn.zRevRangeWithScores(k, start, stop)
                    : conn.zRangeWithScores(k, start, stop);
            return members(tuples);
        });
    }

    @Override
    public List<ScoredMember> zrangeByScore(String key, double min, double max,
                                            boolean reverse, int offset, int count) {
        return call(conn -> {
            byte[] k = k(key);
            Range<Double> range = Range.closed(min, max);
            // Redis applies the limit as it scans, so a page costs the page rather than the
            // whole range
            Limit limit = offset <= 0 && count < 0
                    ? Limit.unlimited()
                    : Limit.limit().offset(Math.max(offset, 0)).count(count);
            Set<Tuple> tuples = reverse
                    ? conn.zRevRangeByScoreWithScores(k, range, limit)
                    : conn.zRangeByScoreWithScores(k, range, limit);
            return members(tuples);
        });
    }

    private static List<ScoredMember> members(Set<Tuple> tuples) {
        if (tuples == null) {
            return List.of();
        }
        List<ScoredMember> out = new ArrayList<>(tuples.size());
        for (Tuple t : tuples) {
            out.add(new ScoredMember(t.getValue(), t.getScore() == null ? 0d : t.getScore()));
        }
        return out;
    }

    @Override
    public int zcount(String key, double min, double max) {
        return call(conn -> (int) count(conn.zCount(k(key), min, max)));
    }

    @Override
    public long zrank(String key, byte[] member, boolean reverse) {
        return call(conn -> {
            byte[] k = k(key);
            Long rank = reverse ? conn.zRevRank(k, member) : conn.zRank(k, member);
            return rank == null ? -1L : rank;
        });
    }

    @Override
    public ScoredMember zpeek(String key, boolean min) {
        return call(conn -> {
            byte[] k = k(key);
            Set<Tuple> tuples = min
                    ? conn.zRangeWithScores(k, 0, 0)
                    : conn.zRevRangeWithScores(k, 0, 0);
            List<ScoredMember> found = members(tuples);
            return found.isEmpty() ? null : found.get(0);
        });
    }

    private static long count(Long value) {
        return value == null ? 0L : value;
    }
}
