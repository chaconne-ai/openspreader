package com.chaconneai.openspreader.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * This process's copy of the data: a purely local data structure that knows nothing of the
 * cluster.
 *
 * <h2>The concurrency model: one writer, many readers, and locks only on a single key</h2>
 * <b>Writes are serialised globally</b> -- every write is started by {@link CacheService}
 * inside its state lock, so one thread changes the data at a time. There is therefore no need
 * to guard against write-write conflicts here, only against "reading a list that is half
 * changed".
 *
 * <p>So there is no global lock: the top level is a {@link ConcurrentHashMap} and reading one
 * key is lock-free, while a list's or a hash's contents exclude each other through <b>the
 * value object itself</b>, leaving a read of key A entirely unrelated to a write of key B.
 * That matters for a read-heavy cache -- with one global read-write lock, a single write would
 * shut every reader out, and a cache's reads usually outnumber its writes by two or three
 * orders of magnitude.
 *
 * <h2>A full synchronisation does not block reads</h2>
 * {@link #restore} does not clear in place and refill; it <b>builds another table beside the
 * old one and swaps the reference</b>. A read during the synchronisation sees either the
 * complete old data or the complete new data, never a half-cleared intermediate state, and is
 * never blocked once.
 *
 * <h2>Expiry is handled separately</h2>
 * {@link #apply} makes <b>no</b> expiry decision and only executes; expired keys are cleared
 * by the leader broadcasting a {@link CacheOp#DEL}. The reason is clock skew between nodes:
 * having each decide "this key has expired, so I delete it" would leave a subsequent lpush
 * landing on an empty list on some nodes and on the old list on others.
 *
 * <p><b>Reads</b>, however, treat an expired key as absent ({@link #live}), so the read
 * semantics are correct even before the deletion has been broadcast.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class CacheStore {

    /**
     * volatile: {@link #restore} swaps this table wholesale, and readers must see the new
     * reference at once.
     */
    private volatile ConcurrentHashMap<String, CacheValue> data = new ConcurrentHashMap<>();

    /**
     * The approximate bytes of everything held, maintained incrementally.
     *
     * <p>"Approximate" is meant seriously: it counts byte[] lengths plus a fixed per-entry
     * overhead, without the JVM's real object header layout and without
     * ConcurrentHashMap's own bucket array. Its purpose is to draw a line for eviction, not to
     * account for the JVM's memory -- precision would need instrumentation, at a cost far
     * beyond its worth.
     */
    private final AtomicLong approxBytes = new AtomicLong();

    /** The approximate overhead of the key name itself: a String object plus a map node. */
    private static final int KEY_OVERHEAD = 64;

    /**
     * How large one bitmap may be: 64MB, which is somewhat over 500 million bits.
     *
     * <p>Without a ceiling, one slip writing {@code setbit(key, Long.MAX_VALUE, true)} would
     * ask for an exabyte-scale array on the spot, dragging the whole process down before the
     * OOM. Five hundred million bits is enough for a Bloom filter of ten million entries at a
     * 0.1% rate.
     */
    private static final int MAX_BITMAP_BYTES = 64 * 1024 * 1024;

    /** Whether reads record access information. Switched off where nothing is evicted or
     *  eviction is random, saving two writes on the read path. */
    private volatile boolean trackAccess = true;

    /**
     * Which keys this round has read, gathered to report to the leader. null means nothing is
     * reported, which is the leader's own case.
     *
     * <p>Why it exists: <b>reads all happen locally and the leader cannot see them</b>. Without
     * the report, the leader's LRU has only the writes to go on, and would delete as cold a hot
     * key that is never written and constantly read -- exactly what a cache should most keep.
     */
    private volatile Set<String> recentReads;

    /** The reporting sample rate: one read in this many is recorded on average. Reads run into
     *  the tens of millions a second, and recording them all would be crushed by the set
     *  insertions alone. */
    private volatile int readSampleRate = 16;

    /** The cap on {@link #recentReads}, so that a round reading a few million distinct keys
     *  does not eat the memory. */
    private volatile int recentReadsCapacity = 10_000;

    /**
     * One write's result. Each operation type uses whichever of the three fields it needs:
     *
     * <ul>
     *   <li>{@code bytes} -- the element lpop or rpop removed</li>
     *   <li>{@code number} -- the value after incr, the length after a push, the keys cleared
     *       by clear</li>
     *   <li>{@code flag} -- whether setIfAbsent wrote, whether delete really removed
     *       something</li>
     * </ul>
     */
    public record Result(byte[] bytes, long number, boolean flag) {

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

    // ------------------------------------------------------------------
    // Writes -- the caller guarantees global serialisation
    // ------------------------------------------------------------------

    /**
     * Executes one write.
     *
     * <p><b>It must be deterministic</b>: the same input executed on any node must give the
     * same result, or the replicas fork on replay. So nothing here may use a random number or
     * read the current time to decide anything -- a TTL travels as milliseconds remaining and
     * each node converts it to a local absolute instant; see {@link CacheOp#SET}.
     *
     * <p><b>The caller must guarantee that one thread calls this at a time.</b>
     * {@link CacheService} calls it inside its own state lock, since assigning a version and
     * executing belong together anyway.
     *
     * @param arg its meaning varies with op: a TTL in milliseconds, or incr's increment
     * @throws ProcessingCacheException on a type mismatch, or when incr's value is not an
     *                                  integer
     */
    public Result apply(CacheOp op, String key, String field, byte[] value, long arg) {
        Result result = applyInternal(op, key, field, value, arg);
        // A write is a use too.
        //
        // Were only reads to refresh the timestamp, a key written far more than it is read -- a
        // Bloom filter's bitmap, a counter, a de-duplication table -- would keep the timestamp
        // it was created with, and LRU would take the most active key for the oldest and evict
        // it first.
        //
        // The symptom is "write it, read it back at once, and it is gone", with nothing
        // reported. Redis's LRU and LFU count a write as an access too
        if (trackAccess && op != CacheOp.DEL && op != CacheOp.CLEAR) {
            CacheValue touched = data.get(key);
            if (touched != null) {
                touched.touch(System.currentTimeMillis());
            }
        }
        return result;
    }

    private Result applyInternal(CacheOp op, String key, String field, byte[] value, long arg) {
        long now = System.currentTimeMillis();
        Map<String, CacheValue> map = data;
        return switch (op) {
            case SET -> {
                CacheValue v = CacheValue.ofString(value);
                v.expireAt(arg > 0 ? now + arg : 0L);
                put(map, key, v);
                yield Result.of(true);
            }
            case SET_IF_ABSENT -> {
                if (map.containsKey(key)) {
                    yield Result.of(false);
                }
                CacheValue v = CacheValue.ofString(value);
                v.expireAt(arg > 0 ? now + arg : 0L);
                put(map, key, v);
                yield Result.of(true);
            }
            case DEL -> Result.of(remove(map, key) != null);
            case EXPIRE -> {
                CacheValue v = map.get(key);
                if (v == null) {
                    yield Result.of(false);
                }
                if (arg <= 0) {
                    // Redis's EXPIRE with 0 or a negative value deletes the key immediately;
                    // it does not cancel the expiry, which is PERSIST's job. The two mean
                    // opposite things, and confusing them turns "expire it now" into "never
                    // expire it"
                    yield Result.of(remove(map, key) != null);
                }
                v.expireAt(now + arg);
                yield Result.of(true);
            }
            case PERSIST -> {
                CacheValue v = map.get(key);
                if (v == null || v.expireAt() == 0L) {
                    yield Result.of(false);
                }
                v.expireAt(0L);
                yield Result.of(true);
            }
            case INCR -> {
                CacheValue v = map.get(key);
                long current = 0L;
                if (v != null) {
                    v.require(CacheValue.Kind.STRING, key, "incr");
                    current = parseLong(key, v.str());
                }
                long next;
                try {
                    // Redis answers "increment or decrement would overflow" rather than
                    // wrapping round to a negative number. Silent wrapping is catastrophic in a
                    // counter
                    next = Math.addExact(current, arg);
                } catch (ArithmeticException e) {
                    throw new ProcessingCacheException("incr on key " + key + " would overflow: "
                            + current + " + " + arg);
                }
                byte[] encoded = Long.toString(next).getBytes(StandardCharsets.UTF_8);
                if (v == null) {
                    // A newly created key carries no TTL; an existing key keeps the one it
                    // had, as in Redis
                    put(map, key, CacheValue.ofString(encoded));
                } else {
                    resize(v, () -> v.str(encoded));
                }
                yield Result.of(next);
            }
            case SETBIT -> {
                if (arg < 0) {
                    throw new ProcessingCacheException("a bit offset must not be negative: "
                            + arg);
                }
                // A Bloom filter for 150 million entries at a 0.1% rate already exceeds an
                // int's 2.14 billion bits, so the offset is a long along the whole path:
                // CacheMessage.arg, here, and ProcessingCache
                int byteIndex = (int) (arg >>> 3);
                if (byteIndex >= MAX_BITMAP_BYTES) {
                    throw new ProcessingCacheException("bit offset " + arg + " exceeds the "
                            + "ceiling; one bitmap holds at most " + MAX_BITMAP_BYTES
                            + " bytes");
                }
                boolean on = value != null && value.length > 0 && value[0] != 0;
                CacheValue v = map.get(key);
                if (v == null) {
                    if (!on) {
                        // Setting a bit to 0 on an absent key: Redis creates the key, but that
                        // leaves an empty bitmap behind for what was purely a look. Nothing is
                        // created here
                        yield Result.of(false);
                    }
                    v = CacheValue.ofString(new byte[byteIndex + 1]);
                    put(map, key, v);
                } else {
                    v.require(CacheValue.Kind.STRING, key, "setbit");
                }
                CacheValue target = v;
                byte[] bytes = target.str();
                boolean was = byteIndex < bytes.length
                        && (bytes[byteIndex] & bitMask(arg)) != 0;
                if (byteIndex >= bytes.length) {
                    if (!on) {
                        // The bit to clear lies beyond the array and is therefore already 0,
                        // so nothing needs growing for it
                        yield Result.of(false);
                    }
                    // Grown exactly to byteIndex+1, not one byte more.
                    //
                    // Headroom -- doubling, or 4KB extra -- was tried to remove the O(n^2)
                    // described below, and is wrong: a bitmap and a string share the STRING
                    // type, and {@code get(key)} returns the whole underlying array, whose
                    // length is publicly visible semantics -- `growsOnDemand` asserts that
                    // 10000 bits is 1251 bytes. Headroom would have get read back a tail of
                    // zeroes, and would disagree with Redis's SETBIT.
                    //
                    // The cost: writing at increasing offsets needs another byte every 8
                    // setbits, so the whole array is copied every 8 writes, O(n^2) in total.
                    // Measured, the P50 stayed at 0.32ms against a control's 0.25ms while the
                    // mean jumped between 0.61 and 1.43ms -- most operations normal, a few
                    // resizes blowing out the tail.
                    //
                    // Where the bitmap's final size is known, <b>set the highest bit first</b>:
                    // one allocation, and every write afterwards changes a bit in place. A
                    // Bloom filter has that shape naturally, its bit count fixed at create
                    // time, and is unaffected
                    byte[] grown = new byte[byteIndex + 1];
                    System.arraycopy(bytes, 0, grown, 0, bytes.length);
                    bytes = grown;
                }
                byte[] next = bytes;
                if (on) {
                    next[byteIndex] |= bitMask(arg);
                } else {
                    next[byteIndex] &= (byte) ~bitMask(arg);
                }
                resize(target, () -> target.str(next));
                // The bit's previous value is returned, as Redis's SETBIT does. A Bloom filter
                // has no use for it, but "was I the first to set this bit" is useful
                // elsewhere
                yield Result.of(was);
            }
            case HINCRBY -> {
                CacheValue v = map.get(key);
                if (v == null) {
                    v = CacheValue.ofHash();
                    put(map, key, v);
                } else {
                    v.require(CacheValue.Kind.HASH, key, "hincrby");
                }
                CacheValue target = v;
                byte[] existing = target.hash().get(field);
                long current = existing == null ? 0L : parseLong(key, existing);
                long next;
                try {
                    next = Math.addExact(current, arg);
                } catch (ArithmeticException e) {
                    throw new ProcessingCacheException("hincrby on field " + field + " of hash "
                            + key + " would overflow: " + current + " + " + arg);
                }
                byte[] encoded = Long.toString(next).getBytes(StandardCharsets.UTF_8);
                resize(target, () -> target.hashPut(field, encoded));
                yield Result.of(next);
            }
            case LTRIM -> {
                CacheValue v = map.get(key);
                if (v == null) {
                    yield Result.of(false);
                }
                v.require(CacheValue.Kind.LIST, key, "ltrim");
                CacheValue target = v;
                int start = (int) (arg >> 32);
                int stop = (int) arg;
                resize(target, () -> target.ltrim(start, stop));
                if (target.isEmpty()) {
                    remove(map, key);
                }
                yield Result.of(true);
            }
            case MAX, MIN, SUM -> {
                double sample = Double.longBitsToDouble(arg);
                CacheValue v = map.get(key);
                if (v == null) {
                    v = CacheValue.ofStats();
                    put(map, key, v);
                } else {
                    v.require(CacheValue.Kind.STATS, key, op.name().toLowerCase());
                }
                // The size is fixed, so no resize is needed
                double updated = switch (op) {
                    case MAX -> v.statMax(sample);
                    case MIN -> v.statMin(sample);
                    default -> v.statSum(sample);
                };
                yield Result.of(Double.doubleToRawLongBits(updated));
            }
            case LPUSH, RPUSH -> {
                CacheValue v = map.get(key);
                if (v == null) {
                    v = CacheValue.ofList();
                    put(map, key, v);
                } else {
                    v.require(CacheValue.Kind.LIST, key, op.name().toLowerCase());
                }
                CacheValue target = v;
                byte[] item = copy(value);
                resize(target, () -> target.listAdd(item, op == CacheOp.LPUSH));
                synchronized (target) {
                    yield Result.of(target.list().size());
                }
            }
            case LPOP, RPOP -> {
                CacheValue v = map.get(key);
                if (v == null) {
                    yield Result.NONE;
                }
                v.require(CacheValue.Kind.LIST, key, op.name().toLowerCase());
                byte[][] popped = new byte[1][];
                resize(v, () -> popped[0] = v.listPoll(op == CacheOp.LPOP));
                boolean emptied;
                synchronized (v) {
                    emptied = v.list().isEmpty();
                }
                if (emptied) {
                    remove(map, key, v);
                }
                yield Result.of(popped[0]);
            }
            case HSET -> {
                CacheValue v = map.get(key);
                if (v == null) {
                    v = CacheValue.ofHash();
                    put(map, key, v);
                } else {
                    v.require(CacheValue.Kind.HASH, key, "hset");
                }
                CacheValue target = v;
                byte[] item = copy(value);
                boolean[] isNew = new boolean[1];
                resize(target, () -> isNew[0] = target.hashPut(field, item));
                yield Result.of(isNew[0]);
            }
            case HDEL -> {
                CacheValue v = map.get(key);
                if (v == null) {
                    yield Result.of(false);
                }
                v.require(CacheValue.Kind.HASH, key, "hdel");
                boolean[] removed = new boolean[1];
                resize(v, () -> removed[0] = v.hashRemove(field));
                boolean emptied;
                synchronized (v) {
                    emptied = v.hash().isEmpty();
                }
                if (emptied) {
                    remove(map, key, v);
                }
                yield Result.of(removed[0]);
            }
            case ZADD -> {
                double score = Double.longBitsToDouble(arg);
                // Validated before the data is touched. The other way round, the key would
                // already exist by the time the score was found invalid, and the leader would
                // have changed its own copy without broadcasting, having consumed no version --
                // forking the replicas on the spot
                requireScore(key, score, "zadd");
                CacheValue v = map.get(key);
                if (v == null) {
                    v = CacheValue.ofZSet();
                    put(map, key, v);
                } else {
                    v.require(CacheValue.Kind.ZSET, key, "zadd");
                }
                CacheValue target = v;
                byte[] member = copy(value);
                boolean[] added = new boolean[1];
                resize(target, () -> added[0] = target.zadd(member, score));
                yield Result.of(added[0]);
            }
            case ZREM -> {
                CacheValue v = map.get(key);
                if (v == null) {
                    yield Result.of(false);
                }
                v.require(CacheValue.Kind.ZSET, key, "zrem");
                byte[] member = copy(value);
                boolean[] removed = new boolean[1];
                resize(v, () -> removed[0] = v.zrem(member));
                boolean emptied;
                synchronized (v) {
                    emptied = v.size() == 0;
                }
                if (emptied) {
                    remove(map, key, v);
                }
                yield Result.of(removed[0]);
            }
            case ZINCRBY -> {
                double delta = Double.longBitsToDouble(arg);
                requireScore(key, delta, "zincrby");
                CacheValue existing = map.get(key);
                if (existing != null) {
                    existing.require(CacheValue.Kind.ZSET, key, "zincrby");
                }
                byte[] member = copy(value);

                // The new score is computed and validated first, and only once everything
                // passes are the key created and the data changed. +inf plus -inf gives NaN,
                // which Redis refuses outright -- and once NaN enters the ordered structure it
                // compares false against every number, so the entry can never be located or
                // removed again
                double current = 0.0;
                if (existing != null) {
                    synchronized (existing) {
                        Double score = existing.zscore(member);
                        current = score == null ? 0.0 : score;
                    }
                }
                double next = current + delta;
                if (Double.isNaN(next)) {
                    throw new ProcessingCacheException("zincrby on key " + key
                            + " gives NaN: "
                            + current + " + " + delta);
                }

                CacheValue target = existing;
                if (target == null) {
                    target = CacheValue.ofZSet();
                    put(map, key, target);
                }
                CacheValue finalTarget = target;
                resize(finalTarget, () -> finalTarget.zadd(member, next));
                yield new Result(null, Double.doubleToRawLongBits(next), true);
            }
            case ZPOPMIN, ZPOPMAX -> {
                // Reaching here means someone put a pop operation straight into the replication
                // stream. That is a programming error: each node popping for itself pops a
                // different member; see the note on CacheOp
                throw new ProcessingCacheException("ZPOPMIN and ZPOPMAX must not enter the "
                        + "replication stream; the leader must resolve the member first and "
                        + "broadcast a ZREM");
            }
            case CLEAR -> {
                int n = map.size();
                // A new table rather than deleting one by one: no reader sees a half-deleted
                // state
                data = new ConcurrentHashMap<>();
                approxBytes.set(0L);
                yield Result.of(n);
            }
        };
    }

    /**
     * A score must be a number.
     *
     * <p>Caught <b>before anything is touched</b>: NaN compares false against every number, and
     * an entry placed in the ordered structure could never be located or removed again. And
     * were the key created before the invalidity was found, the leader would throw having
     * changed its own copy without broadcasting, and the replicas would fork from there. Redis
     * likewise refuses nan outright.
     */
    private static void requireScore(String key, double score, String operation) {
        if (Double.isNaN(score)) {
            throw new ProcessingCacheException(operation + " on key " + key
                    + ": the score must not be NaN");
        }
    }

    /**
     * Changes the content and records the byte difference against the total.
     *
     * <p>The lock keeps concurrent reads out -- a read takes {@code synchronized (v)} too --
     * while writes are serialised against each other anyway; see the class javadoc.
     */
    private void resize(CacheValue v, Runnable mutation) {
        long delta;
        synchronized (v) {
            long before = v.approxBytes();
            mutation.run();
            delta = v.approxBytes() - before;
        }
        if (delta != 0) {
            approxBytes.addAndGet(delta);
        }
    }

    private void put(Map<String, CacheValue> map, String key, CacheValue v) {
        CacheValue old = map.put(key, v);
        long delta = v.approxBytes() - (old == null ? -KEY_OVERHEAD : old.approxBytes());
        approxBytes.addAndGet(delta);
    }

    private CacheValue remove(Map<String, CacheValue> map, String key) {
        CacheValue old = map.remove(key);
        if (old != null) {
            approxBytes.addAndGet(-(old.approxBytes() + KEY_OVERHEAD));
        }
        return old;
    }

    private void remove(Map<String, CacheValue> map, String key, CacheValue expected) {
        if (map.remove(key, expected)) {
            approxBytes.addAndGet(-(expected.approxBytes() + KEY_OVERHEAD));
        }
    }

    // ------------------------------------------------------------------
    // Reads -- lock-free lookup, excluding only on a single value, and always skipping
    // expired keys
    // ------------------------------------------------------------------

    public byte[] get(String key) {
        CacheValue v = live(key);
        if (v == null) {
            return null;
        }
        v.require(CacheValue.Kind.STRING, key, "get");
        synchronized (v) {
            return copy(v.str());
        }
    }

    public boolean exists(String key) {
        return live(key) != null;
    }

    /**
     * @return none, string, list, hash or zset -- the names Redis's {@code TYPE} uses
     */
    public String type(String key) {
        CacheValue v = live(key);
        return v == null ? "none" : v.kind().name().toLowerCase();
    }

    public int size(String key) {
        CacheValue v = live(key);
        if (v == null) {
            return 0;
        }
        synchronized (v) {
            return v.size();
        }
    }

    /** Reads one bit. An absent key, the wrong type, or a bit beyond the array all count
     *  as 0. */
    public boolean getbit(String key, long offset) {
        if (offset < 0) {
            return false;
        }
        CacheValue v = live(key);
        if (v == null || v.kind() != CacheValue.Kind.STRING) {
            return false;
        }
        byte[] bytes = v.str();
        int byteIndex = (int) (offset >>> 3);
        // A bit beyond the array was never written and is therefore 0 -- no error, as in
        // Redis
        return byteIndex < bytes.length && (bytes[byteIndex] & bitMask(offset)) != 0;
    }

    /** Reads a statistical aggregate. An absent key returns an empty one. */
    public CacheStats stats(String key) {
        CacheValue v = live(key);
        if (v == null) {
            return CacheStats.EMPTY;
        }
        v.require(CacheValue.Kind.STATS, key, "stats");
        // With no samples at all, what is stored internally is +/-Infinity, the aggregate's
        // correct initial value -- but what is published should be NaN: "no data" must not be
        // charted as a line at +/-infinity
        double max = v.statCountValue() == 0 && Double.isInfinite(v.statMaxValue())
                ? Double.NaN : v.statMaxValue();
        double min = v.statCountValue() == 0 && Double.isInfinite(v.statMinValue())
                ? Double.NaN : v.statMinValue();
        return new CacheStats(max, min, v.statSumValue(), v.statCountValue());
    }

    public byte[] hget(String key, String field) {
        CacheValue v = live(key);
        if (v == null) {
            return null;
        }
        v.require(CacheValue.Kind.HASH, key, "hget");
        synchronized (v) {
            return copy(v.hash().get(field));
        }
    }

    public Map<String, byte[]> hgetAll(String key) {
        CacheValue v = live(key);
        if (v == null) {
            return Map.of();
        }
        v.require(CacheValue.Kind.HASH, key, "hgetAll");
        synchronized (v) {
            Map<String, byte[]> out = new LinkedHashMap<>(Math.max(4, v.hash().size() * 2));
            v.hash().forEach((f, b) -> out.put(f, copy(b)));
            return out;
        }
    }

    /** Indexes are inclusive at both ends and may be negative; out-of-range values clamp to
     *  the valid range. */
    public List<byte[]> lrange(String key, int start, int stop) {
        CacheValue v = live(key);
        if (v == null) {
            return List.of();
        }
        v.require(CacheValue.Kind.LIST, key, "lrange");
        synchronized (v) {
            int n = v.list().size();
            if (n == 0) {
                return List.of();
            }
            int from = start < 0 ? Math.max(0, n + start) : Math.min(start, n);
            int to = stop < 0 ? n + stop : Math.min(stop, n - 1);
            if (from > to) {
                return List.of();
            }
            List<byte[]> out = new ArrayList<>(to - from + 1);
            int i = 0;
            for (byte[] item : v.list()) {
                if (i > to) {
                    break;
                }
                if (i >= from) {
                    out.add(copy(item));
                }
                i++;
            }
            return out;
        }
    }

    /** @return the member's score, or null when the key or the member is absent */
    public Double zscore(String key, byte[] member) {
        CacheValue v = live(key);
        if (v == null) {
            return null;
        }
        v.require(CacheValue.Kind.ZSET, key, "zscore");
        synchronized (v) {
            return v.zscore(member);
        }
    }

    /**
     * Takes a stretch by index, both ends inclusive, indexes possibly negative with -1 the
     * last -- as Redis's ZRANGE does.
     *
     * <p><b>This is an O(n) walk.</b> Underneath is a red-black tree, which has no span
     * information as Redis's skip list does, so locating by index means counting through one by
     * one. It slows noticeably for a large range far along; a range by score
     * ({@link #zrangeByScore}) is unaffected.
     *
     * @param reverse true makes it ZREVRANGE, counting from the high-score end
     */
    public List<ScoredMember> zrange(String key, int start, int stop, boolean reverse) {
        CacheValue v = live(key);
        if (v == null) {
            return List.of();
        }
        v.require(CacheValue.Kind.ZSET, key, reverse ? "zrevrange" : "zrange");
        synchronized (v) {
            int n = v.sorted().size();
            if (n == 0) {
                return List.of();
            }
            int from = start < 0 ? Math.max(0, n + start) : Math.min(start, n);
            int to = stop < 0 ? n + stop : Math.min(stop, n - 1);
            if (from > to) {
                return List.of();
            }
            List<ScoredMember> out = new ArrayList<>(to - from + 1);
            int i = 0;
            for (ScoredMember m : (reverse ? v.sorted().descendingSet() : v.sorted())) {
                if (i > to) {
                    break;
                }
                if (i >= from) {
                    out.add(new ScoredMember(m.member().clone(), m.score()));
                }
                i++;
            }
            return out;
        }
    }

    /**
     * Takes a stretch by score, both ends inclusive -- as Redis's ZRANGEBYSCORE does.
     *
     * <p>This is O(log n + m): it locates the start in the tree and walks m from there.
     *
     * @param reverse true makes it ZREVRANGEBYSCORE, ordered by score descending
     */
    public List<ScoredMember> zrangeByScore(String key, double min, double max, boolean reverse) {
        CacheValue v = live(key);
        if (v == null) {
            return List.of();
        }
        v.require(CacheValue.Kind.ZSET, key, "zrangeByScore");
        synchronized (v) {
            // Two sentinels bound the range: the lower one sits before the byte-wise smallest
            // member at that score and the upper one after the largest, so no member sharing a
            // score is missed
            ScoredMember lo = new ScoredMember(new byte[0], min);
            NavigableSet<ScoredMember> range = v.sorted().tailSet(lo, true);
            List<ScoredMember> out = new ArrayList<>();
            for (ScoredMember m : range) {
                if (m.score() > max) {
                    break;
                }
                out.add(new ScoredMember(m.member().clone(), m.score()));
            }
            if (reverse) {
                Collections.reverse(out);
            }
            return out;
        }
    }

    /** How many members have a score within the range, both ends inclusive -- Redis's
     *  ZCOUNT. */
    public int zcount(String key, double min, double max) {
        return zrangeByScore(key, min, max, false).size();
    }

    /**
     * A member's rank, from 0 -- Redis's ZRANK and ZREVRANK.
     *
     * <p>This too is O(n); see {@link #zrange} for why.
     *
     * @return the rank, or -1 when the key or the member is absent
     */
    public long zrank(String key, byte[] member, boolean reverse) {
        CacheValue v = live(key);
        if (v == null) {
            return -1L;
        }
        v.require(CacheValue.Kind.ZSET, key, reverse ? "zrevrank" : "zrank");
        synchronized (v) {
            Double score = v.zscore(member);
            if (score == null) {
                return -1L;
            }
            long i = 0;
            for (ScoredMember m : (reverse ? v.sorted().descendingSet() : v.sorted())) {
                if (m.score() == score && Arrays.equals(m.member(), member)) {
                    return i;
                }
                i++;
            }
            return -1L;
        }
    }

    /** The leader uses it to decide which member to pop, then broadcasts the removal of that
     *  one. */
    public ScoredMember zpeek(String key, boolean min) {
        CacheValue v = live(key);
        if (v == null) {
            return null;
        }
        v.require(CacheValue.Kind.ZSET, key, min ? "zpopmin" : "zpopmax");
        synchronized (v) {
            ScoredMember m = v.zpeek(min);
            return m == null ? null : new ScoredMember(m.member().clone(), m.score());
        }
    }

    public long ttl(String key) {
        CacheValue v = live(key);
        if (v == null) {
            return -2L;
        }
        return v.expireAt() == 0 ? -1L : v.expireAt() - System.currentTimeMillis();
    }

    /**
     * Lists key names by pattern.
     *
     * <p>This is a full scan and is not cheap with many keys, so do not put it on a hot path --
     * which is also why Redis advises against {@code KEYS}.
     */
    public Set<String> keys(String pattern) {
        Pattern regex = glob(pattern);
        long now = System.currentTimeMillis();
        Set<String> out = new LinkedHashSet<>();
        data.forEach((k, v) -> {
            if (!v.expired(now) && regex.matcher(k).matches()) {
                out.add(k);
            }
        });
        return out;
    }

    /** It includes expired keys not yet swept, so it may exceed {@code keys("*")}. */
    public int keyCount() {
        return data.size();
    }

    /** Keys that have expired and await the leader's broadcast deletion. Only the leader calls
     *  this. */
    public List<String> expiredKeys(long now) {
        List<String> out = new ArrayList<>();
        data.forEach((k, v) -> {
            if (v.expired(now)) {
                out.add(k);
            }
        });
        return out;
    }

    /** Whether the key exists but has expired -- the leader confirms this before writing; see
     *  {@link CacheService}. */
    public boolean isExpired(String key) {
        CacheValue v = data.get(key);
        return v != null && v.expired(System.currentTimeMillis());
    }

    /**
     * Takes a live value: absent or expired both count as absent.
     *
     * <p>It records an access along the way, for eviction. That is the read path's only extra
     * cost -- two ordinary long field writes, with no lock and no memory barrier; see
     * {@link CacheValue#touch}.
     */
    private CacheValue live(String key) {
        long now = System.currentTimeMillis();
        CacheValue v = data.get(key);
        if (v == null || v.expired(now)) {
            return null;
        }
        if (trackAccess) {
            v.touch(now);
            recordRead(key);
        }
        return v;
    }

    /**
     * Records a sampled read, to be reported to the leader.
     *
     * <p>Sampling uses {@link ThreadLocalRandom} rather than a shared counter: read throughput
     * runs into the tens of millions a second, and at that scale a shared
     * {@code AtomicInteger} is nothing but a point of contention.
     */
    private void recordRead(String key) {
        Set<String> sink = recentReads;
        if (sink == null) {
            return;
        }
        int rate = readSampleRate;
        if (rate > 1 && ThreadLocalRandom.current().nextInt(rate) != 0) {
            return;
        }
        if (sink.size() < recentReadsCapacity) {
            sink.add(key);
        }
    }

    /** Enables read-access reporting. The leader does not need it -- its own reads are
     *  recorded on the values directly. */
    public void enableReadReporting(int sampleRate, int capacity) {
        this.readSampleRate = Math.max(1, sampleRate);
        this.recentReadsCapacity = Math.max(1, capacity);
        this.recentReads = ConcurrentHashMap.newKeySet();
    }

    public void disableReadReporting() {
        this.recentReads = null;
    }

    /** Takes the keys this round gathered and swaps in an empty container to keep
     *  gathering. */
    public Set<String> drainRecentReads() {
        Set<String> sink = recentReads;
        if (sink == null || sink.isEmpty()) {
            return Set.of();
        }
        recentReads = ConcurrentHashMap.newKeySet();
        return sink;
    }

    /** Read accesses reported by other nodes: they refresh the eviction information only, and
     *  touch no data. */
    public void touch(String key) {
        CacheValue v = data.get(key);
        if (v != null) {
            v.touch(System.currentTimeMillis());
        }
    }

    /** With an eviction policy of NONE or RANDOM, access recording can be switched off,
     *  saving the cost on the read path. */
    public void trackAccess(boolean track) {
        this.trackAccess = track;
    }

    public long approxBytes() {
        return approxBytes.get();
    }

    // ------------------------------------------------------------------
    // Eviction: random sampling, with no LRU list maintained
    // ------------------------------------------------------------------

    /**
     * Samples {@code samples} keys and picks the one most deserving eviction.
     *
     * <p>Why not exact LRU: exact LRU moves a node to the head of a list on <b>every read</b>,
     * which needs a global lock and would cut read throughput from tens of millions a second to
     * a few hundred thousand -- and reads not blocking is where this cache's performance mostly
     * comes from. Redis samples for the same reason.
     *
     * <p>The sample is drawn with equal probability from one pass -- reservoir sampling --
     * rather than taking the first N: taking the first N would pick the keys near the front of
     * the map again and again, which amounts to deleting in insertion order.
     *
     * @return the key to evict, or null when there is nothing to evict
     */
    public String pickEvictionCandidate(EvictionPolicy policy, int samples) {
        if (policy == EvictionPolicy.NONE) {
            return null;
        }
        Map<String, CacheValue> map = data;
        if (map.isEmpty()) {
            return null;
        }
        String[] pool = new String[samples];
        CacheValue[] vals = new CacheValue[samples];
        int filled = 0;
        int seen = 0;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (Map.Entry<String, CacheValue> e : map.entrySet()) {
            if (filled < samples) {
                pool[filled] = e.getKey();
                vals[filled] = e.getValue();
                filled++;
            } else {
                int j = rnd.nextInt(seen + 1);
                if (j < samples) {
                    pool[j] = e.getKey();
                    vals[j] = e.getValue();
                }
            }
            seen++;
        }
        if (filled == 0) {
            return null;
        }
        if (policy == EvictionPolicy.RANDOM) {
            return pool[rnd.nextInt(filled)];
        }
        int best = 0;
        for (int i = 1; i < filled; i++) {
            // LRU compares accessOrder, in nanoseconds, rather than lastAccessAt, in
            // milliseconds: tens of thousands of keys written within one millisecond share a
            // timestamp exactly, nothing distinguishes them, and what gets picked is "the first
            // in the sample pool" -- possibly the key just written
            boolean better = policy == EvictionPolicy.LFU
                    ? vals[i].hits() < vals[best].hits()
                    : vals[i].accessOrder() < vals[best].accessOrder();
            if (better) {
                best = i;
            }
        }
        return pool[best];
    }

    // ------------------------------------------------------------------
    // Full snapshots: for a newly joined node, and after a change of leader
    // ------------------------------------------------------------------

    /**
     * Serialises the whole body of data into chunks.
     *
     * <p>Chunking exists because the transport caps a single frame, at 8MB by default, and many
     * keys will not fit in one. Each chunk fills up to {@code maxChunkBytes} before the next
     * begins; <b>one key exceeding the cap is still not split</b> -- split, the receiver could
     * not reassemble it, so it is better to let that frame fail over the cap and leave a log
     * line.
     *
     * <p>A TTL is stored as <b>the milliseconds remaining</b> rather than an absolute instant:
     * two machines' clocks need not agree, and an absolute instant would have a peer whose
     * clock is a few seconds slow treat unexpired keys as expired.
     *
     * <p>The caller must call it where no write can intervene -- {@link CacheService} calls it
     * inside its state lock -- or the snapshot's version and its contents fall out of step.
     */
    public List<byte[]> dump(int maxChunkBytes) {
        return dump(maxChunkBytes, false);
    }

    /**
     * @param absoluteExpiry whether a TTL is stored as an <b>absolute expiry instant</b> or as
     *                       <b>the milliseconds remaining</b>. Going to disk passes true --
     *                       however long the file sits there makes no difference, and loading
     *                       compares directly; synchronising between nodes passes false, since
     *                       two machines' clocks do not agree and an absolute instant would have
     *                       the peer decide wrongly
     */
    public List<byte[]> dump(int maxChunkBytes, boolean absoluteExpiry) {
        long now = System.currentTimeMillis();
        List<byte[]> chunks = new ArrayList<>();
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(Math.min(maxChunkBytes, 64 * 1024));
            DataOutputStream out = new DataOutputStream(bos);
            int inChunk = 0;
            for (Map.Entry<String, CacheValue> e : data.entrySet()) {
                CacheValue v = e.getValue();
                if (v.expired(now)) {
                    continue;   // no reason to send an expired key across
                }
                synchronized (v) {
                    writeEntry(out, e.getKey(), v, now, absoluteExpiry);
                }
                inChunk++;
                if (bos.size() >= maxChunkBytes) {
                    chunks.add(finishChunk(bos, inChunk));
                    bos = new ByteArrayOutputStream(Math.min(maxChunkBytes, 64 * 1024));
                    out = new DataOutputStream(bos);
                    inChunk = 0;
                }
            }
            // Even with no keys at all, one empty chunk is sent, so the receiver knows the
            // snapshot is empty rather than incomplete
            chunks.add(finishChunk(bos, inChunk));
            return chunks;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Replaces the local data wholesale from a snapshot.
     *
     * <p><b>The new table is built beside the old one and the reference swapped</b>: nothing
     * blocks a read at any point, and a read always sees one complete body of data rather than
     * a table cleared and not yet refilled. A full synchronisation routinely covers tens of
     * thousands of keys, and clearing in place would leave the cache effectively dead for that
     * whole stretch.
     */
    public void restore(List<byte[]> chunks) {
        restore(chunks, false);
    }

    /**
     * Replaces the local data from a snapshot.
     *
     * @param absoluteExpiry whether the snapshot's TTLs are absolute expiry instants or
     *                       milliseconds remaining. It must match what
     *                       {@link #dump(int, boolean)} was given: true for going to and coming
     *                       from disk, false for synchronising between nodes
     */
    public void restore(List<byte[]> chunks, boolean absoluteExpiry) {
        long now = System.currentTimeMillis();
        ConcurrentHashMap<String, CacheValue> loaded = new ConcurrentHashMap<>();
        for (byte[] chunk : chunks) {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(chunk))) {
                int count = in.readInt();
                for (int i = 0; i < count; i++) {
                    readEntry(in, loaded, now, absoluteExpiry);
                }
            } catch (IOException e) {
                throw new ProcessingCacheException("the snapshot data is corrupt", e);
            }
        }
        // An expired entry is simply discarded. Only loading from disk can meet one -- a file
        // that sat for hours holds keys that should long since have gone -- while synchronising
        // between nodes is a matter of milliseconds and never does.
        //
        // The test lives here rather than in readEntry, which also serves synchronisation
        // between nodes, where each node deleting by its own clock would fork the replicas: one
        // key deleted on A and still present on B, with subsequent operations acting on
        // different states. Expiry within the cluster is the leader broadcasting a DEL
        loaded.entrySet().removeIf(e -> e.getValue().expired(now));
        long total = 0;
        for (Map.Entry<String, CacheValue> e : loaded.entrySet()) {
            e.getValue().recomputeBytes();
            total += e.getValue().approxBytes() + KEY_OVERHEAD;
        }
        data = loaded;
        approxBytes.set(total);
    }

    private static byte[] finishChunk(ByteArrayOutputStream bos, int count) throws IOException {
        // The entry count belongs at the front, but the entries are already written, so the
        // two are joined here
        byte[] body = bos.toByteArray();
        ByteArrayOutputStream framed = new ByteArrayOutputStream(body.length + 4);
        DataOutputStream out = new DataOutputStream(framed);
        out.writeInt(count);
        out.write(body);
        out.flush();
        return framed.toByteArray();
    }

    /**
     * The mask for bit {@code offset} within the byte holding it.
     *
     * <p>The bit order matches Redis's: <b>within each byte, the most significant bit is bit
     * 0</b>. The intuitive form is {@code 1 << (offset & 7)}, treating the least significant
     * bit as bit 0, which is self-consistent but produces a bitmap that disagrees with Redis's
     * -- and anyone checking the result with redis-cli would find it entirely scrambled.
     */
    private static byte bitMask(long offset) {
        return (byte) (1 << (7 - (int) (offset & 7)));
    }

    /**
     * @param now            the current instant, unused when {@code absoluteExpiry} is true
     * @param absoluteExpiry true writes an <b>absolute expiry instant</b>, false writes <b>the
     *                       milliseconds remaining</b>
     */
    private static void writeEntry(DataOutputStream out, String key, CacheValue v, long now,
                                   boolean absoluteExpiry) throws IOException {
        out.writeUTF(key);
        out.writeByte(v.kind().ordinal());
        if (absoluteExpiry) {
            // Going to disk: the expiry instant is stored directly. However long the file sits
            // there makes no difference, and loading compares it with the now of that moment,
            // with no need to know how long it was stored
            out.writeLong(v.expireAt() == 0 ? -1L : v.expireAt());
        } else {
            // Synchronising between nodes: the milliseconds remaining are stored. Two machines'
            // clocks do not agree, and an absolute instant would have a peer whose clock is a
            // few seconds slow treat unexpired keys as expired. The max(1) is because computing
            // 0 would collide with the meaning of a 0 outside the "-1 means no expiry"
            out.writeLong(v.expireAt() == 0 ? -1L : Math.max(1L, v.expireAt() - now));
        }
        switch (v.kind()) {
            case STRING -> writeBytes(out, v.str());
            case LIST -> {
                out.writeInt(v.list().size());
                for (byte[] item : v.list()) {
                    writeBytes(out, item);
                }
            }
            case HASH -> {
                out.writeInt(v.hash().size());
                for (Map.Entry<String, byte[]> f : v.hash().entrySet()) {
                    out.writeUTF(f.getKey());
                    writeBytes(out, f.getValue());
                }
            }
            case ZSET -> {
                out.writeInt(v.sorted().size());
                for (ScoredMember m : v.sorted()) {
                    writeBytes(out, m.member());
                    out.writeDouble(m.score());
                }
            }
            case STATS -> {
                out.writeDouble(v.statMaxValue());
                out.writeDouble(v.statMinValue());
                out.writeDouble(v.statSumValue());
                out.writeLong(v.statCountValue());
            }
        }
    }

    private static void readEntry(DataInputStream in, Map<String, CacheValue> into, long now,
                                  boolean absoluteExpiry) throws IOException {
        String key = in.readUTF();
        CacheValue.Kind kind = CacheValue.Kind.values()[in.readByte()];
        long remaining = in.readLong();
        CacheValue v = switch (kind) {
            case STRING -> CacheValue.ofString(readBytes(in));
            case LIST -> {
                CacheValue l = CacheValue.ofList();
                int n = in.readInt();
                for (int i = 0; i < n; i++) {
                    l.list().addLast(readBytes(in));
                }
                yield l;
            }
            case HASH -> {
                CacheValue h = CacheValue.ofHash();
                int n = in.readInt();
                for (int i = 0; i < n; i++) {
                    h.hash().put(in.readUTF(), readBytes(in));
                }
                yield h;
            }
            case ZSET -> {
                CacheValue z = CacheValue.ofZSet();
                int n = in.readInt();
                for (int i = 0; i < n; i++) {
                    byte[] member = readBytes(in);
                    z.zadd(member, in.readDouble());
                }
                yield z;
            }
            case STATS -> {
                CacheValue s = CacheValue.ofStats();
                s.restoreStats(in.readDouble(), in.readDouble(), in.readDouble(), in.readLong());
                yield s;
            }
        };
        // remaining means different things in the two formats: an absolute instant is used as
        // it is, while milliseconds remaining are added to now
        v.expireAt(remaining < 0 ? 0L : (absoluteExpiry ? remaining : now + remaining));
        into.put(key, v);
    }

    private static void writeBytes(DataOutputStream out, byte[] b) throws IOException {
        if (b == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(b.length);
        out.write(b);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0) {
            return null;
        }
        byte[] b = new byte[len];
        in.readFully(b);
        return b;
    }

    // ------------------------------------------------------------------

    /**
     * Every byte array read out is a copy.
     *
     * <p>Handing the internal array out directly would be quicker, but a caller changing it
     * would change this process's copy -- and that change <b>would not</b> be synchronised to
     * anyone, forking the replicas invisibly. This much copying buys the invariant that a
     * replica is only ever changed by the replication stream.
     */
    private static byte[] copy(byte[] b) {
        return b == null ? null : b.clone();
    }

    private static long parseLong(String key, byte[] raw) {
        String s = new String(raw, StandardCharsets.UTF_8).trim();
        try {
            return s.isEmpty() ? 0L : Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new ProcessingCacheException("the value on key " + key + " is not an integer, "
                    + "so incr cannot be applied: " + s);
        }
    }

    /**
     * Redis's glob wildcards: {@code *} for any length, {@code ?} for one character,
     * {@code [abc]} for a character set, {@code [a-z]} for a range, {@code [^abc]} for
     * negation, and {@code \\} to escape the next character.
     *
     * <p>A character set's contents pass through to the regular expression, except that
     * {@code \} and {@code ]} are escaped first -- otherwise an input like {@code [a\]} could
     * bend the whole expression's meaning.
     */
    private static Pattern glob(String pattern) {
        if (pattern == null || pattern.isEmpty() || "*".equals(pattern)) {
            return Pattern.compile(".*", Pattern.DOTALL);
        }
        StringBuilder sb = new StringBuilder(pattern.length() * 2 + 2).append('^');
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case '*' -> sb.append(".*");
                case '?' -> sb.append('.');
                case '\\' -> {
                    // Escapes the next character; a trailing backslash is a literal itself
                    if (i + 1 < pattern.length()) {
                        sb.append(Pattern.quote(String.valueOf(pattern.charAt(++i))));
                    } else {
                        sb.append(Pattern.quote("\\"));
                    }
                }
                case '[' -> {
                    int close = findClassEnd(pattern, i);
                    if (close < 0) {
                        // With no matching bracket, Redis treats it as an ordinary character
                        sb.append(Pattern.quote("["));
                    } else {
                        sb.append(translateClass(pattern.substring(i + 1, close)));
                        i = close;
                    }
                }
                default -> sb.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return Pattern.compile(sb.append('$').toString(), Pattern.DOTALL);
    }

    /** Finds a character set's closing bracket, accounting for escapes. */
    private static int findClassEnd(String pattern, int open) {
        for (int i = open + 1; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '\\') {
                i++;
            } else if (c == ']' && i > open + 1) {
                return i;
            }
        }
        return -1;
    }

    private static String translateClass(String body) {
        StringBuilder sb = new StringBuilder(body.length() + 4).append('[');
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            switch (c) {
                case '^' -> sb.append(i == 0 ? "^" : "\\^");
                case '\\', ']', '[', '&' -> sb.append('\\').append(c);
                default -> sb.append(c);
            }
        }
        return sb.append(']').toString();
    }

    /** For troubleshooting: each key's kind, size and remaining TTL. */
    public Map<String, String> describe() {
        long now = System.currentTimeMillis();
        Map<String, String> out = new LinkedHashMap<>();
        data.forEach((k, v) -> out.put(k, v.kind() + "(" + v.size() + ")"
                + (v.expireAt() == 0 ? "" : " ttl=" + (v.expireAt() - now) + "ms")
                + (v.expired(now) ? " [expired, awaiting sweep]" : "")));
        return out;
    }
}
