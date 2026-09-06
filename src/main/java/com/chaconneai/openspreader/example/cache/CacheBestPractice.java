package com.chaconneai.openspreader.example.cache;

import com.chaconneai.openspreader.cache.ProcessingCacheException;
import com.chaconneai.openspreader.cache.ProcessingCache;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * How to use the cluster cache.
 *
 * <h2>Two steps to adopt it</h2>
 * <ol>
 *   <li>Add spreader-commons; the container already has a {@code GossipCluster} bean</li>
 *   <li>Inject {@link ProcessingCache} and use it</li>
 * </ol>
 * Not a line of configuration is needed -- the defaults are the recommended values:
 * <pre>
 * spring.spreader.multiprocessing.cache.enabled            = true
 * spring.spreader.multiprocessing.cache.eviction-policy    = LRU
 * spring.spreader.multiprocessing.cache.max-keys           = 1000000
 * spring.spreader.multiprocessing.cache.max-bytes          = -1     # 25% of the maximum heap
 * spring.spreader.multiprocessing.cache.request-timeout-ms = 8000   # must cover a takeover
 * </pre>
 *
 * <h2>Five data types</h2>
 * Strings, lists, hashes, sorted sets and statistical aggregates, <b>behaving as Redis
 * does</b> -- including WRONGTYPE type checking, {@code EXPIRE 0} meaning delete rather than
 * cancel an expiry, incr overflow reporting an error, and glob wildcards in {@code keys}.
 *
 * <h2>More specific scenarios live in their own classes</h2>
 * This class covers the cache <b>itself</b>. Several common scenarios each have a class:
 * <ul>
 *   <li>{@link RateLimitBestPractice} -- cluster-wide rate limiting</li>
 *   <li>{@link LeaderboardBestPractice} -- a live leaderboard</li>
 *   <li>{@link DelayedQueueBestPractice} -- a delayed task queue</li>
 * </ul>
 *
 * <h2>Two things you must know</h2>
 * <ol>
 *   <li><b>Reads are local; writes go through the leader.</b> A read is a pure memory
 *       operation, measured in the millions of ops/s; a write runs locally on the leader and
 *       costs a network round trip elsewhere, measured at about 20,000 ops/s, and 700,000
 *       ops/s on the leader itself. So <b>it pays where reads far outnumber writes</b>.</li>
 *   <li><b>It is eventually consistent.</b> A write is readable here at once, but other nodes
 *       wait for the broadcast -- measured at 0.65 seconds for five nodes to catch up on
 *       100,000 writes. Under a network partition each side may write its own and they cannot
 *       be merged back, so do not deduct stock with it.</li>
 * </ol>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class CacheBestPractice {

    private final ProcessingCache cache;

    public CacheBestPractice(ProcessingCache cache) {
        this.cache = cache;
    }

    // ------------------------------------------------------------------
    // 1. The commonest usage: caching what the source returns
    // ------------------------------------------------------------------

    /**
     * On a miss, go to the source and write the result back.
     *
     * <p>Several instances missing at once each go to the source -- there is <b>no</b> stampede
     * protection here. Where going to the source is expensive, a slow query say, pair it with
     * {@code ProcessingMutex} so one instance queries and the rest wait to read what it writes,
     * or use the shape in {@link #loadOnce} below.
     */
    public String getOrLoad(String userId) {
        String key = "user:" + userId;
        byte[] cached = cache.get(key);
        if (cached != null) {
            return new String(cached, StandardCharsets.UTF_8);
        }
        String fresh = loadFromDatabase(userId);
        cache.set(key, fresh.getBytes(StandardCharsets.UTF_8), 10, TimeUnit.MINUTES);
        return fresh;
    }

    /**
     * Lets one instance go to the source, using setIfAbsent as a ticket.
     *
     * <p>{@code setIfAbsent} resolves serially on the leader, so exactly one instance in the
     * cluster gets the ticket. Those that do not wait a while and read again -- not waiting on
     * a lock, but for that instance to write the result in.
     */
    public String loadOnce(String userId) throws InterruptedException {
        String key = "user:" + userId;
        byte[] cached = cache.get(key);
        if (cached != null) {
            return new String(cached, StandardCharsets.UTF_8);
        }
        // The ticket carries a TTL of its own: if the instance holding it crashes, the ticket
        // has to expire by itself, or nothing ever goes to the source for this key again
        boolean gotTicket = cache.setIfAbsent("loading:" + key, new byte[]{1}, 30, TimeUnit.SECONDS);
        if (gotTicket) {
            try {
                String fresh = loadFromDatabase(userId);
                cache.set(key, fresh.getBytes(StandardCharsets.UTF_8), 10, TimeUnit.MINUTES);
                return fresh;
            } finally {
                cache.delete("loading:" + key);
            }
        }
        for (int i = 0; i < 20; i++) {
            Thread.sleep(50);
            byte[] v = cache.get(key);
            if (v != null) {
                return new String(v, StandardCharsets.UTF_8);
            }
        }
        // Give up waiting and query directly rather than hanging the request
        return loadFromDatabase(userId);
    }

    // ------------------------------------------------------------------
    // 2. Counting: one number across the cluster
    // ------------------------------------------------------------------

    /**
     * A counter. {@code incr} executes atomically on the leader, so five instances adding 200
     * each give <b>exactly</b> 1000 -- verified by measurement, no more and no less.
     *
     * <p>This is its greatest difference from "a local counter plus periodic aggregation": that
     * loses counts when an instance restarts, and the aggregation lags, so what you see is
     * always a few seconds old.
     *
     * <p>Rate limiting is the commonest use of this, and has a class of its own in
     * {@link RateLimitBestPractice}.
     */
    public long countPageView(String pageId) {
        return cache.incr("pv:" + pageId, 1);
    }

    /**
     * One counting key per day, cleaned up by TTL.
     *
     * <p>Building the date into the key name moves to a new key each day of its own accord --
     * <b>no cleanup task of any kind</b>. The pattern recurs throughout counting scenarios and
     * is worth remembering.
     *
     * <p>Note that {@code incr} <b>keeps any existing TTL</b>, as Redis does, so the TTL is set
     * only when the key first appears; setting it every time extends the key indefinitely and
     * it never expires.
     */
    public long countDailyActive(String userId) {
        String key = "dau:" + LocalDate.now();
        long count = cache.incr(key, 1);
        if (count == 1) {
            cache.expire(key, 2, TimeUnit.DAYS);
        }
        return count;
    }

    // ------------------------------------------------------------------
    // 3. Storing an object in a hash
    // ------------------------------------------------------------------

    /**
     * Configuration stored field by field, so <b>changing one field does not rewrite the whole
     * object</b>.
     *
     * <p>Compare: with the whole object serialised into one string, changing a single switch
     * means reading it, deserialising, changing, serialising and writing back -- and any step
     * of that racing with someone else overwrites their change. With a hash, each writer
     * changes its own field and nothing conflicts.
     */
    public void updateSetting(String module, String field, String value) {
        cache.hset("config:" + module, field, value.getBytes(StandardCharsets.UTF_8));
    }

    /** Reads a module's whole configuration. */
    public Map<String, byte[]> loadSettings(String module) {
        return cache.hgetAll("config:" + module);
    }

    /**
     * Counters on hash fields, for <b>statistics across several dimensions</b>.
     *
     * <pre>{@code
     * // One key holds every metric for an article
     * countBy("article:42", "view");
     * countBy("article:42", "like");
     * countBy("article:42", "share");
     * // one hgetAll brings all three numbers back
     * }</pre>
     *
     * <p>Against three separate keys -- {@code pv:42}, {@code like:42}, {@code share:42} --
     * this saves two round trips on a read and holds a third as many keys.
     */
    public long countBy(String subject, String dimension) {
        return cache.hincrby("stat:" + subject, dimension, 1);
    }

    /**
     * A list as a queue: {@code lpop} executes serially on the leader, so <b>exactly one
     * instance in the cluster gets any given element</b>.
     *
     * <p>Delays or priorities call for a sorted set rather than a list; see
     * {@link DelayedQueueBestPractice}.
     */
    public byte[] takeNextJob() {
        return cache.lpop("jobs:pending");
    }

    /** Submits a job. */
    public void submitJob(String payload) {
        cache.rpush("jobs:pending", payload.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * An event stream keeping only the most recent N entries.
     *
     * <p>{@code ltrim} follows {@code lpush} directly -- keep the two <b>adjacent</b>. With
     * other logic between them, an exception path can push without trimming, and the list grows
     * without bound.
     */
    public void recordEvent(String event) {
        cache.lpush("events:recent", event.getBytes(StandardCharsets.UTF_8));
        cache.ltrim("events:recent", 0, 99);
    }

    /** The most recent n events. */
    public List<byte[]> recentEvents(int n) {
        return cache.lrange("events:recent", 0, n - 1);
    }

    // ------------------------------------------------------------------
    // 5. What to do when a write fails
    // ------------------------------------------------------------------

    /**
     * A write throws {@link ProcessingCacheException}, of two kinds handled differently:
     *
     * <ul>
     *   <li><b>Misuse</b> -- lpush on a string key, incr on something that is not a number.
     *       Retrying does not help; the code is wrong.</li>
     *   <li><b>Temporarily unavailable</b> -- the leader is changing. The method has already
     *       retried for the whole {@code request-timeout-ms}, 8 seconds by default, which
     *       comfortably covers the 3.3 to 4.4 seconds a takeover was measured to take. Failing
     *       past that, the cluster really is in trouble.</li>
     * </ul>
     *
     * <p>A failed cache write usually <b>should not fail the application</b>: log a line, and
     * let the next read go to the source.
     */
    public void cacheQuietly(String key, String value) {
        try {
            cache.set(key, value.getBytes(StandardCharsets.UTF_8), 5, TimeUnit.MINUTES);
        } catch (ProcessingCacheException e) {
            // A failed write is let go; the next read goes to the source. Do not break a
            // request for the sake of writing a cache
            System.getLogger("cache").log(System.Logger.Level.WARNING,
                    "Cache write failed and was ignored: " + key + " - " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // 6. How not to use it
    // ------------------------------------------------------------------

    /**
     * A counter-example: <b>do not use it for anything needing strong consistency</b>.
     *
     * <pre>{@code
     * // Wrong: under a network partition each side has a leader deducting its own, and stock
     * // is oversold
     * long left = cache.incr("stock:" + itemId, -1);
     * if (left < 0) { throw new SoldOutException(); }
     * }</pre>
     *
     * Stock belongs in a database, or behind a CP lock with a fencing token. What this cache
     * can guarantee does not exceed the strength of "there is one leader", and across machines
     * that is only an agreement about timing.
     *
     * <p>Two more counter-examples:
     * <ul>
     *   <li><b>Written more than read</b> -- every write goes through the leader, so writing to
     *       a database directly is better</li>
     *   <li><b>Large values written frequently</b> -- every write is broadcast to every node,
     *       and the bandwidth is the value's size times the node count</li>
     * </ul>
     */
    public void antiPatterns() {
        // See the javadoc
    }

    private String loadFromDatabase(String userId) {
        return "user-data-of-" + userId;
    }
}
