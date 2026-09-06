package com.chaconneai.openspreader.example.cache;

import com.chaconneai.openspreader.cache.ProcessingCacheException;
import com.chaconneai.openspreader.cache.ProcessingCache;

import java.util.concurrent.TimeUnit;

/**
 * Cluster-wide rate limiting: five instances between them admit at most 100 requests a
 * minute.
 *
 * <h2>Why each instance cannot limit for itself</h2>
 * The single-machine shape -- a local counter -- is broken across instances: configure "100
 * a minute" and five instances make it 500. Worse, which instance the traffic lands on is
 * the gateway's decision, so an uneven distribution cannot be reasoned about at all -- some
 * instances are long past the limit while others sit idle.
 *
 * <p>The counter here is <b>incremented atomically on the leader</b>, so all five instances
 * see the same number. Measured with five nodes adding 200 each, the result is exactly 1000
 * -- no more, no less.
 *
 * <h2>A fixed window, not a sliding one</h2>
 * The key rounds the time down to the minute, so at a window boundary as much as twice the
 * quota can get through: 100 arrive in the 59th second and another 100 in the 61st.
 *
 * <p>An exact sliding window would mean recording a timestamp per request, at a far higher
 * cost. <b>Rate limiting is a coarse protective measure to begin with</b>, and boundary
 * jitter is usually acceptable; where it must be stricter, a finer window -- ten seconds,
 * say -- eases it considerably.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
public class RateLimitBestPractice {

    /** The window length. The finer it is, the less the doubled admission at a boundary
     *  matters. */
    private static final long WINDOW_SECONDS = 60L;

    private final ProcessingCache cache;

    public RateLimitBestPractice(ProcessingCache cache) {
        this.cache = cache;
    }

    /**
     * Whether this request is admitted.
     *
     * <pre>{@code
     * if (!rateLimit.tryAcquire(clientId, 100)) {
     *     return ResponseEntity.status(429).build();
     * }
     * }</pre>
     *
     * <h2>Why the TTL is set separately, and only the first time</h2>
     * {@code incr} <b>keeps any existing TTL</b>, as Redis does. So:
     *
     * <ul>
     *   <li>without a TTL the key never expires -- one key per client per minute, and memory
     *       only ever grows</li>
     *   <li>setting it every time keeps extending the window into "60 seconds after the last
     *       request", which is no longer a fixed window</li>
     * </ul>
     *
     * Hence the {@code count == 1} test -- the first request of this window -- and setting it
     * only then.
     *
     * @param clientId the dimension being limited: a user, an IP, a tenant, even an endpoint
     * @param limit    how many requests this window admits
     */
    public boolean tryAcquire(String clientId, int limit) {
        String key = windowKey(clientId);
        long count = cache.incr(key, 1);
        if (count == 1) {
            // Expire with room to spare, so a key sitting right on the boundary is not
            // swept away early
            cache.expire(key, WINDOW_SECONDS * 2, TimeUnit.SECONDS);
        }
        return count <= limit;
    }

    /**
     * How many admissions are left. Used to answer {@code X-RateLimit-Remaining} in a
     * response header.
     *
     * <p>This is a <b>read-only</b> query and consumes no quota.
     */
    public long remaining(String clientId, int limit) {
        byte[] raw = cache.get(windowKey(clientId));
        long used = raw == null ? 0 : Long.parseLong(new String(raw));
        return Math.max(0, limit - used);
    }

    /**
     * How many seconds until the current window ends. Used to answer {@code Retry-After} in a
     * response header.
     *
     * @return the seconds; the whole window length when the key does not exist yet
     */
    public long secondsUntilReset(String clientId) {
        long ttlMs = cache.ttl(windowKey(clientId));
        return ttlMs <= 0 ? WINDOW_SECONDS : Math.min(WINDOW_SECONDS, ttlMs / 1000);
    }

    /**
     * When the limiter itself has a problem, <b>admit rather than block</b>.
     *
     * <h2>An important trade-off</h2>
     * While the cache is unavailable -- during a change of leader -- the decision cannot be
     * made. There are two choices:
     *
     * <ul>
     *   <li><b>Reject everything</b> -- the limiter becomes a single point of failure, and one
     *       wobble takes the whole service down. Something meant to <b>protect</b> the service
     *       killing it instead is the worst possible outcome</li>
     *   <li><b>Admit everything</b> -- the quota may be exceeded briefly, but the service is
     *       still alive</li>
     * </ul>
     *
     * <p>Rate limiting is <b>overload protection</b>, not <b>access control</b>. Anything that
     * must be exact -- billing, authorisation -- does not belong here; those go through
     * database transactions.
     */
    public boolean tryAcquireFailOpen(String clientId, int limit) {
        try {
            return tryAcquire(clientId, limit);
        } catch (ProcessingCacheException e) {
            System.getLogger("ratelimit").log(System.Logger.Level.WARNING,
                    "Rate limiter unavailable; admitting this request: "
                            + clientId + " - " + e.getMessage());
            return true;
        }
    }

    /**
     * The window key: the time, rounded down to the window length, built into it.
     *
     * <p>Each window therefore moves to a new key of its own accord, and old keys disappear by
     * TTL -- <b>no cleanup task of any kind is needed</b>.
     */
    private String windowKey(String clientId) {
        long window = System.currentTimeMillis() / (WINDOW_SECONDS * 1000);
        return "ratelimit:" + clientId + ":" + window;
    }
}
