package com.chaconneai.openspreader.sync;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The lock register, <b>meaningful only on the leader</b>.
 *
 * <p>Mutual exclusion happens here: one lock name has one record at a time, and whoever
 * registers first holds it. Every request converges on the leader and is decided serially, so
 * two parties can never both acquire it.
 *
 * <h2>How a lock is released</h2>
 * <ol>
 *   <li><b>Released normally</b> -- the holder sends a RELEASE</li>
 *   <li><b>The holder departs</b> -- the leader receives the departure event and releases
 *       every lock it held, without waiting for the lease</li>
 *   <li><b>The lease expires</b> -- the safety net. Where a holder's process has hung, or the
 *       network is cut without it being declared departed, the lease lapses on its own. A
 *       holder renews periodically to keep it</li>
 * </ol>
 *
 * <h2>A change of leader takes all of this with it</h2>
 * The register lives in memory alone, neither replicated nor persisted. When a new leader
 * comes up after the old one dies, the table is empty and every lock counts as released. It
 * is a deliberate trade; {@code MutexService} documents what it costs.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class LockRegistry {

    /** Lock name -> the holding record. */
    private final Map<String, LockEntry> locks = new ConcurrentHashMap<>();

    /**
     * Requests the lock.
     *
     * @param lockName the lock's full name, granularity prefix included
     * @param ownerId  the requester's node id
     * @param value    an identifier written by the requester, shown by
     *                 {@code currentOccupied()}
     * @param leaseMs  the lease duration
     * @return null when the lock was acquired; otherwise the current holder's record
     */
    public LockEntry tryAcquire(String lockName, String ownerId, String value, long leaseMs) {
        long now = System.currentTimeMillis();
        // compute makes the check and the write atomic for one lock name, so several
        // concurrent requests still leave exactly one winner
        LockEntry[] blocker = new LockEntry[1];
        locks.compute(lockName, (k, current) -> {
            if (current != null && !current.isExpired(now) && !current.ownerId().equals(ownerId)) {
                blocker[0] = current;
                return current;
            }
            // Unheld, expired, or already held by this very owner (a renewal) all count as acquired
            return new LockEntry(lockName, ownerId, value, now + leaseMs);
        });
        return blocker[0];
    }

    /**
     * The placeholder holder id meaning "this lock is cooling down".
     *
     * <p>No real node's id can equal it, so during the cooldown <b>nobody can take the
     * lock</b> -- not even the node that just released it.
     */
    private static final String COOLDOWN_OWNER = "__cooldown__";

    /**
     * Releases the lock.
     *
     * @return true when this holder really did release it; false when the lock was not in its
     *         hands -- it may long since have expired, or been taken by someone else
     */
    public boolean release(String lockName, String ownerId) {
        return release(lockName, ownerId, 0L);
    }

    /**
     * Releases the lock and makes it unobtainable by anyone for a while afterwards.
     *
     * <p>This is exactly what a scheduled task needs: the task body may finish in a few
     * milliseconds, and releasing at once lets another instance take the lock immediately, so
     * the same cycle runs several times. Holding it down for a while is what makes it <b>run
     * once per scheduling period</b>.
     *
     * @param cooldownMs the cooldown; 0 or negative releases it at once
     */
    public boolean release(String lockName, String ownerId, long cooldownMs) {
        boolean[] released = new boolean[1];
        locks.computeIfPresent(lockName, (k, current) -> {
            if (!current.ownerId().equals(ownerId)) {
                return current;
            }
            released[0] = true;
            if (cooldownMs <= 0) {
                return null;
            }
            // Swapped for a holder nobody matches, which simply expires when its time comes,
            // so no extra cleanup is needed. The value is left as it was, so a query can still
            // show who ran the previous round
            return new LockEntry(lockName, COOLDOWN_OWNER, current.value(),
                    System.currentTimeMillis() + cooldownMs);
        });
        return released[0];
    }

    /** Whether the lock is cooling down -- just run, and not yet due for the next round. */
    public boolean isCoolingDown(String lockName) {
        LockEntry entry = current(lockName);
        return entry != null && COOLDOWN_OWNER.equals(entry.ownerId());
    }

    /**
     * Renews the lease.
     *
     * @return true when the renewal succeeded; false when the lock is no longer in this
     *         holder's hands, and the holder should consider it lost
     */
    public boolean renew(String lockName, String ownerId, long leaseMs) {
        long now = System.currentTimeMillis();
        boolean[] renewed = new boolean[1];
        locks.computeIfPresent(lockName, (k, current) -> {
            if (current.ownerId().equals(ownerId) && !current.isExpired(now)) {
                renewed[0] = true;
                return current.withExpiry(now + leaseMs);
            }
            return current;
        });
        return renewed[0];
    }

    /** @return the current holder, or null when nobody holds it or it has expired */
    public LockEntry current(String lockName) {
        LockEntry entry = locks.get(lockName);
        return entry == null || entry.isExpired(System.currentTimeMillis()) ? null : entry;
    }

    /**
     * Releases every lock a node holds. The leader calls it when a member departs, so the
     * locks become available at once rather than idling out a whole lease.
     *
     * @return the names of the locks released
     */
    public List<String> releaseAllOf(String ownerId) {
        List<String> released = new ArrayList<>();
        for (Iterator<Map.Entry<String, LockEntry>> it = locks.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, LockEntry> e = it.next();
            if (e.getValue().ownerId().equals(ownerId)) {
                released.add(e.getKey());
                it.remove();
            }
        }
        return released;
    }

    /** Clears expired entries, called periodically. Skipping it costs no correctness, only
     *  memory. */
    public int evictExpired() {
        long now = System.currentTimeMillis();
        int before = locks.size();
        locks.entrySet().removeIf(e -> e.getValue().isExpired(now));
        return before - locks.size();
    }

    public void clear() {
        locks.clear();
    }

    public int size() {
        return locks.size();
    }

    /** Every current holding, for troubleshooting. */
    public Map<String, LockEntry> snapshot() {
        return Map.copyOf(locks);
    }
}
