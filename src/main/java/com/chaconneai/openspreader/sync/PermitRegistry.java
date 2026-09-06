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
package com.chaconneai.openspreader.sync;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The permit register, <b>meaningful only on the leader</b>.
 *
 * <p>It occupies the same position as {@link LockRegistry}, differing in that a lock records
 * only who holds it while a semaphore records how many each holder has taken. So one record
 * carries a {@code holder -> count} table, and whether another permit can be issued is
 * decided by that table's sum.
 *
 * <h2>Who fixes the permit total</h2>
 * The {@code permits} brought by the first requester is this semaphore's total, and anyone
 * arriving afterwards with a <b>different</b> number is refused outright, with the reason in
 * the response. An inconsistency like that is a misconfiguration -- silently running with one
 * party's number would only make "how many are actually allowed through" a mystery.
 *
 * <h2>How permits are released</h2>
 * Exactly as with locks: returned deliberately, released when the holder departs (the leader
 * clears all of its permits on receiving the event), or expired with the lease. A change of
 * leader takes this table with it.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class PermitRegistry {

    /** Semaphore name -> entry. */
    private final Map<String, Entry> semaphores = new ConcurrentHashMap<>();

    /**
     * Requests one permit.
     *
     * @return the outcome, carrying how many permits remain
     */
    public Result tryAcquire(String name, String ownerId, String value, int permits, long leaseMs) {
        if (permits <= 0) {
            return new Result(false, 0, "a semaphore needs more than 0 permits");
        }
        long now = System.currentTimeMillis();
        Result[] out = new Result[1];

        // compute makes "work out what is left" and "record it" atomic, so two concurrent
        // requests cannot both succeed
        semaphores.compute(name, (k, entry) -> {
            if (entry == null) {
                entry = new Entry(permits);
            } else if (entry.permits != permits) {
                out[0] = new Result(false, entry.available(now),
                        "permit total mismatch: this semaphore was registered with "
                                + entry.permits + ", but this request carries " + permits);
                return entry;
            }
            entry.evictExpired(now);
            if (entry.used() >= entry.permits) {
                out[0] = new Result(false, 0, "no permits are left");
                return entry;
            }
            Holder holder = entry.holders.get(ownerId);
            if (holder == null) {
                entry.holders.put(ownerId, new Holder(value, 1, now + leaseMs));
            } else {
                holder.count++;
                holder.expiryMs = now + leaseMs;
            }
            out[0] = new Result(true, entry.available(now), "");
            return entry;
        });
        return out[0];
    }

    /**
     * Returns one permit.
     *
     * @return whether one was actually returned; false when none was held
     */
    public Result release(String name, String ownerId) {
        long now = System.currentTimeMillis();
        Result[] out = new Result[1];
        out[0] = new Result(false, 0, "no permit is held");

        semaphores.computeIfPresent(name, (k, entry) -> {
            Holder holder = entry.holders.get(ownerId);
            if (holder == null) {
                out[0] = new Result(false, entry.available(now), "no permit is held");
                return entry;
            }
            holder.count--;
            if (holder.count <= 0) {
                entry.holders.remove(ownerId);
            }
            out[0] = new Result(true, entry.available(now), "");
            // The entry stays even with no holders: the permit total is a property of this
            // semaphore and the next request is validated against it
            return entry;
        });
        return out[0];
    }

    /**
     * Renews: pushes the expiry of every permit held by this holder further out.
     *
     * @return whether the renewal succeeded; false means it holds no permits any more
     */
    public boolean renew(String name, String ownerId, long leaseMs) {
        long now = System.currentTimeMillis();
        boolean[] renewed = new boolean[1];
        semaphores.computeIfPresent(name, (k, entry) -> {
            Holder holder = entry.holders.get(ownerId);
            if (holder != null && holder.expiryMs > now) {
                holder.expiryMs = now + leaseMs;
                renewed[0] = true;
            }
            return entry;
        });
        return renewed[0];
    }

    /** @return how many permits remain available, or -1 when this semaphore has never been registered */
    public int available(String name) {
        Entry entry = semaphores.get(name);
        return entry == null ? -1 : entry.available(System.currentTimeMillis());
    }

    /**
     * Releases every permit held by a node. Called by the leader when a member departs.
     *
     * @return the semaphores affected and how many permits each released
     */
    public Map<String, Integer> releaseAllOf(String ownerId) {
        Map<String, Integer> released = new LinkedHashMap<>();
        for (Map.Entry<String, Entry> e : semaphores.entrySet()) {
            Entry entry = e.getValue();
            Holder holder = entry.holders.remove(ownerId);
            if (holder != null && holder.count > 0) {
                released.put(e.getKey(), holder.count);
            }
        }
        return released;
    }

    /** Sweeps expired holdings; called periodically. */
    public int evictExpired() {
        long now = System.currentTimeMillis();
        int evicted = 0;
        for (Entry entry : semaphores.values()) {
            evicted += entry.evictExpired(now);
        }
        return evicted;
    }

    /** How many semaphores are registered. Consistent with the lock, latch and barrier
     *  registers, for diagnostics. */
    public int size() {
        return semaphores.size();
    }

    public void clear() {
        semaphores.clear();
    }

    /** Everything currently registered, for diagnostics. */
    public Map<String, Snapshot> snapshot() {
        long now = System.currentTimeMillis();
        Map<String, Snapshot> result = new LinkedHashMap<>();
        for (Map.Entry<String, Entry> e : semaphores.entrySet()) {
            Entry entry = e.getValue();
            Map<String, Integer> holders = new LinkedHashMap<>();
            entry.holders.forEach((id, h) -> holders.put(h.value, h.count));
            result.put(e.getKey(), new Snapshot(entry.permits, entry.available(now), holders));
        }
        return result;
    }

    /** The outcome of a request or a return. */
    public record Result(boolean success, int available, String message) {
    }

    /** An outward-facing snapshot of one semaphore. */
    public record Snapshot(int permits, int available, Map<String, Integer> holders) {
    }

    /** One semaphore's register entry. Every change happens inside {@code compute}, so the
     *  internal fields need no locking. */
    private static final class Entry {
        final int permits;
        final Map<String, Holder> holders = new LinkedHashMap<>();

        Entry(int permits) {
            this.permits = permits;
        }

        int used() {
            int sum = 0;
            for (Holder h : holders.values()) {
                sum += h.count;
            }
            return sum;
        }

        int available(long now) {
            evictExpired(now);
            return Math.max(0, permits - used());
        }

        int evictExpired(long now) {
            List<String> expired = new ArrayList<>();
            holders.forEach((id, h) -> {
                if (h.expiryMs <= now) {
                    expired.add(id);
                }
            });
            expired.forEach(holders::remove);
            return expired.size();
        }
    }

    /** How many permits one holder has taken. */
    private static final class Holder {
        final String value;
        int count;
        long expiryMs;

        Holder(String value, int count, long expiryMs) {
            this.value = value;
            this.count = count;
            this.expiryMs = expiryMs;
        }
    }
}
