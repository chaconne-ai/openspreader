package com.chaconneai.openspreader.sync;

/**
 * One record of a held lock.
 *
 * @param lockName the lock's full name
 * @param ownerId  the holder's node id -- spreader's {@code Node.id()}, unique per process
 * @param value    an identifier written by the holder, defined by the application, so that
 *                 "who holds it right now" can be inspected
 * @param expiryMs when the lease expires
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public record LockEntry(String lockName, String ownerId, String value, long expiryMs) {

    public boolean isExpired(long now) {
        return now >= expiryMs;
    }

    public LockEntry withExpiry(long newExpiryMs) {
        return new LockEntry(lockName, ownerId, value, newExpiryMs);
    }
}
