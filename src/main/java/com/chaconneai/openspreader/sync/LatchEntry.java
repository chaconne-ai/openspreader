package com.chaconneai.openspreader.sync;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A latch's record on the leader.
 *
 * <p>Mutable. Every change is driven serially by {@link LatchRegistry} inside
 * {@code compute}, or is a concurrency-safe collection operation.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class LatchEntry {

    private final String name;
    private final long initialCount;

    /** How many are still outstanding. Reaching 0 means satisfied; it never goes negative. */
    private volatile long remaining;

    /**
     * The participants that have already counted down.
     *
     * <p>This is what makes recovery after a change of leader possible: the count returns to
     * its initial value on the new leader, each participant counts down again, and
     * de-duplicating through this set arrives back at the correct remainder. Plain counting,
     * with an empty participantId, never enters this set and therefore cannot recover.
     */
    private final Set<String> countedDown = ConcurrentHashMap.newKeySet();

    /** Who is waiting; these are the nodes pushed to once the condition is met. */
    private final Set<String> waiters = ConcurrentHashMap.newKeySet();

    private volatile long lastTouchedMs;

    LatchEntry(String name, long initialCount) {
        this.name = name;
        this.initialCount = initialCount;
        this.remaining = initialCount;
        this.lastTouchedMs = System.currentTimeMillis();
    }

    /** @return the count remaining after the decrement */
    synchronized long countDown(String participantId) {
        touch();
        if (participantId != null && !participantId.isBlank()) {
            if (!countedDown.add(participantId)) {
                // The same participant counting down again does not count twice. Both a retry
                // and a recount after a change of leader arrive here
                return remaining;
            }
        }
        if (remaining > 0) {
            remaining--;
        }
        return remaining;
    }

    public String name() {
        return name;
    }

    public long initialCount() {
        return initialCount;
    }

    public long remaining() {
        return remaining;
    }

    public boolean isSatisfied() {
        return remaining <= 0;
    }

    Set<String> waiters() {
        return waiters;
    }

    public int waiterCount() {
        return waiters.size();
    }

    public long lastTouchedMs() {
        return lastTouchedMs;
    }

    void touch() {
        lastTouchedMs = System.currentTimeMillis();
    }

    String describe() {
        return "remaining=" + remaining + "/" + initialCount
                + ", countedDown=" + countedDown.size()
                + ", waiters=" + waiters.size();
    }
}
