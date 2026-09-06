package com.chaconneai.openspreader.sync;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The latch register, <b>meaningful only on the leader</b>.
 *
 * <p>The counting down happens here. Every {@code countDown} converges on the leader and is
 * handled serially, so two processes decrementing at once can never lose a decrement.
 *
 * <h2>A change of leader takes all of this with it</h2>
 * As with the lock register, it lives only in memory. But the consequence is <b>worse than
 * for a lock</b>: a lost lock is merely contended for again, whereas a lost latch count
 * returns to its initial value, the count-downs already made are wasted, and waiters may
 * never reach zero.
 *
 * <p>Which is why counting <b>de-duplicated per participant</b> is offered:
 * {@code countDown(name, participantId)} takes effect once per participantId. After a change
 * of leader each participant counts down again and the count returns to its correct value.
 * That is the only genuinely usable form in a distributed setting; the plain counting version
 * cannot recover from a change of leader.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class LatchRegistry {

    /** Latch name -> record. */
    private final Map<String, LatchEntry> latches = new ConcurrentHashMap<>();

    /**
     * Declares a latch.
     *
     * <p>The first arrival fixes the initial count, and anyone arriving afterwards with a
     * different number is refused: everyone under one name must agree on how many to wait
     * for, or nobody can say when it is finished.
     *
     * @return why it failed, or null on success
     */
    public String declare(String name, long initialCount) {
        if (initialCount < 0) {
            return "the initial count must not be negative: " + initialCount;
        }
        String[] error = new String[1];
        latches.compute(name, (k, current) -> {
            if (current == null) {
                return new LatchEntry(name, initialCount);
            }
            if (current.initialCount() != initialCount) {
                error[0] = "latch " + name + " already has an initial count of "
                        + current.initialCount() + " and cannot be changed to " + initialCount;
            }
            current.touch();
            return current;
        });
        return error[0];
    }

    /**
     * Counts down by one.
     *
     * @param participantId the participant's identifier. Empty means plain counting, one
     *                      decrement per call; non-empty de-duplicates by it, so one
     *                      participant counting down repeatedly decrements once
     * @return the count remaining afterwards, or -1 when no such latch exists
     */
    public long countDown(String name, String participantId) {
        long[] remaining = {-1L};
        latches.computeIfPresent(name, (k, entry) -> {
            remaining[0] = entry.countDown(participantId);
            return entry;
        });
        return remaining[0];
    }

    /** Registers a waiter, so the leader knows whom to push to once the condition is met. */
    public void addWaiter(String name, String nodeId) {
        LatchEntry entry = latches.get(name);
        if (entry != null) {
            entry.waiters().add(nodeId);
            entry.touch();
        }
    }

    public void removeWaiter(String name, String nodeId) {
        LatchEntry entry = latches.get(name);
        if (entry != null) {
            entry.waiters().remove(nodeId);
        }
    }

    /** @return which nodes to push to; empty when no such latch exists */
    public Set<String> waiters(String name) {
        LatchEntry entry = latches.get(name);
        return entry == null ? Set.of() : Set.copyOf(entry.waiters());
    }

    public LatchEntry get(String name) {
        return latches.get(name);
    }

    /**
     * A node has departed; removes the waits it registered.
     *
     * <p><b>The count is left alone</b>: the count-downs it made are facts that have already
     * happened and cannot be undone because it departed -- that would leave the remaining
     * participants never reaching zero. This is the opposite of a lock, where a departing
     * holder must have its lock released.
     */
    public void onNodeLeft(String nodeId) {
        latches.values().forEach(e -> e.waiters().remove(nodeId));
    }

    /** Sweeps records nobody has touched for a long time. A latch is one-shot, and without
     *  sweeping them the leader's memory grows without bound. */
    public int evictIdle(long idleTimeoutMs) {
        long deadline = System.currentTimeMillis() - idleTimeoutMs;
        int before = latches.size();
        latches.entrySet().removeIf(e -> e.getValue().lastTouchedMs() < deadline);
        return before - latches.size();
    }

    public void remove(String name) {
        latches.remove(name);
    }

    public void clear() {
        latches.clear();
    }

    public int size() {
        return latches.size();
    }

    /** For diagnostics. */
    public Map<String, String> snapshot() {
        Map<String, String> out = new LinkedHashMap<>();
        latches.forEach((k, v) -> out.put(k, v.describe()));
        return out;
    }

    /** Every latch name currently held. */
    public List<String> names() {
        return new ArrayList<>(latches.keySet());
    }
}
