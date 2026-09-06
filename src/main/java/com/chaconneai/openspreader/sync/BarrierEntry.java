package com.chaconneai.openspreader.sync;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * A barrier's record on the leader.
 *
 * <p>Mutable, with every method {@code synchronized}: "arrive and decide whether it is full"
 * has to be one atomic action, or with parties requests arriving at once two of them could
 * each believe they were the last and release the barrier twice.
 *
 * <h2>Generations</h2>
 * As in the JDK's {@code CyclicBarrier}: filling it once moves to a new generation, clears
 * the arrivals, and the next round can proceed. A waiter polls carrying the generation it
 * saw on entry, and finding <b>the current generation higher than its own</b> means this
 * round has already been released.
 *
 * <p>A generation rather than a boolean flag, because there is no gap at all between
 * releasing and waiting again -- with a flag, the fastest party could enter the next round
 * and clear it before the others had even woken.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class BarrierEntry {

    private final String name;
    private final int parties;

    private long generation;

    /** Which participants have arrived this generation. A participant is a node id plus a
     *  thread id, so one thread counts as one. */
    private final Set<String> arrived = new LinkedHashSet<>();

    /** Participant -> the node id it sits on, for pushing notifications and handling departures. */
    private final Map<String, String> participantNodes = new LinkedHashMap<>();

    /** Whether this generation is broken, and why. */
    private boolean broken;
    private String brokenReason = "";

    private long lastTouchedMs;

    BarrierEntry(String name, int parties) {
        this.name = name;
        this.parties = parties;
        this.lastTouchedMs = System.currentTimeMillis();
    }

    /**
     * Arrives at the barrier.
     *
     * @return the arrival index: {@code parties - 1} means this party arrived last and its
     *         arrival releases the round. Returns -1 when the barrier is already broken
     */
    synchronized long arrive(String participantId, String nodeId) {
        touch();
        if (broken) {
            return -1L;
        }
        arrived.add(participantId);
        participantNodes.put(participantId, nodeId);
        long index = arrived.size() - 1L;
        if (arrived.size() >= parties) {
            // Full: move to a new generation, clear the arrivals, and the next round can proceed
            generation++;
            arrived.clear();
            participantNodes.clear();
            return parties - 1L;
        }
        return index;
    }

    /**
     * Breaks this generation of the barrier.
     *
     * <p>How this is reached: a party timed out, was interrupted, or the node it sits on
     * departed. By the JDK's semantics, every other party <b>must be failed immediately</b>
     * -- the fullness they are waiting for will never happen, and waiting on means hanging
     * forever.
     */
    synchronized void breakBarrier(String reason) {
        touch();
        if (!broken) {
            broken = true;
            brokenReason = reason == null ? "" : reason;
        }
    }

    /**
     * Resets: discards this generation and starts afresh.
     *
     * <p>Moving the generation number is mandatory -- without it, those still waiting on the
     * old generation would take the new one for their own.
     */
    synchronized void reset() {
        touch();
        generation++;
        arrived.clear();
        participantNodes.clear();
        broken = false;
        brokenReason = "";
    }

    /**
     * A node has departed.
     *
     * <p>No participant still waiting on it can ever arrive, so the barrier is broken
     * outright. This differs from a latch: a departing latch participant does not undo the
     * counts already made, whereas a barrier requires everyone to be present <b>at once</b>,
     * and one absence ends the round.
     *
     * @return whether this actually broke it
     */
    synchronized boolean onNodeLeft(String nodeId) {
        if (broken || !participantNodes.containsValue(nodeId)) {
            return false;
        }
        breakBarrier("the node a participant was on, " + nodeId + ", has departed");
        return true;
    }

    public String name() {
        return name;
    }

    public int parties() {
        return parties;
    }

    public synchronized long generation() {
        return generation;
    }

    public synchronized int arrivedCount() {
        return arrived.size();
    }

    public synchronized boolean isBroken() {
        return broken;
    }

    public synchronized String brokenReason() {
        return brokenReason;
    }

    synchronized Set<String> waiterNodes() {
        return Set.copyOf(participantNodes.values());
    }

    public long lastTouchedMs() {
        return lastTouchedMs;
    }

    void touch() {
        lastTouchedMs = System.currentTimeMillis();
    }

    synchronized String describe() {
        return "generation=" + generation + ", arrived=" + arrived.size() + "/" + parties
                + (broken ? ", broken(" + brokenReason + ")" : "");
    }
}
