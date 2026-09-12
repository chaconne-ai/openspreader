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

import com.chaconneai.spreader.Node;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An exchange point's record on the leader.
 *
 * <p>Mutable, with every method {@code synchronized}: "is anyone waiting, and if so pair
 * with them" has to be one atomic action. Two arrivals at once would otherwise each find the
 * slot empty and each park in it, one overwriting the other -- and the overwritten party's
 * item would be lost with nobody the wiser.
 *
 * <h2>One slot, not a queue</h2>
 * An exchange is a rendezvous of <b>two</b>. So the record holds a single waiting party: an
 * arrival either takes that slot, or finds it taken and pairs with whoever is in it. A third
 * arrival, coming after a pairing has emptied the slot, simply takes the slot itself and
 * waits for a fourth. That is the JDK's behaviour too -- {@code Exchanger} pairs arrivals
 * two at a time, in whatever order they come.
 *
 * <h2>Why pairing leaves something behind</h2>
 * The two sides are not symmetrical. The party that arrives <b>second</b> triggers the
 * pairing and takes its partner's item straight back in the reply to its own request, in one
 * round trip. The party that arrived <b>first</b> is asleep somewhere else in the cluster,
 * and its item has to be left here in {@link #results} until it wakes and collects.
 *
 * <p>That asymmetry is what makes a lost push survivable: the push only wakes the sleeper
 * sooner, and the item is on the leader either way.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class ExchangeEntry {

    /** The party occupying the slot, waiting for someone to pair with. */
    private record Waiting(String participantId, Node node, byte[] payload, long arrivedAtMs) {
    }

    /** An item paired and waiting to be collected by the party that arrived first. */
    record Result(byte[] payload, Node node, long pairedAtMs) {
    }

    private final String name;

    /** The one party currently waiting, or null when the exchange point is empty. */
    private Waiting waiting;

    /**
     * Participant -> the item waiting for it.
     *
     * <p>Ordinarily it holds at most one or two entries for a moment: a party is paired and
     * collects within a round trip. It grows only when a paired party never comes back -- its
     * process died between arriving and collecting -- and {@link #evictStaleResults} clears
     * those out.
     */
    private final Map<String, Result> results = new LinkedHashMap<>();

    private long exchanges;
    private long lastTouchedMs;

    ExchangeEntry(String name) {
        this.name = name;
        this.lastTouchedMs = System.currentTimeMillis();
    }

    /**
     * Arrives at the exchange point.
     *
     * @return the partner's item when this arrival completed a pair, or null when it took the
     *         slot and must now wait
     */
    synchronized Result arrive(String participantId, Node node, byte[] payload) {
        touch();
        if (waiting == null) {
            waiting = new Waiting(participantId, node, payload, lastTouchedMs);
            return null;
        }
        if (waiting.participantId().equals(participantId)) {
            // The same party is already in the slot. It is never two live calls at once -- a
            // thread is blocked inside exchange() and cannot call again -- so it is one of
            // two things, and replacing the slot is right for both:
            //
            //   a resend whose reply went missing, where the payload is the same bytes and
            //   replacing changes nothing;
            //
            //   a stale slot. A pool thread whose previous exchange timed out with its CANCEL
            //   lost on the way is still sitting here, and the participant id is node plus
            //   thread, so the next task on that same pool thread arrives under the same id.
            //   Leaving the old entry would hand the <b>previous</b> task's item to the next
            //   partner while this call waited out its own timeout.
            //
            // What must not happen is pairing the party with itself, which would hand it back
            // its own item and quietly lose the exchange
            waiting = new Waiting(participantId, node, payload, lastTouchedMs);
            return null;
        }
        Waiting partner = waiting;
        waiting = null;
        exchanges++;
        // The arriving party takes its partner's item away in the reply, in one round trip.
        // The partner is asleep elsewhere, so its item is left here to be collected
        results.put(partner.participantId(), new Result(payload, partner.node(), lastTouchedMs));
        return new Result(partner.payload(), partner.node(), lastTouchedMs);
    }

    /**
     * Takes the item left for a party that arrived first and then waited.
     *
     * @return the item, or null when no partner has arrived yet
     */
    synchronized Result collect(String participantId) {
        touch();
        return results.remove(participantId);
    }

    /** Whether this party is still the one occupying the slot. Distinguishes "keep waiting"
     *  from "the register no longer knows you". */
    synchronized boolean isWaiting(String participantId) {
        return waiting != null && waiting.participantId().equals(participantId);
    }

    /**
     * Stops waiting, after a timeout or an interrupt.
     *
     * <h2>A cancel may still come back with an item</h2>
     * A pairing can happen in the instant between a waiter deciding to give up and its cancel
     * reaching the leader. By then the <b>partner has already gone away with this party's
     * item</b>, and refusing the cancelling party its half would leave the exchange
     * half-completed with an item silently dropped.
     *
     * <p>So a result already set aside is handed back and the exchange counts as successful,
     * even though the caller had run out of time. Arriving a moment late is a far smaller
     * fault than losing someone's data.
     *
     * @return the item when the pairing had already happened, or null when the party was
     *         simply removed from the slot
     */
    synchronized Result cancel(String participantId) {
        touch();
        Result paired = results.remove(participantId);
        if (paired != null) {
            return paired;
        }
        if (waiting != null && waiting.participantId().equals(participantId)) {
            waiting = null;
        }
        return null;
    }

    /**
     * A node has departed.
     *
     * <p>Two things go with it: a party of its still occupying the slot can never come back to
     * complete a pairing, and any item set aside for one of its parties can never be
     * collected. Both are cleared here, so that the next arrival meets a clean exchange point
     * rather than pairing with a ghost and waiting out its whole timeout.
     *
     * @return whether anything was actually cleared
     */
    synchronized boolean onNodeLeft(String nodeId) {
        boolean changed = false;
        if (waiting != null && waiting.node().id().equals(nodeId)) {
            waiting = null;
            changed = true;
        }
        changed |= results.entrySet().removeIf(e -> e.getValue().node().id().equals(nodeId));
        return changed;
    }

    /**
     * Sweeps items nobody came back for.
     *
     * <p>Only reachable when a paired process died between arriving and collecting -- the
     * ordinary path removes a result within one round trip. Without the sweep, one such death
     * leaves an item pinned in the leader's memory for as long as the exchange point stays in
     * use.
     *
     * @return how many were swept
     */
    synchronized int evictStaleResults(long idleTimeoutMs) {
        long deadline = System.currentTimeMillis() - idleTimeoutMs;
        int before = results.size();
        results.entrySet().removeIf(e -> e.getValue().pairedAtMs() < deadline);
        return before - results.size();
    }

    /** The nodes with something outstanding here: the waiting party's, and those of any items
     *  not yet collected. */
    synchronized List<Node> involvedNodes() {
        List<Node> nodes = new ArrayList<>(results.size() + 1);
        if (waiting != null) {
            nodes.add(waiting.node());
        }
        results.values().forEach(r -> nodes.add(r.node()));
        return nodes;
    }

    public String name() {
        return name;
    }

    /** Whether someone is standing at this exchange point waiting for a partner. */
    public synchronized boolean hasWaiter() {
        return waiting != null;
    }

    /** How many pairings this exchange point has completed. */
    public synchronized long exchanges() {
        return exchanges;
    }

    public synchronized int pendingResults() {
        return results.size();
    }

    public long lastTouchedMs() {
        return lastTouchedMs;
    }

    void touch() {
        lastTouchedMs = System.currentTimeMillis();
    }

    synchronized String describe() {
        return "waiting=" + (waiting == null ? "none" : waiting.participantId())
                + ", uncollected=" + results.size() + ", exchanges=" + exchanges;
    }
}
