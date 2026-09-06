package com.chaconneai.openspreader.sync;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The barrier register, <b>meaningful only on the leader</b>.
 *
 * <p>The "is it full yet" decision happens here. Every arrival converges on the leader and
 * is handled serially, so two processes can never both believe they are the last and
 * release the barrier twice.
 *
 * <h2>A change of leader takes all of this with it</h2>
 * As with the lock register. For a barrier the consequence is that recorded arrivals reset
 * to zero, and waiters see the generation change and <b>leave with an error</b>
 * ({@code BrokenBarrierException}) rather than go on waiting for a count that will never be
 * reached. Callers should be ready to run the round again.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class BarrierRegistry {

    /** Barrier name -> record. */
    private final Map<String, BarrierEntry> barriers = new ConcurrentHashMap<>();

    /**
     * Gets a barrier, creating it with the given parties when it does not exist.
     *
     * <p>The first arrival fixes parties, and anyone arriving afterwards with a different
     * number is refused outright: everyone under one name must agree on how many make it
     * full.
     *
     * @return why it failed, or null on success
     */
    public String declare(String name, int parties) {
        if (parties <= 0) {
            return "a barrier needs more than 0 parties: " + parties;
        }
        String[] error = new String[1];
        barriers.compute(name, (k, current) -> {
            if (current == null) {
                return new BarrierEntry(name, parties);
            }
            if (current.parties() != parties) {
                error[0] = "barrier " + name + " already has " + current.parties()
                        + " parties and cannot be changed to " + parties;
            }
            current.touch();
            return current;
        });
        return error[0];
    }

    public BarrierEntry get(String name) {
        return barriers.get(name);
    }

    /**
     * A node has departed: no participant still waiting on it can ever arrive, so every
     * barrier involved is broken.
     *
     * @return the names of the barriers that were broken
     */
    public List<String> onNodeLeft(String nodeId) {
        List<String> broken = new ArrayList<>();
        barriers.forEach((name, entry) -> {
            if (entry.onNodeLeft(nodeId)) {
                broken.add(name);
            }
        });
        return broken;
    }

    /** Sweeps records nobody has touched for a long time, to save memory on the leader. */
    public int evictIdle(long idleTimeoutMs) {
        long deadline = System.currentTimeMillis() - idleTimeoutMs;
        int before = barriers.size();
        barriers.entrySet().removeIf(e -> e.getValue().lastTouchedMs() < deadline);
        return before - barriers.size();
    }

    public void clear() {
        barriers.clear();
    }

    public int size() {
        return barriers.size();
    }

    /** For diagnostics. */
    public Map<String, String> snapshot() {
        Map<String, String> out = new LinkedHashMap<>();
        barriers.forEach((k, v) -> out.put(k, v.describe()));
        return out;
    }
}
