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
 * The exchange-point register, <b>meaningful only on the leader</b>.
 *
 * <p>Pairing happens here. Every arrival in the cluster converges on the leader, so the
 * decision "is anyone waiting" is taken in one place and two parties can never each believe
 * they arrived first.
 *
 * <h2>An exchange point needs no declaration</h2>
 * Unlike a latch's count or a barrier's parties, an exchange point carries <b>no agreed
 * parameter</b> -- it is always two, and always the next two to arrive. So there is nothing
 * for the sides to disagree about and no {@code declare} to refuse: the first arrival under
 * a name creates the record.
 *
 * <h2>A change of leader takes all of this with it</h2>
 * As with the lock register. For an exchanger the consequence is sharper than for a barrier:
 * a waiting party's <b>item</b> lived in the old leader's memory, so a change of leader
 * loses items that were handed over but not yet paired. Waiters find out through the cluster
 * events their own node receives and leave with {@link ProcessingExchangerException}, so the
 * caller still holds its item and can try again. What cannot be recovered is an exchange
 * that had already paired but not yet been collected; see {@code ExchangerService}.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class ExchangerRegistry {

    /** Exchange point name -> record. */
    private final Map<String, ExchangeEntry> exchangers = new ConcurrentHashMap<>();

    /** Gets an exchange point, creating it on first use. */
    public ExchangeEntry get(String name) {
        return exchangers.computeIfAbsent(name, ExchangeEntry::new);
    }

    /**
     * A node has departed: its waiting parties can never complete a pairing, and items set
     * aside for them can never be collected.
     *
     * @return the names of the exchange points that were affected
     */
    public List<String> onNodeLeft(String nodeId) {
        List<String> affected = new ArrayList<>();
        exchangers.forEach((name, entry) -> {
            if (entry.onNodeLeft(nodeId)) {
                affected.add(name);
            }
        });
        return affected;
    }

    /**
     * Sweeps records nobody has touched for a long time, and items nobody came back for
     * inside records still in use.
     *
     * <p>Both halves are needed. Sweeping whole records alone would miss the dangerous case:
     * a busy exchange point is touched constantly and never falls idle, so an item left by a
     * process that died would stay pinned in memory indefinitely.
     *
     * @return how many records were dropped
     */
    public int evictIdle(long idleTimeoutMs) {
        long deadline = System.currentTimeMillis() - idleTimeoutMs;
        int before = exchangers.size();
        exchangers.entrySet().removeIf(e -> e.getValue().lastTouchedMs() < deadline);
        exchangers.values().forEach(e -> e.evictStaleResults(idleTimeoutMs));
        return before - exchangers.size();
    }

    public void clear() {
        exchangers.clear();
    }

    public int size() {
        return exchangers.size();
    }

    /** How many parties are standing at an exchange point waiting for a partner, across every
     *  name. */
    public int waiterCount() {
        int n = 0;
        for (ExchangeEntry entry : exchangers.values()) {
            if (entry.hasWaiter()) {
                n++;
            }
        }
        return n;
    }

    /** For diagnostics. */
    public Map<String, String> snapshot() {
        Map<String, String> out = new LinkedHashMap<>();
        exchangers.forEach((k, v) -> out.put(k, v.describe()));
        return out;
    }
}
