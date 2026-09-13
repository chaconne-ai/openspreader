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
package com.chaconneai.openspreader.dag;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The reducers worth having ready-made.
 *
 * <p>Each one handles a null {@code current}, which is the first write to a channel. That
 * single null check is the reason this class exists: written out at every call site it
 * would be forgotten exactly once, and the symptom would be a NullPointerException from
 * inside the engine on the first run of a new graph.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class Reducers {

    private Reducers() {
    }

    /**
     * The last write wins.
     *
     * <p><b>Fine for a channel one branch writes; a trap for a channel two parallel
     * branches write.</b> "Last" is decided by node-name order, so it is at least
     * deterministic, but it still means one branch's work is discarded on purpose. Declare
     * it only when discarding really is what is wanted.
     */
    public static <T> Reducer<T> lastWins() {
        return Reducer.named("lastWins", (current, incoming) -> incoming);
    }

    /** The first write wins; later ones are ignored. Suits a channel that records how
     *  something started. */
    public static <T> Reducer<T> firstWins() {
        return Reducer.named("firstWins",
                (current, incoming) -> current == null ? incoming : current);
    }

    /** Appends. The natural choice for a fan-in that gathers what each branch produced. */
    public static <E> Reducer<List<E>> concatList() {
        return Reducer.named("concatList", (current, incoming) -> {
            List<E> merged = current == null ? new ArrayList<>() : new ArrayList<>(current);
            if (incoming != null) {
                merged.addAll(incoming);
            }
            return merged;
        });
    }

    /** Unions, keeping insertion order so the result is stable rather than merely correct. */
    public static <E> Reducer<Set<E>> unionSet() {
        return Reducer.named("unionSet", (current, incoming) -> {
            Set<E> merged = current == null ? new LinkedHashSet<>() : new LinkedHashSet<>(current);
            if (incoming != null) {
                merged.addAll(incoming);
            }
            return merged;
        });
    }

    /** Puts every entry of the incoming map over the current one. */
    public static <K, V> Reducer<Map<K, V>> mergeMap() {
        return Reducer.named("mergeMap", (current, incoming) -> {
            Map<K, V> merged = current == null ? new LinkedHashMap<>() : new LinkedHashMap<>(current);
            if (incoming != null) {
                merged.putAll(incoming);
            }
            return merged;
        });
    }

    /** Adds. Null counts as zero, so a counter needs no initial value. */
    public static Reducer<Long> sumLong() {
        return Reducer.named("sumLong", (current, incoming) ->
                (current == null ? 0L : current) + (incoming == null ? 0L : incoming));
    }

    /** Adds. Null counts as zero. */
    public static Reducer<Integer> sumInt() {
        return Reducer.named("sumInt", (current, incoming) ->
                (current == null ? 0 : current) + (incoming == null ? 0 : incoming));
    }

    /**
     * Refuses a second write outright.
     *
     * <p>For a channel that is written once by design: the entry node's input, a run's
     * identifier. A second write is a bug in the graph, and failing loudly beats silently
     * keeping one of the two.
     */
    public static <T> Reducer<T> writeOnce(String channel) {
        return Reducer.named("writeOnce", (current, incoming) -> {
            if (current != null) {
                throw new DagException("channel " + channel + " is declared write-once but "
                        + "was written a second time. Either two nodes write it, or one node "
                        + "runs twice");
            }
            return incoming;
        });
    }
}
