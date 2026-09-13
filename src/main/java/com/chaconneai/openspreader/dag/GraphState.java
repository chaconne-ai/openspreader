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

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The state a run carries, as a set of named channels.
 *
 * <p>Immutable. A node <b>reads</b> it and returns a map of the channels it changed; it
 * never mutates what it was given. That is what makes running two nodes in parallel safe
 * without a single lock in user code: neither can see the other's half-finished work,
 * because neither writes anything the other can see.
 *
 * <h2>Reading is typed, on purpose</h2>
 * The underlying map is {@code String -> Object}, so something has to do the casting. Doing
 * it here means a wrong type surfaces as a message naming the channel and both types,
 * rather than as a {@code ClassCastException} pointing at a line of engine code.
 *
 * <pre>{@code
 * public Map<String, Object> execute(GraphState s) {
 *     int x = s.getInt("x");                     // throws with a clear message if it is a String
 *     List<String> items = s.getList("items");   // empty list when the channel is unset
 *     return Map.of("total", x * items.size());  // only what changed
 * }
 * }</pre>
 *
 * <h2>It crosses the network</h2>
 * A node runs in another process, so the slice it is given travels as an argument of the
 * dispatch call and is serialised by the configured {@code ObjectCodec}. Under the default
 * JDK serialisation that means <b>every channel value must be {@link Serializable}</b>. The
 * state object itself is; what goes into it is the caller's responsibility.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class GraphState implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final GraphState EMPTY = new GraphState(Map.of());

    private final Map<String, Object> channels;

    private GraphState(Map<String, Object> channels) {
        this.channels = channels;
    }

    public static GraphState of(Map<String, Object> channels) {
        if (channels == null || channels.isEmpty()) {
            return EMPTY;
        }
        // Copied rather than wrapped: the caller keeps a reference to the map it passed in,
        // and a state that changed under a running node would be the hardest possible bug
        return new GraphState(Collections.unmodifiableMap(new LinkedHashMap<>(channels)));
    }

    public static GraphState empty() {
        return EMPTY;
    }

    /** The raw view. Unmodifiable. */
    public Map<String, Object> asMap() {
        return channels;
    }

    public Set<String> channels() {
        return channels.keySet();
    }

    public boolean contains(String channel) {
        return channels.containsKey(channel);
    }

    public boolean isEmpty() {
        return channels.isEmpty();
    }

    /** @return null when the channel was never written */
    public Object get(String channel) {
        return channels.get(channel);
    }

    /**
     * Reads a channel as a given type.
     *
     * @return null when the channel was never written
     * @throws DagException when it holds something else, naming the channel and both types
     */
    public <T> T get(String channel, Class<T> type) {
        Object value = channels.get(channel);
        if (value == null) {
            return null;
        }
        if (!type.isInstance(value)) {
            throw new DagException("channel " + channel + " holds a "
                    + value.getClass().getName() + ", but was read as a " + type.getName());
        }
        return type.cast(value);
    }

    /** @return the value, or {@code fallback} when the channel was never written */
    public <T> T getOrDefault(String channel, Class<T> type, T fallback) {
        T value = get(channel, type);
        return value == null ? fallback : value;
    }

    /** @return 0 when the channel was never written, so a counter needs no initial value */
    public int getInt(String channel) {
        Number n = get(channel, Number.class);
        return n == null ? 0 : n.intValue();
    }

    /** @return 0 when the channel was never written */
    public long getLong(String channel) {
        Number n = get(channel, Number.class);
        return n == null ? 0L : n.longValue();
    }

    /** @return false when the channel was never written */
    public boolean getBoolean(String channel) {
        Boolean b = get(channel, Boolean.class);
        return b != null && b;
    }

    /** @return null when the channel was never written */
    public String getString(String channel) {
        return get(channel, String.class);
    }

    /**
     * Reads a channel as a list.
     *
     * <p><b>An unset channel reads as an empty list, not null.</b> Gathering channels are
     * the common case in a fan-in, and every one of them would otherwise start with the
     * same null check.
     */
    @SuppressWarnings("unchecked")
    public <E> List<E> getList(String channel) {
        Object value = channels.get(channel);
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof List)) {
            throw new DagException("channel " + channel + " holds a "
                    + value.getClass().getName() + ", but was read as a List");
        }
        return (List<E>) value;
    }

    /** A view of only these channels. Used to hand a node the slice it declared it reads. */
    public GraphState slice(Set<String> wanted) {
        if (wanted == null) {
            return this;
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (String c : wanted) {
            if (channels.containsKey(c)) {
                out.put(c, channels.get(c));
            }
        }
        return of(out);
    }

    @Override
    public String toString() {
        return "GraphState" + channels;
    }
}
