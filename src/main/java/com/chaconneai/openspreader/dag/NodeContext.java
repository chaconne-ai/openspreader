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

/**
 * Which step this is, and what it was configured with.
 *
 * <p>The state answers "what has happened so far"; this answers "who am I in this graph".
 * Without it one bean cannot serve as two steps: it would have no way to tell which of them
 * it is, and so no way to find its own settings.
 *
 * <pre>{@code
 * public Map<String, Object> execute(GraphState state, NodeContext context) {
 *     String url = context.config().getString("url");
 *     String key = context.idempotencyKey();     // this run, this step, stable across retries
 *     ...
 * }
 * }</pre>
 *
 * <h2>Why the configuration lives on the graph and not in a channel</h2>
 * A channel is what the run <b>produces</b>; configuration is what the graph <b>is</b>. Put
 * settings into channels and they become part of the data every node reads, every merge has
 * to order, and every dispatch has to carry. They also could not be written out with the
 * definition, which is what a graph built from rows in a table needs most.
 *
 * @param graph  the graph's name
 * @param runId  this run's identity, the same one {@link RunResult#runId()} carries
 * @param node   this node's name in this graph, which may differ from the bean's name
 * @param config this node's own settings, as declared on the graph. Never null
 * @param trace  the caller's trace context, in whatever form {@link TracePropagation}
 *               captured it. Empty when nothing was carried, which is the default
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/09/2026
 */
public record NodeContext(String graph, String runId, String node,
                          Map<String, Object> config,
                          Map<String, String> trace) implements Serializable {

    private static final long serialVersionUID = 1L;

    public NodeContext {
        config = config == null ? Map.of() : config;
        trace = trace == null ? Map.of() : trace;
    }

    /** Without a trace carrier, for a caller that has none. */
    public NodeContext(String graph, String runId, String node, Map<String, Object> config) {
        this(graph, runId, node, config, Map.of());
    }

    /**
     * A key that is the same for every attempt of this step in this run, and different for
     * every other step and every other run.
     *
     * <p>Exactly what an outward call needs to be safe to retry. A retry runs the node again
     * in full, so a key generated inside the node would differ between attempts and the far
     * side would see two requests; this one does not change.
     */
    public String idempotencyKey() {
        return runId + ":" + node;
    }

    /** A setting as text, or null. */
    public String getString(String key) {
        Object value = config.get(key);
        return value == null ? null : String.valueOf(value);
    }

    /** A setting as text, or {@code fallback} when it was not given. */
    public String getString(String key, String fallback) {
        String value = getString(key);
        return value == null ? fallback : value;
    }

    /** A setting as a whole number, or {@code fallback}. */
    public long getLong(String key, long fallback) {
        Object value = config.get(key);
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            throw new DagException("setting \"" + key + "\" on node " + node
                    + " should be a number, but is \"" + value + "\"", e);
        }
    }

    /** A setting as a flag, or {@code fallback}. */
    public boolean getBoolean(String key, boolean fallback) {
        Object value = config.get(key);
        if (value == null) {
            return fallback;
        }
        return value instanceof Boolean flag ? flag : Boolean.parseBoolean(String.valueOf(value));
    }

    /** A nested map setting, empty when it was not given. */
    @SuppressWarnings("unchecked")
    public Map<String, String> getMap(String key) {
        Object value = config.get(key);
        if (value == null) {
            return Map.of();
        }
        if (!(value instanceof Map<?, ?> map)) {
            throw new DagException("setting \"" + key + "\" on node " + node
                    + " should be a map, but is \"" + value + "\"");
        }
        Map<String, String> out = new LinkedHashMap<>();
        ((Map<Object, Object>) map).forEach((k, v) ->
                out.put(String.valueOf(k), v == null ? null : String.valueOf(v)));
        return Collections.unmodifiableMap(out);
    }

    /** A list setting, empty when it was not given. */
    public List<String> getList(String key) {
        Object value = config.get(key);
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof List<?> list)) {
            throw new DagException("setting \"" + key + "\" on node " + node
                    + " should be a list, but is \"" + value + "\"");
        }
        return list.stream().map(v -> v == null ? null : String.valueOf(v)).toList();
    }

    /** The same setting, refused when it is absent. For something the node cannot run without. */
    public String require(String key) {
        String value = getString(key);
        if (value == null || value.isBlank()) {
            throw new DagException("node " + node + " needs the setting \"" + key
                    + "\", which this graph does not give it. Declared settings: "
                    + config.keySet());
        }
        return value;
    }
}
