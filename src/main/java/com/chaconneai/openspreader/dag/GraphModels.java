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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The reading half of the round trip, shared by every format.
 *
 * <p>A renderer's own job stops at the text: JSON and YAML each parse their syntax into the
 * same tree of maps, lists and strings, and everything after that happens here. So exactly one
 * piece of code decides what a definition <b>means</b>.
 *
 * <p>That split is the point. Two formats each rebuilding a graph in their own way is how one
 * of them comes to treat a conditional slightly differently from the other, and neither looks
 * wrong on its own.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 13/09/2026
 */
public class GraphModels {

    private GraphModels() {
    }

    // ------------------------------------------------------------------
    // Model to tree
    // ------------------------------------------------------------------

    /**
     * Lays a graph out as maps and lists, ready for a serialiser to write.
     *
     * <p>The other half of {@link #fromTree}: JSON and YAML differ in <b>syntax</b> only, so
     * the shape of a definition is decided once, here, and neither format gets to have an
     * opinion about it. That is also what makes the two interchangeable, which the round-trip
     * tests check by writing a graph in one, reading it back, and comparing.
     *
     * <p>Keys are in a fixed order, and a node's settings are sorted. Both matter for the
     * same reason: a stored definition that writes itself out differently on a different day
     * makes every diff a lie.
     */
    public static Map<String, Object> toTree(GraphModel model) {
        Map<String, Object> tree = new LinkedHashMap<>();
        tree.put("graph", model.graph());
        tree.put("entries", List.copyOf(model.entries()));
        tree.put("inputs", List.copyOf(model.inputs()));

        List<Object> channels = new ArrayList<>();
        for (GraphModel.ChannelView channel : model.channels()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", channel.name());
            entry.put("reducer", channel.reducer());
            channels.add(entry);
        }
        tree.put("channels", channels);

        List<Object> nodes = new ArrayList<>();
        for (GraphModel.NodeView node : model.nodes()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", node.name());
            entry.put("type", node.type());
            entry.put("kind", node.kind());
            entry.put("entry", node.entry());
            entry.put("local", node.local());
            entry.put("trigger", node.trigger());
            entry.put("retries", node.retries());
            if (node.bean() != null) {
                entry.put("bean", node.bean());
            }
            if (!node.config().isEmpty()) {
                entry.put("config", sortedDeep(node.config()));
            }
            if (node.status() != null) {
                entry.put("status", node.status().name());
            }
            nodes.add(entry);
        }
        tree.put("nodes", nodes);

        List<Object> edges = new ArrayList<>();
        for (GraphModel.EdgeView edge : model.edges()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("from", edge.from());
            entry.put("to", edge.to());
            entry.put("kind", edge.kind());
            entry.put("condition", edge.condition());
            if (edge.branch() != null) {
                entry.put("branch", edge.branch());
            }
            edges.add(entry);
        }
        tree.put("edges", edges);

        List<Object> conditionals = new ArrayList<>();
        for (GraphModel.ConditionalView conditional : model.conditionals()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("sources", List.copyOf(conditional.sources()));
            entry.put("form", conditional.form());
            entry.put("expression", conditional.expression());
            entry.put("predicates", listWithNulls(conditional.predicates()));
            List<Object> branches = new ArrayList<>();
            conditional.branches().forEach((key, targets) -> {
                Map<String, Object> branch = new LinkedHashMap<>();
                branch.put("key", key);
                branch.put("targets", List.copyOf(targets));
                branches.add(branch);
            });
            entry.put("branches", branches);
            entry.put("else", List.copyOf(conditional.elseTargets()));
            conditionals.add(entry);
        }
        tree.put("conditionals", conditionals);
        return tree;
    }

    /**
     * A node's settings with every map sorted by key, however deep.
     *
     * <p>Java's {@code Map.of} randomises its iteration order per JVM, so a graph built with
     * one would write out differently on every restart. Sorting makes the file a function of
     * the graph rather than of how it happened to be built.
     */
    private static Object sortedDeep(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> out = new TreeMap<>();
            map.forEach((key, nested) -> out.put(String.valueOf(key), sortedDeep(nested)));
            return out;
        }
        if (value instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            list.forEach(item -> out.add(sortedDeep(item)));
            return out;
        }
        return value;
    }

    /** Copies a list that may hold nulls, which {@code List.copyOf} refuses. */
    private static List<String> listWithNulls(List<String> values) {
        List<String> out = new ArrayList<>(values.size());
        out.addAll(values);
        return Collections.unmodifiableList(out);
    }

    // ------------------------------------------------------------------
    // Tree to model
    // ------------------------------------------------------------------

    /**
     * Reads a parsed definition into a {@link GraphModel}.
     *
     * @param tree what a format's parser produced: maps, lists, strings, and for JSON also
     *             booleans and numbers. Both are accepted for every scalar, because YAML
     *             hands back text where JSON hands back a type
     */
    public static GraphModel fromTree(Map<String, Object> tree) {
        String name = text(tree.get("graph"));
        if (name == null) {
            throw new DagException("this is not a graph definition: no \"graph\" name in it");
        }

        List<GraphModel.ChannelView> channels = new ArrayList<>();
        for (Map<String, Object> entry : maps(tree.get("channels"), "channels")) {
            channels.add(new GraphModel.ChannelView(
                    required(entry, "name", "channels"), text(entry.get("reducer"))));
        }

        List<GraphModel.NodeView> nodes = new ArrayList<>();
        for (Map<String, Object> entry : maps(tree.get("nodes"), "nodes")) {
            String status = text(entry.get("status"));
            nodes.add(new GraphModel.NodeView(
                    required(entry, "name", "nodes"),
                    text(entry.get("type")),
                    text(entry.get("kind")),
                    flag(entry.get("entry")),
                    flag(entry.get("local")),
                    text(entry.get("trigger")),
                    number(entry.get("retries")),
                    text(entry.get("bean")),
                    settings(entry.get("config")),
                    status == null ? null : NodeStatus.valueOf(status)));
        }

        List<GraphModel.EdgeView> edges = new ArrayList<>();
        for (Map<String, Object> entry : maps(tree.get("edges"), "edges")) {
            edges.add(new GraphModel.EdgeView(
                    required(entry, "from", "edges"),
                    required(entry, "to", "edges"),
                    text(entry.get("kind")),
                    text(entry.get("condition")),
                    text(entry.get("branch"))));
        }

        List<GraphModel.ConditionalView> conditionals = new ArrayList<>();
        for (Map<String, Object> entry : maps(tree.get("conditionals"), "conditionals")) {
            Map<String, List<String>> branches = new LinkedHashMap<>();
            for (Map<String, Object> branch : maps(entry.get("branches"), "branches")) {
                branches.put(required(branch, "key", "branches"),
                        strings(branch.get("targets")));
            }
            conditionals.add(new GraphModel.ConditionalView(
                    strings(entry.get("sources")),
                    text(entry.get("form")),
                    text(entry.get("expression")),
                    strings(entry.get("predicates")),
                    Collections.unmodifiableMap(branches),
                    strings(entry.get("else"))));
        }

        return new GraphModel(name, strings(tree.get("entries")), List.copyOf(nodes),
                List.copyOf(edges), List.copyOf(channels), List.copyOf(conditionals),
                strings(tree.get("inputs")));
    }

    // ------------------------------------------------------------------
    // Model to builder
    // ------------------------------------------------------------------

    /**
     * Rebuilds a builder from a model.
     *
     * <p>A builder rather than a {@link CompiledGraph}, so that whatever the text could not
     * carry can still be added: listeners above all.
     *
     * @throws DagException naming what is missing when the catalog cannot supply a node class
     *                      or a reducer, or when the definition holds a lambda, which was
     *                      never text and so was never going to come back
     */
    public static StateGraph toStateGraph(GraphModel model, GraphCatalog catalog) {
        if (catalog == null) {
            throw new IllegalArgumentException("loading a definition needs a GraphCatalog to "
                    + "supply node classes and reducers");
        }
        StateGraph graph = StateGraph.create(model.graph());

        for (GraphModel.ChannelView channel : model.channels()) {
            if (channel.reducer() == null) {
                // Written from a graph whose reducer was a lambda. It was already unloadable
                // when it was written, and saying so here, naming the channel, beats handing
                // back a graph that quietly merges differently
                throw new DagException("channel \"" + channel.name() + "\" has no named "
                        + "reducer, so this definition cannot be read back. Use a factory on "
                        + "Reducers, or Reducer.named(...), when the graph is built");
            }
            Reducer<?> reducer = catalog.reducer(channel.reducer());
            if (reducer == null) {
                throw new DagException("the catalog has no reducer called \""
                        + channel.reducer() + "\", needed by channel \"" + channel.name()
                        + "\". Register it with GraphCatalog.builder().reducer(...)");
            }
            graph.channel(channel.name(), cast(reducer));
        }

        // Every node first, so that the edges below can be declared by name alone
        for (GraphModel.NodeView node : model.nodes()) {
            if (node.bean() != null) {
                // Declared by bean name, which is what a graph assembled from data does. No
                // class is needed and none is looked up
                graph.node(node.name(), node.bean(), node.config());
                if (!triggerOf(node.trigger()).equals(Trigger.all())) {
                    graph.trigger(node.name(), triggerOf(node.trigger()));
                }
            } else {
                graph.node(node.name(), nodeClass(catalog, node), triggerOf(node.trigger()));
                if (!node.config().isEmpty()) {
                    graph.node(node.name(), nodeClass(catalog, node), node.config());
                }
            }
            if (node.retries() > 0) {
                graph.retry(node.name(), node.retries());
            }
            if (node.local()) {
                graph.local(node.name());
            }
        }

        for (GraphModel.EdgeView edge : model.edges()) {
            if ("conditional".equals(edge.kind())) {
                // Conditional edges come back from the conditionals section, which keeps the
                // switch whole. They appear in the flat edge list for drawing only
                continue;
            }
            StateGraph.From from = graph.from(edge.from());
            from = switch (condition(edge)) {
                case ON_SUCCESS -> from;
                case ON_FAILURE -> from.onFailure();
                case ON_COMPLETE -> from.onComplete();
            };
            from.to(edge.to());
        }

        for (GraphModel.ConditionalView conditional : model.conditionals()) {
            rebuild(graph, conditional);
        }

        for (String input : model.inputs()) {
            graph.input(input);
        }
        graph.entry(model.entries().toArray(new String[0]));
        return graph;
    }

    /**
     * One conditional, in whichever of the two forms it was written.
     *
     * <p>The forms are kept apart on the way back in because the predicate form's branch keys
     * are positions: rebuilding it as a switch would produce a graph that routes on the string
     * "0" rather than on the conditions.
     */
    private static void rebuild(StateGraph graph, GraphModel.ConditionalView conditional) {
        String[] sources = conditional.sources().toArray(new String[0]);

        if ("predicate".equals(conditional.form())) {
            List<String> predicates = conditional.predicates();
            if (predicates.isEmpty() || predicates.contains(null)) {
                throw new DagException("the conditional from " + conditional.sources()
                        + " was written with a lambda condition, which has no text to store. "
                        + "Write conditions as SpEL, when(\"#risk > 80\"), for a definition "
                        + "that can be read back");
            }
            StateGraph.When when = null;
            for (int i = 0; i < predicates.size(); i++) {
                List<String> targets = conditional.branches().get(String.valueOf(i));
                if (targets == null) {
                    throw new DagException("the conditional from " + conditional.sources()
                            + " has " + predicates.size() + " conditions but no branch "
                            + i + ". The definition is incomplete");
                }
                when = when == null ? graph.from(sources).when(predicates.get(i))
                        : when.when(predicates.get(i));
                when = when.to(targets.toArray(new String[0]));
            }
            if (!conditional.elseTargets().isEmpty()) {
                when.otherwise(conditional.elseTargets().toArray(new String[0]));
            }
            return;
        }

        if (conditional.expression() == null) {
            throw new DagException("the conditional from " + conditional.sources()
                    + " was written with a lambda router, which has no text to store. Use "
                    + "switchOn(\"#kind\") for a definition that can be read back");
        }
        StateGraph.Switch branch = graph.from(sources).switchOn(conditional.expression());
        for (Map.Entry<String, List<String>> entry : conditional.branches().entrySet()) {
            branch.caseOf(entry.getKey(), entry.getValue().toArray(new String[0]));
        }
        if (!conditional.elseTargets().isEmpty()) {
            branch.orElse(conditional.elseTargets().toArray(new String[0]));
        }
    }

    private static Class<? extends GraphNode> nodeClass(GraphCatalog catalog,
                                                        GraphModel.NodeView node) {
        if (node.type() == null) {
            throw new DagException("node \"" + node.name() + "\" has no type in this "
                    + "definition, so there is nothing to look up");
        }
        Class<? extends GraphNode> type = catalog.nodeClass(node.type());
        if (type == null) {
            throw new DagException("the catalog offers no node class \"" + node.type()
                    + "\", needed by node \"" + node.name() + "\". Register it with "
                    + "GraphCatalog.builder().node(...). Nothing is resolved by name on its "
                    + "own: a definition is data, and turning a string in a data row into a "
                    + "loaded class is how data becomes code");
        }
        return type;
    }

    private static EdgeCondition condition(GraphModel.EdgeView edge) {
        if (edge.condition() == null) {
            return EdgeCondition.ON_SUCCESS;
        }
        try {
            return EdgeCondition.valueOf(edge.condition());
        } catch (IllegalArgumentException e) {
            throw new DagException("edge " + edge.from() + " -> " + edge.to()
                    + " has an unrecognised condition \"" + edge.condition() + "\"", e);
        }
    }

    /** {@code ALL}, {@code ANY} or {@code AT_LEAST(n)}, as {@link Trigger} writes itself. */
    private static Trigger triggerOf(String label) {
        if (label == null || "ALL".equals(label)) {
            return Trigger.all();
        }
        if ("ANY".equals(label)) {
            return Trigger.any();
        }
        if (label.startsWith("AT_LEAST(") && label.endsWith(")")) {
            String n = label.substring("AT_LEAST(".length(), label.length() - 1);
            try {
                return Trigger.atLeast(Integer.parseInt(n.trim()));
            } catch (NumberFormatException e) {
                throw new DagException("unrecognised trigger \"" + label + "\"", e);
            }
        }
        throw new DagException("unrecognised trigger \"" + label + "\"");
    }

    // ------------------------------------------------------------------
    // Scalars, as either format hands them over
    // ------------------------------------------------------------------

    private static String text(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static String required(Map<String, Object> entry, String key, String section) {
        String value = text(entry.get(key));
        if (value == null) {
            throw new DagException("an entry under \"" + section + "\" has no \"" + key + "\"");
        }
        return value;
    }

    private static boolean flag(Object value) {
        return value instanceof Boolean b ? b : "true".equals(text(value));
    }

    private static int number(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.parseInt(text(value).trim());
        } catch (NumberFormatException e) {
            throw new DagException("expected a whole number, found \"" + value + "\"", e);
        }
    }

    private static List<String> strings(Object value) {
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof List<?> list)) {
            throw new DagException("expected a list, found \"" + value + "\"");
        }
        List<String> out = new ArrayList<>(list.size());
        // Not List.copyOf: a null is meaningful here, marking a condition that was a lambda
        list.forEach(item -> out.add(text(item)));
        return Collections.unmodifiableList(out);
    }

    /** A node's settings as they came back: whatever the format parsed, unchanged. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> settings(Object value) {
        if (value == null) {
            return Map.of();
        }
        if (!(value instanceof Map<?, ?> map)) {
            throw new DagException("a node's \"config\" should be a map, found \"" + value + "\"");
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>((Map<String, Object>) map));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> maps(Object value, String section) {
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof List<?> list)) {
            throw new DagException("\"" + section + "\" should be a list of entries");
        }
        List<Map<String, Object>> out = new ArrayList<>(list.size());
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> map)) {
                throw new DagException("\"" + section + "\" should hold entries, found \""
                        + item + "\"");
            }
            out.add((Map<String, Object>) map);
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static <T> Reducer<T> cast(Reducer<?> reducer) {
        return (Reducer<T>) reducer;
    }
}
