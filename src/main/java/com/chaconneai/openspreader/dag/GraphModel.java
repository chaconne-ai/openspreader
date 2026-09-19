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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One flat description of a graph, extracted once and handed to every renderer.
 *
 * <p>It exists so that the picture and the export cannot disagree. Four renderers each
 * walking {@link CompiledGraph} in their own way is how a Mermaid diagram comes to show an
 * edge the JSON does not, and that is a bug nobody thinks to look for because both look
 * plausible on their own.
 *
 * @param graph   the graph's name
 * @param entries where runs start
 * @param nodes   every node, in declaration order
 * @param edges   every edge, conditional ones included and labelled as such
 * @param channels     every channel with a declared reducer, and what the reducer is called
 * @param conditionals  the switches, kept whole so that a definition can be read back. The
 *                      same edges also appear flattened in {@code edges}, where a renderer
 *                      wants them; the duplication is deliberate, because drawing wants a
 *                      flat list and reconstructing wants the structure
 * @param inputs  the declared input channels
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 13/09/2026
 */
public record GraphModel(String graph, List<String> entries, List<NodeView> nodes,
                         List<EdgeView> edges, List<ChannelView> channels,
                         List<ConditionalView> conditionals, List<String> inputs) {

    /**
     * @param reducer what the reducer is called, or null for a lambda. A null is what makes a
     *                definition unloadable, and saying so here beats discovering it on read
     */
    public record ChannelView(String name, String reducer) {
    }

    /**
     * @param form        {@code "switch"} for {@code switchOn}, {@code "predicate"} for a
     *                    {@code when} chain
     * @param expression  the router's SpEL for the switch form, null for a lambda
     * @param predicates  each {@code when}'s SpEL in order, for the predicate form
     * @param branches    branch key to its targets
     * @param elseTargets where an unmatched key goes, empty for nowhere
     */
    public record ConditionalView(List<String> sources, String form, String expression,
                                  List<String> predicates,
                                  Map<String, List<String>> branches,
                                  List<String> elseTargets) {
    }

    /**
     * @param kind   {@code "subgraph"} for a node that is itself a graph, {@code "node"}
     *               otherwise
     * @param local  whether it runs on the coordinating instance rather than being dispatched
     * @param status what became of it in the run being rendered, or null when there is none
     */
    /**
     * @param bean   the bean this node dispatches to, or null when it dispatches by class
     * @param config this node's own settings. A graph built from data is mostly this
     */
    public record NodeView(String name, String type, String kind, boolean entry, boolean local,
                           String trigger, int retries, String bean,
                           Map<String, Object> config, NodeStatus status) {

        public NodeView {
            config = config == null ? Map.of() : config;
        }
    }

    /**
     * @param kind      {@code "conditional"} when a switch decides it, {@code "plain"}
     *                  otherwise
     * @param condition what has to become of the source: ON_SUCCESS, ON_FAILURE, ON_COMPLETE
     * @param branch    the branch key for a conditional edge, {@code "else"} for its default,
     *                  null for a plain edge
     */
    public record EdgeView(String from, String to, String kind, String condition,
                           String branch) {
    }

    /**
     * A defensive copy that tolerates nulls, which {@code List.copyOf} does not.
     *
     * <p>A null in the predicate list is meaningful: it marks a condition written as a lambda,
     * and losing it would turn an unloadable definition into a wrong one.
     */
    private static List<String> listWithNulls(List<String> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    /** Walks the graph once. */
    public static GraphModel of(CompiledGraph graph, Map<String, NodeStatus> statuses) {
        List<NodeView> nodes = new ArrayList<>();
        for (String name : graph.nodeNames()) {
            Class<?> type = graph.typeOf(name);
            nodes.add(new NodeView(
                    name,
                    type == null ? null : type.getName(),
                    type != null && SubGraph.class.isAssignableFrom(type) ? "subgraph" : "node",
                    graph.entries().contains(name),
                    graph.isLocal(name),
                    graph.triggerOf(name).toString(),
                    graph.retriesOf(name),
                    graph.beanNameOf(name),
                    graph.configOf(name),
                    statuses == null ? null : statuses.get(name)));
        }

        List<EdgeView> edges = new ArrayList<>();
        Set<String> conditionalEdges = new LinkedHashSet<>();
        for (StateGraph.ConditionalSpec spec : graph.conditionals()) {
            for (String from : spec.sources()) {
                for (Map.Entry<String, List<String>> branch : spec.branches().entrySet()) {
                    for (String to : branch.getValue()) {
                        edges.add(new EdgeView(from, to, "conditional",
                                EdgeCondition.ON_SUCCESS.name(), branch.getKey()));
                        conditionalEdges.add(from + ' ' + to);
                    }
                }
                for (String to : spec.defaultTargets()) {
                    edges.add(new EdgeView(from, to, "conditional",
                            EdgeCondition.ON_SUCCESS.name(), "else"));
                    conditionalEdges.add(from + ' ' + to);
                }
            }
        }
        for (String from : graph.nodeNames()) {
            for (String to : graph.successors(from)) {
                if (!conditionalEdges.contains(from + ' ' + to)) {
                    edges.add(new EdgeView(from, to, "plain",
                            graph.conditionOf(from, to).name(), null));
                }
            }
        }

        List<ChannelView> channels = new ArrayList<>();
        graph.channels().forEach((name, reducer) ->
                channels.add(new ChannelView(name, reducer.name())));

        List<ConditionalView> conditionals = new ArrayList<>();
        for (StateGraph.ConditionalSpec spec : graph.conditionals()) {
            boolean predicateForm = !spec.predicateExpressions().isEmpty();
            conditionals.add(new ConditionalView(
                    spec.sources(),
                    predicateForm ? "predicate" : "switch",
                    spec.expression(),
                    listWithNulls(spec.predicateExpressions()),
                    // Not Map.copyOf: its iteration order is unspecified, and an unstable
                    // order would make the same graph write out differently twice
                    Collections.unmodifiableMap(new LinkedHashMap<>(spec.branches())),
                    spec.defaultTargets()));
        }

        return new GraphModel(graph.name(), List.copyOf(graph.entries()), List.copyOf(nodes),
                List.copyOf(edges), List.copyOf(channels), List.copyOf(conditionals),
                List.copyOf(graph.declaredInputs()));
    }
}
