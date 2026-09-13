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

import com.chaconneai.openspreader.dag.DagRuntime;
import com.chaconneai.openspreader.dag.GraphRunner;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A graph that has been checked over and frozen. Immutable, thread-safe, and reusable: one
 * compiled graph serves any number of concurrent runs.
 *
 * <h2>What compile() refuses</h2>
 * <ol>
 *   <li><b>No entry node</b>, or an entry that was never declared</li>
 *   <li><b>An edge to a node that does not exist</b>. Only reachable through the
 *       string-named overloads; the class-named ones cannot produce it</li>
 *   <li><b>A cycle.</b> The D in DAG. Plain and conditional edges are walked together,
 *       because a cycle that closes through a conditional is still a cycle</li>
 *   <li><b>A node unreachable from the entry.</b> Nearly always a typo or a forgotten edge,
 *       and a node that can never run is worth hearing about before the run rather than
 *       after</li>
 *   <li><b>Two parallel branches writing one channel with no reducer</b>, where the nodes
 *       declared {@link GraphNode#writes()}. Undeclared, the same conflict is caught at run
 *       time when the second write lands</li>
 * </ol>
 *
 * <h2>Running it needs a runtime; rendering it does not</h2>
 * {@link #toMermaid()} and {@link #toDot()} work on a bare compiled graph, so a graph can be
 * drawn in a unit test with no cluster and no Spring context at all. {@link #invoke} needs a
 * {@link DagRuntime}, which is nothing but the three openspreader beans this engine borrows.
 *
 * <h2>One machine is not a special case</h2>
 * There is no local execution path here, and that is deliberate. {@code ProcessingPool}
 * already runs a node in this process when the application has no other replica, with no
 * network and no serialisation, and it decides that afresh on every call. Writing a second
 * local path would mean maintaining two engines that have to behave identically, to save
 * nothing.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class CompiledGraph {

    private final String name;
    private final Set<String> entries;
    private final Set<String> localNodes;
    private final Map<String, Integer> retries;
    private final List<GraphListener> listeners;
    private final Map<String, Object> inputs;

    /**
     * Plain edges and what makes each carry. A conditional edge is not here: it carries when
     * the router chose it, which already implies its source succeeded.
     */
    private final Map<String, Map<String, EdgeCondition>> conditions;
    private final Map<String, StateGraph.NodeSpec> nodes;
    private final Map<String, Set<String>> edges;
    private final Map<String, Set<String>> reverseEdges;
    private final List<StateGraph.ConditionalSpec> conditionals;
    private final Map<String, Reducer<?>> reducers;
    private final Map<String, Trigger> triggers;

    /**
     * For every node, every node that can possibly run before it.
     *
     * <p>Precomputed here because the run-time conflict check needs it on every merge: two
     * nodes writing one channel are <b>concurrent</b> exactly when neither is an ancestor of
     * the other, and that is the difference between a legitimate overwrite and a lost write.
     */
    private final Map<String, Set<String>> ancestors;

    /**
     * The openspreader beans that do the actual work. Null on a graph that was only compiled
     * for rendering or validation; {@link #invoke} then says so rather than failing with a
     * null pointer.
     */
    private final DagRuntime runtime;

    private CompiledGraph(String name, Set<String> entries, Map<String, StateGraph.NodeSpec> nodes,
                          Map<String, Set<String>> edges, Map<String, Set<String>> reverseEdges,
                          Map<String, Map<String, EdgeCondition>> conditions,
                          List<StateGraph.ConditionalSpec> conditionals,
                          Map<String, Reducer<?>> reducers, Map<String, Trigger> triggers,
                          Map<String, Set<String>> ancestors, Set<String> localNodes,
                          Map<String, Integer> retries, List<GraphListener> listeners,
                          Map<String, Object> inputs, DagRuntime runtime) {
        this.name = name;
        this.entries = entries;
        this.localNodes = localNodes;
        this.retries = retries;
        this.listeners = listeners;
        this.inputs = inputs;
        this.conditions = conditions;
        this.nodes = nodes;
        this.edges = edges;
        this.reverseEdges = reverseEdges;
        this.conditionals = conditionals;
        this.reducers = reducers;
        this.triggers = triggers;
        this.ancestors = ancestors;
        this.runtime = runtime;
    }

    static CompiledGraph of(String name, Set<String> entries,
                            Map<String, StateGraph.NodeSpec> nodes,
                            Map<String, Map<String, EdgeCondition>> edges,
                            List<StateGraph.ConditionalSpec> conditionals,
                            Map<String, Reducer<?>> reducers,
                            Map<String, Trigger> declaredTriggers, Set<String> localNodes,
                            Map<String, Integer> retries, List<GraphListener> listeners,
                            Map<String, Object> inputs) {

        Map<String, StateGraph.NodeSpec> frozenNodes = ordered(nodes);

        // Every edge, conditional ones included, in one adjacency map. The checks below and
        // the runner both want "where can this node lead", and a conditional edge leads
        // somewhere just as surely as a plain one does
        Map<String, Set<String>> allEdges = new LinkedHashMap<>();
        edges.forEach((from, tos) -> allEdges.computeIfAbsent(from, k -> new LinkedHashSet<>())
                .addAll(tos.keySet()));
        for (StateGraph.ConditionalSpec c : conditionals) {
            for (String from : c.sources()) {
                allEdges.computeIfAbsent(from, k -> new LinkedHashSet<>()).addAll(c.allTargets());
            }
        }

        validateEntries(entries, frozenNodes);
        validateEndpoints(name, frozenNodes, allEdges);
        validateAcyclic(name, frozenNodes, allEdges);
        validateReachable(name, entries, frozenNodes, allEdges);

        Map<String, Set<String>> reverse = reverseOf(frozenNodes.keySet(), allEdges);
        Map<String, Set<String>> ancestors = ancestorsOf(frozenNodes.keySet(), reverse);

        Map<String, Trigger> triggers = new LinkedHashMap<>();
        for (String node : frozenNodes.keySet()) {
            triggers.put(node, declaredTriggers.getOrDefault(node, Trigger.all()));
        }

        validateQuorums(name, triggers, reverse);

        Map<String, Map<String, EdgeCondition>> frozenConditions = new LinkedHashMap<>();
        edges.forEach((from, tos) -> frozenConditions.put(from, ordered(tos)));

        return new CompiledGraph(name, ordered(entries), frozenNodes,
                freeze(allEdges), freeze(reverse), ordered(frozenConditions),
                List.copyOf(conditionals),
                ordered(reducers), ordered(triggers), freeze(ancestors),
                ordered(localNodes), ordered(retries), List.copyOf(listeners),
                // Not Map.copyOf: an optional input may legitimately default to null, and
                // Map.copyOf refuses null values
                Collections.unmodifiableMap(new LinkedHashMap<>(inputs)), null);
    }

    /**
     * The same graph, wired to a runtime.
     *
     * <p>Compilation and execution are separated on purpose: a graph is a value and can be
     * built in a constructor, a test or a static field, long before any cluster exists.
     * {@code ProcessingDag} binds the runtime in as the graph is handed out.
     */
    public CompiledGraph withRuntime(DagRuntime runtime) {
        return new CompiledGraph(name, entries, nodes, edges, reverseEdges, conditions,
                conditionals, reducers, triggers, ancestors, localNodes, retries, listeners,
                inputs, runtime);
    }

    // ------------------------------------------------------------------
    // Running
    // ------------------------------------------------------------------

    /** Runs the graph from an initial set of channels and waits for it to finish. */
    public RunResult invoke(Map<String, Object> initialChannels) {
        return invoke(GraphState.of(initialChannels));
    }

    /**
     * Checks the declared inputs and fills in the optional ones.
     *
     * <p>The one check that cannot happen at compile time, because what a caller will pass is
     * not known until it passes it. It happens <b>before any node is dispatched</b>, so a
     * missing input is an error at the call site rather than a null three steps into the
     * graph.
     *
     * @throws DagException naming every missing input at once. Naming one at a time would
     *                      mean running it three times to find three mistakes
     */
    private GraphState applyInputs(GraphState initial) {
        if (inputs.isEmpty()) {
            return initial;
        }
        GraphState state = initial == null ? GraphState.empty() : initial;
        List<String> missing = new ArrayList<>();
        Map<String, Object> filled = null;

        for (Map.Entry<String, Object> input : inputs.entrySet()) {
            if (state.contains(input.getKey())) {
                continue;
            }
            if (input.getValue() == StateGraph.REQUIRED) {
                missing.add(input.getKey());
            } else if (input.getValue() != null) {
                if (filled == null) {
                    filled = new LinkedHashMap<>(state.asMap());
                }
                filled.put(input.getKey(), input.getValue());
            }
        }

        if (!missing.isEmpty()) {
            throw new DagException("graph " + name + " was invoked without its required "
                    + "input(s) " + missing + ". Declared inputs: " + inputs.keySet()
                    + ", supplied: " + state.channels());
        }
        return filled == null ? state : GraphState.of(filled);
    }

    /** Runs the graph and waits for it to finish. */
    public RunResult invoke(GraphState initial) {
        return invoke(initial, 0L, TimeUnit.MILLISECONDS);
    }

    /**
     * Runs the graph with a ceiling on the whole run.
     *
     * @param timeout 0 or less waits indefinitely. A run that overruns comes back with the
     *                nodes that did finish and a {@link RunResult#failure()} saying so,
     *                rather than throwing, because a partial result is usually more useful
     *                than none
     */
    public RunResult invoke(GraphState initial, long timeout, TimeUnit unit) {
        // Inputs first, and deliberately: checking them is a pure function of the graph and
        // the arguments, so it needs no runtime and a graph compiled purely for validation can
        // still be checked against a set of arguments. It is also the mistake more likely to
        // be the caller's
        GraphState prepared = applyInputs(initial);

        if (runtime == null) {
            throw new DagException("graph " + name + " is not bound to a runtime. Run it "
                    + "through the ProcessingDag bean, which binds one, or attach one with "
                    + "withRuntime(...) when using this outside Spring");
        }
        return new GraphRunner(this, runtime)
                .run(prepared, unit.toMillis(Math.max(0L, timeout)));
    }

    // ------------------------------------------------------------------
    // Rendering
    // ------------------------------------------------------------------

    /**
     * The graph as text, in whatever format is configured.
     *
     * <p><b>One output, not one method per format.</b> The four built-in renderers describe
     * the same graph, so a method each would invite an application to emit two of them and
     * then have to keep both in step wherever they land. Which one this is comes from
     * {@code spring.spreader.multiprocessing.dag.renderer}; see {@link RendererType}.
     *
     * <p>An unbound graph, one compiled purely for validation or in a unit test, has no
     * configuration to consult and renders as JSON. Rendering deliberately needs no cluster
     * and no Spring context.
     */
    public String render() {
        return configuredRenderer().render(this, null);
    }

    /**
     * The graph as text, in a format of your choosing.
     *
     * <p>The escape hatch, and the reason {@link GraphRenderer} is an interface: a format the
     * engine does not know about is one implementation away, and nothing here has to be told
     * about it.
     */
    public String render(GraphRenderer renderer) {
        return renderer.render(this, null);
    }

    /** The configured renderer, or Mermaid when this graph is not bound to a runtime. */
    GraphRenderer configuredRenderer() {
        return runtime == null || runtime.renderer() == null
                ? RendererType.JSON.renderer()
                : runtime.renderer();
    }

    /**
     * The graph as a Mermaid diagram, for a README, a pull request or an issue.
     *
     * <p>Separate from {@link #render()} because it is not a serialisation: it cannot be read
     * back, and there is nothing to configure. Drawing and storing are different jobs, and
     * {@link PrintUtils} is where the drawing lives.
     *
     * <p>Printing one at startup to show the shape a graph came out as is reasonable; for a
     * running commentary, {@code RunResult.describe()} is the one line per node a log wants.
     */
    public String toMermaid() {
        return PrintUtils.asMermaidString(this, null);
    }

    /** The same picture as Graphviz DOT, where a real image file is wanted. */
    public String toDot() {
        return PrintUtils.asDotString(this, null);
    }

    // ------------------------------------------------------------------
    // Structure, read by the runner and the renderers
    // ------------------------------------------------------------------

    public String name() {
        return name;
    }

    /** Where runs start. More than one for a graph with several roots. */
    public Set<String> entries() {
        return entries;
    }

    /**
     * What has to become of {@code from} for the edge to {@code to} to carry.
     *
     * <p>{@link EdgeCondition#ON_SUCCESS} for a conditional edge and for anything not
     * declared, which is the ordinary case.
     */
    public EdgeCondition conditionOf(String from, String to) {
        Map<String, EdgeCondition> out = conditions.get(from);
        EdgeCondition declared = out == null ? null : out.get(to);
        return declared == null ? EdgeCondition.ON_SUCCESS : declared;
    }

    public Set<String> nodeNames() {
        return nodes.keySet();
    }

    public Class<? extends GraphNode> typeOf(String node) {
        StateGraph.NodeSpec spec = nodes.get(node);
        return spec == null ? null : spec.type();
    }

    /** Where this node leads, conditional targets included. */
    public Set<String> successors(String node) {
        return edges.getOrDefault(node, Set.of());
    }

    /** What leads into this node. */
    public Set<String> predecessors(String node) {
        return reverseEdges.getOrDefault(node, Set.of());
    }

    /**
     * Whether this node runs on the coordinating instance rather than being dispatched.
     *
     * <p>Declared with {@code StateGraph.local(...)}; see {@link NodeInvoker} for why it is a
     * property of the graph and not of the node.
     */
    public boolean isLocal(String node) {
        return localNodes.contains(node);
    }

    /** How many further attempts this node gets after a failure. 0 means none. */
    public int retriesOf(String node) {
        return retries.getOrDefault(node, 0);
    }

    /** The observers, in declaration order. */
    public List<GraphListener> listeners() {
        return listeners;
    }

    /** The declared inputs: channel to its default, or a sentinel for the required ones. */
    public java.util.Set<String> declaredInputs() {
        return inputs.keySet();
    }

    public Trigger triggerOf(String node) {
        return triggers.getOrDefault(node, Trigger.all());
    }

    public List<StateGraph.ConditionalSpec> conditionals() {
        return conditionals;
    }

    /** The conditionals this node is a source of. Empty for an ordinary node. */
    public List<StateGraph.ConditionalSpec> conditionalsFrom(String node) {
        List<StateGraph.ConditionalSpec> out = new ArrayList<>();
        for (StateGraph.ConditionalSpec c : conditionals) {
            if (c.sources().contains(node)) {
                out.add(c);
            }
        }
        return out;
    }

    /** Every node that some conditional can choose, across the whole graph. */
    public Set<String> conditionalTargets() {
        Set<String> out = new LinkedHashSet<>();
        conditionals.forEach(c -> out.addAll(c.allTargets()));
        return out;
    }

    @SuppressWarnings("unchecked")
    public Reducer<Object> reducerOf(String channel) {
        return (Reducer<Object>) reducers.get(channel);
    }

    public boolean hasReducer(String channel) {
        return reducers.containsKey(channel);
    }

    /** Every channel that has a declared reducer, and the reducer. For serialisation. */
    public Map<String, Reducer<?>> channels() {
        return reducers;
    }

    /**
     * Whether these two nodes could be running at the same time.
     *
     * <p>Two nodes are concurrent exactly when neither can reach the other. It is the test
     * the merge step uses to tell a legitimate overwrite, where a later node replaces an
     * earlier one's value, from a lost write, where two branches raced and one vanished.
     */
    /**
     * Whether {@code a} must be finished before {@code b} can start, directly or through
     * however many steps.
     *
     * <p>The other half of {@link #concurrent}: one says these two race, this one says this
     * one comes first. The runner needs both, to know whose update a node is entitled to see.
     */
    public boolean precedes(String a, String b) {
        return ancestors.getOrDefault(b, Set.of()).contains(a);
    }

    public boolean concurrent(String a, String b) {
        if (a.equals(b)) {
            return false;
        }
        return !ancestors.getOrDefault(a, Set.of()).contains(b)
                && !ancestors.getOrDefault(b, Set.of()).contains(a);
    }

    @Override
    public String toString() {
        return "CompiledGraph[" + name + ", " + nodes.size() + " nodes, entry=" + entries + "]";
    }

    // ------------------------------------------------------------------
    // Validation
    // ------------------------------------------------------------------

    private static void validateEntries(Set<String> entries,
                                        Map<String, StateGraph.NodeSpec> nodes) {
        if (entries == null || entries.isEmpty()) {
            throw new DagException("the graph has no entry node. Add entry(SomeNode.class)");
        }
        for (String entry : entries) {
            if (!nodes.containsKey(entry)) {
                throw new DagException("the entry node " + entry + " was never declared");
            }
        }
    }

    /**
     * A quorum bigger than the number of edges that could satisfy it.
     *
     * <p>{@code atLeast(3)} on a node with two inbound edges can never fire, so the graph can
     * never finish. Left to run time it would present as a hang, which is the least
     * diagnosable failure there is.
     */
    private static void validateQuorums(String graph, Map<String, Trigger> triggers,
                                        Map<String, Set<String>> reverse) {
        triggers.forEach((node, trigger) -> {
            int inbound = reverse.getOrDefault(node, Set.of()).size();
            if (trigger.required() > inbound) {
                throw new DagException("graph " + graph + ": node " + node + " asks for "
                        + trigger + " but only " + inbound + " edge(s) lead into it, so it "
                        + "could never run");
            }
        });
    }

    private static void validateEndpoints(String graph, Map<String, StateGraph.NodeSpec> nodes,
                                          Map<String, Set<String>> edges) {
        edges.forEach((from, tos) -> {
            if (!nodes.containsKey(from)) {
                throw new DagException("graph " + graph + " has an edge out of " + from
                        + ", which is not a declared node");
            }
            for (String to : tos) {
                if (!nodes.containsKey(to)) {
                    throw new DagException("graph " + graph + " has an edge " + from + " -> "
                            + to + ", but " + to + " is not a declared node");
                }
            }
        });
    }

    /**
     * Depth-first cycle detection, reporting the cycle it found rather than merely that one
     * exists. "There is a cycle" in a graph of thirty nodes is not an actionable message.
     */
    private static void validateAcyclic(String graph, Map<String, StateGraph.NodeSpec> nodes,
                                        Map<String, Set<String>> edges) {
        Set<String> done = new LinkedHashSet<>();
        Set<String> onPath = new LinkedHashSet<>();
        Deque<String> path = new ArrayDeque<>();
        for (String node : nodes.keySet()) {
            walk(graph, node, edges, done, onPath, path);
        }
    }

    private static void walk(String graph, String node, Map<String, Set<String>> edges,
                             Set<String> done, Set<String> onPath, Deque<String> path) {
        if (done.contains(node)) {
            return;
        }
        if (!onPath.add(node)) {
            List<String> cycle = new ArrayList<>(path);
            cycle.add(node);
            throw new DagException("graph " + graph + " is not acyclic: "
                    + String.join(" -> ", cycle.subList(cycle.indexOf(node), cycle.size())));
        }
        path.addLast(node);
        for (String next : edges.getOrDefault(node, Set.of())) {
            walk(graph, next, edges, done, onPath, path);
        }
        path.removeLast();
        onPath.remove(node);
        done.add(node);
    }

    private static void validateReachable(String graph, Set<String> entries,
                                          Map<String, StateGraph.NodeSpec> nodes,
                                          Map<String, Set<String>> edges) {
        Set<String> seen = new LinkedHashSet<>(entries);
        Deque<String> queue = new ArrayDeque<>(entries);
        while (!queue.isEmpty()) {
            for (String next : edges.getOrDefault(queue.poll(), Set.of())) {
                if (seen.add(next)) {
                    queue.add(next);
                }
            }
        }
        Set<String> orphans = new LinkedHashSet<>(nodes.keySet());
        orphans.removeAll(seen);
        if (!orphans.isEmpty()) {
            throw new DagException("graph " + graph + " declares node(s) " + orphans
                    + " that nothing leads to. Either add an edge into them, or remove them: "
                    + "a node that can never run is almost always a forgotten edge");
        }
    }

    // ------------------------------------------------------------------

    private static Map<String, Set<String>> reverseOf(Set<String> nodes,
                                                      Map<String, Set<String>> edges) {
        Map<String, Set<String>> reverse = new LinkedHashMap<>();
        nodes.forEach(n -> reverse.put(n, new LinkedHashSet<>()));
        edges.forEach((from, tos) -> tos.forEach(to -> reverse.get(to).add(from)));
        return reverse;
    }

    /**
     * Transitive predecessors of every node.
     *
     * <p>Computed once at compile time because the merge step asks "were these two
     * concurrent" on every channel of every update, and answering that by walking the graph
     * each time would make merging cost more than the work being merged.
     */
    private static Map<String, Set<String>> ancestorsOf(Set<String> nodes,
                                                        Map<String, Set<String>> reverse) {
        Map<String, Set<String>> out = new LinkedHashMap<>();
        for (String node : nodes) {
            Set<String> seen = new LinkedHashSet<>();
            Deque<String> queue = new ArrayDeque<>(reverse.getOrDefault(node, Set.of()));
            while (!queue.isEmpty()) {
                String p = queue.poll();
                if (seen.add(p)) {
                    queue.addAll(reverse.getOrDefault(p, Set.of()));
                }
            }
            out.put(node, seen);
        }
        return out;
    }

    /**
     * An unmodifiable copy that keeps declaration order.
     *
     * <p>Not {@code Map.copyOf}, whose iteration order is unspecified, and that is not a
     * detail here: this order is the order a graph is written out in. With a hash order, a
     * definition kept in version control produces a different file on a different day for no
     * change at all, and the diff a reviewer is meant to read becomes noise.
     */
    private static <V> Map<String, V> ordered(Map<String, V> map) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(map));
    }

    /** The same, for a set. */
    private static Set<String> ordered(Set<String> set) {
        return Collections.unmodifiableSet(new LinkedHashSet<>(set));
    }

    private static Map<String, Set<String>> freeze(Map<String, Set<String>> map) {
        Map<String, Set<String>> out = new LinkedHashMap<>();
        map.forEach((k, v) -> out.put(k, ordered(v)));
        return ordered(out);
    }
}
