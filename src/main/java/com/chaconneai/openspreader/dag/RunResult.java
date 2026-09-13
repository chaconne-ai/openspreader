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

import com.chaconneai.openspreader.dag.NodeOutcome;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * What one run produced: the final state, and what happened to every node.
 *
 * <h2>A failure is reported, not thrown</h2>
 * {@link CompiledGraph#invoke} does not throw when a node fails. It comes back with
 * {@link #failed()} true, {@link #failedNode()} naming the node and {@link #failure()}
 * carrying what that node threw, <b>unwrapped</b>.
 *
 * <p>The reason is that a failed run still has results. Six nodes succeeded, one threw, two
 * were never reached: throwing would discard the six, and with them any chance of working out
 * why the seventh failed. Callers that would rather have an exception write one line:
 *
 * <pre>{@code
 * RunResult r = flow.invoke(input);
 * if (r.failed()) {
 *     throw new IllegalStateException("order flow failed at " + r.failedNode(), r.failure());
 * }
 * }</pre>
 *
 * <h2>The picture is the debugging tool</h2>
 * {@link #toMermaid()} draws the run with every node coloured by what became of it. For a
 * graph with conditionals that is the fastest way to answer the question that actually gets
 * asked, which is never "did it work" but "why did it go that way".
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class RunResult {

    private final CompiledGraph graph;
    private final GraphState state;
    private final Map<String, NodeStatus> statuses;
    private final Map<String, NodeOutcome> outcomes;
    private final String failedNode;
    private final Throwable failure;
    private final Map<String, Throwable> failures;
    private final long millis;

    private RunResult(CompiledGraph graph, GraphState state, Map<String, NodeStatus> statuses,
                      Map<String, NodeOutcome> outcomes, String failedNode, Throwable failure,
                      Map<String, Throwable> failures, long millis) {
        this.graph = graph;
        this.state = state;
        this.statuses = statuses;
        this.outcomes = outcomes;
        this.failedNode = failedNode;
        this.failure = failure;
        this.failures = failures;
        this.millis = millis;
    }

    public static RunResult of(CompiledGraph graph, GraphState state,
                               Map<String, NodeStatus> statuses,
                               Map<String, NodeOutcome> outcomes,
                               String failedNode, Throwable failure,
                               Map<String, Throwable> failures, long millis) {
        return new RunResult(graph, state, Map.copyOf(statuses), Map.copyOf(outcomes),
                failedNode, failure, Map.copyOf(failures), millis);
    }

    /** The channels as they stood when the run ended. */
    public GraphState state() {
        return state;
    }

    public CompiledGraph graph() {
        return graph;
    }

    /** How long the whole run took, dispatch included. */
    public long millis() {
        return millis;
    }

    /**
     * Whether the run was stopped by something it could not handle.
     *
     * <p><b>A node throwing is not automatically this.</b> A failure with an
     * {@code onFailure()} or {@code onComplete()} edge out of it has somewhere to go, so the
     * graph carries on down the compensation path and finishes; {@code failed()} is then
     * false while {@link #failures()} still records what was thrown and
     * {@link #statusOf} still says FAILED.
     *
     * <p>That distinction is the whole point of failure edges: "an order was refused and
     * refunded" is a run that worked, not a run that broke.
     */
    public boolean failed() {
        return failure != null;
    }

    /**
     * Every node that threw, whether or not the graph recovered.
     *
     * <p>{@link #failure()} is only the one that stopped the run. This is all of them, and it
     * is what answers "why did the compensation path run" on a run that came back
     * successful.
     */
    public Map<String, Throwable> failures() {
        return failures;
    }

    /** The node that failed, or null. Null with {@link #failed()} true means the run itself
     *  failed, by timing out or being interrupted. */
    public String failedNode() {
        return failedNode;
    }

    /** What the failing node threw, unwrapped. Null on success. */
    public Throwable failure() {
        return failure;
    }

    public NodeStatus statusOf(String node) {
        return statuses.getOrDefault(node, NodeStatus.PENDING);
    }

    public Map<String, NodeStatus> statuses() {
        return statuses;
    }

    /** The nodes that ran and succeeded. */
    public Set<String> succeeded() {
        return withStatus(NodeStatus.SUCCESS);
    }

    /** The nodes a conditional passed over, or that nothing reached. */
    public Set<String> skipped() {
        return withStatus(NodeStatus.SKIPPED);
    }

    /**
     * Where a node ran, as a cluster label.
     *
     * <p>The quickest check that work is genuinely being spread: with several replicas these
     * should not all be the same. With one replica they all are, correctly, because the pool
     * runs locally when there is nobody to send to.
     */
    public String executedOn(String node) {
        NodeOutcome outcome = outcomes.get(node);
        return outcome == null ? null : outcome.executedOn();
    }

    /** How long a node's own code took, excluding dispatch. */
    public long millisOf(String node) {
        NodeOutcome outcome = outcomes.get(node);
        return outcome == null ? 0L : outcome.millis();
    }

    /** Which node last wrote nothing at all. For tests that assert a node was a no-op. */
    public Map<String, Object> updatesOf(String node) {
        NodeOutcome outcome = outcomes.get(node);
        return outcome == null ? Map.of() : outcome.updates();
    }

    /**
     * The run as text, in whatever format is configured, with each node's outcome included.
     *
     * <p>In a picture that is colour: green for success, grey for skipped, red for failed. In
     * an export it is a {@code status} field. Either way it answers the question that actually
     * gets asked about a branching workflow, which is not "did it work" but "why did it go
     * that way".
     *
     * @see CompiledGraph#render()
     */
    public String render() {
        GraphRenderer renderer = graph.configuredRenderer();
        return renderer.render(graph, statuses);
    }

    /** The run as text, in a format of your choosing. */
    public String render(GraphRenderer renderer) {
        return renderer.render(graph, statuses);
    }

    /**
     * The run as a Mermaid diagram, each node coloured by what became of it: green for
     * success, grey for skipped, red for failed.
     *
     * <p>For a README or a pull request, where it renders as a picture. For a log, use
     * {@link #describe()}: one line per node, with timings and where each ran.
     */
    public String toMermaid() {
        return PrintUtils.asMermaidString(graph, statuses);
    }

    /** The same picture as Graphviz DOT, coloured the same way. */
    public String toDot() {
        return PrintUtils.asDotString(graph, statuses);
    }

    /**
     * One line per node, for a log or a failed assertion.
     *
     * <pre>
     * order-flow finished in 412ms: 4 ok, 1 skipped
     *   Validate   SUCCESS   12ms  on app@10.0.0.1:30001
     *   Reserve    SUCCESS  180ms  on app@10.0.0.2:30001
     *   Charge     SUCCESS  164ms  on app@10.0.0.3:30001
     *   Refund     SKIPPED
     *   Ship       SUCCESS   40ms  on app@10.0.0.1:30001
     * </pre>
     */
    public String describe() {
        StringBuilder sb = new StringBuilder(256);
        sb.append(graph.name()).append(failed() ? " failed" : " finished")
                .append(" in ").append(millis).append("ms: ")
                .append(succeeded().size()).append(" ok");
        if (!skipped().isEmpty()) {
            sb.append(", ").append(skipped().size()).append(" skipped");
        }
        if (failedNode != null) {
            sb.append(", failed at ").append(failedNode);
        }
        for (String node : graph.nodeNames()) {
            NodeStatus s = statusOf(node);
            sb.append("\n  ").append(pad(node)).append(' ').append(pad(s.name(), 9));
            if (s == NodeStatus.SUCCESS || s == NodeStatus.FAILED) {
                sb.append(millisOf(node)).append("ms  on ").append(executedOn(node));
            }
        }
        if (failure != null) {
            sb.append("\n  cause: ").append(failure);
        }
        // Handled failures too. A run that recovered still owes an explanation for why the
        // recovery path ran at all
        failures.forEach((node, thrown) -> {
            if (!node.equals(failedNode)) {
                sb.append("\n  handled at ").append(node).append(": ").append(thrown);
            }
        });
        return sb.toString();
    }

    @Override
    public String toString() {
        return "RunResult[" + graph.name() + (failed() ? ", failed at " + failedNode : ", ok")
                + ", " + millis + "ms]";
    }

    private Set<String> withStatus(NodeStatus wanted) {
        Set<String> out = new LinkedHashSet<>();
        // Walked in the graph's node order rather than the map's, so the result reads in the
        // order the graph was written
        for (String node : graph.nodeNames()) {
            if (statuses.get(node) == wanted) {
                out.add(node);
            }
        }
        return out;
    }

    private static String pad(String s) {
        return pad(s, 12);
    }

    private static String pad(String s, int width) {
        return s.length() >= width ? s + " " : s + " ".repeat(width - s.length());
    }

    /** For diagnostics: the raw outcomes, keyed by node. */
    public Map<String, NodeOutcome> outcomes() {
        return new LinkedHashMap<>(outcomes);
    }
}
