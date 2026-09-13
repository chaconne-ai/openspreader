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
package com.chaconneai.openspreader.example.dag;

import com.chaconneai.openspreader.dag.CompiledGraph;
import com.chaconneai.openspreader.dag.ProcessingDag;
import com.chaconneai.openspreader.dag.GraphNode;
import com.chaconneai.openspreader.dag.GraphState;
import com.chaconneai.openspreader.dag.Reducers;
import com.chaconneai.openspreader.dag.RunResult;
import com.chaconneai.openspreader.dag.StateGraph;
import com.chaconneai.openspreader.dag.SubGraph;

import java.util.List;
import java.util.Map;

/**
 * One order, five shapes: everything this engine does, in a workflow that could be real.
 *
 * <p>The graph below:
 *
 * <pre>
 *                     Validate
 *                        |
 *            +-----------+-----------+          fan-out, in parallel
 *            |                       |
 *        Reserve                  Charge
 *            |                       |
 *            +-----------+-----------+          fan-in, waits for both
 *                        |
 *                   RiskScore
 *                    /       \                  conditional, one branch only
 *          risk>80  /         \  otherwise
 *         HumanReview        Fulfilment         a node that is itself a graph
 *                    \       /
 *                     Notify                    any-of, the first one is enough
 * </pre>
 *
 * <p>Every node here is a {@link GraphNode} bean, so every one of them may run on a
 * different instance of the application. Nothing in the code below says where anything runs,
 * which is the point.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class OrderFlowBestPractice {

    private final CompiledGraph flow;

    /**
     * Built once, in the constructor.
     *
     * <p>Compiling validates the whole graph: cycles, unreachable nodes, channels two
     * parallel branches write without a reducer. Doing it per request would pay that cost
     * per request and, worse, would turn a graph that is wrong into a run-time error rather
     * than a startup error.
     */
    public OrderFlowBestPractice(ProcessingDag dagger) {
        this.flow = dagger.bind(StateGraph.create("order-flow")

                // ------------------------------------------------------
                // Channels: how data merges when branches touch it
                // ------------------------------------------------------

                // Two parallel branches both append to it, so it needs a reducer.
                // Without one, compile() would refuse the graph rather than let one
                // branch's writes disappear
                .channel("steps", Reducers.concatList())

                // Only ever written once, by RiskScore. No reducer needed, and declaring
                // writeOnce makes a second writer an error instead of a silent overwrite
                .channel("risk", Reducers.writeOnce("risk"))

                // ------------------------------------------------------
                // Shape
                // ------------------------------------------------------

                // Fan-out: reserving stock and charging the card have nothing to do with
                // each other, so they go at once
                .from(Validate.class).to(Reserve.class, Charge.class)

                // Fan-in: scoring needs both to have happened
                .from(Reserve.class, Charge.class).to(RiskScore.class)

                // Conditional: exactly one of the two runs, the other is SKIPPED, and
                // SKIPPED is not a failure
                .from(RiskScore.class)
                    .when("#risk > 80").to(HumanReview.class)
                    .otherwise(Fulfilment.class)

                // Any-of: whichever path the order took, notify once. Without onAny() this
                // would wait for both, and one of them is never going to arrive
                .from(HumanReview.class, Fulfilment.class).to(Notify.class).onAny()

                .entry(Validate.class)
                .compile());
    }

    /**
     * Runs one order through.
     *
     * <p>A failure comes back rather than being thrown; see {@link RunResult}. Turning it
     * into an exception is one line, and doing it here rather than in the engine means the
     * six nodes that did succeed are still readable from {@code result} first.
     */
    public void place(String orderId) {
        RunResult result = flow.invoke(Map.of("orderId", orderId));
        if (result.failed()) {
            // describe() prints every node, its outcome and where it ran. It is what turns
            // "the order failed" into something actionable
            throw new IllegalStateException(result.describe(), result.failure());
        }
    }

    /**
     * The graph as text, in whatever format is configured.
     *
     * <p>One output rather than a method per format; see
     * {@code spring.spreader.multiprocessing.dag.renderer}. Mermaid for a README, JSON for a
     * front end, and {@code render(GraphRenderer)} for anything else.
     */
    public String diagram() {
        return flow.render();
    }

    /**
     * The same question, about one particular run, with each node's outcome included.
     *
     * <p>This is the one that gets used: a branching workflow is never asked "did it work",
     * it is asked "why did it go that way".
     */
    public String diagramOf(String orderId) {
        return flow.invoke(Map.of("orderId", orderId)).render();
    }

    // ==================================================================
    // The nodes. Each is an ordinary Spring bean.
    // ==================================================================

    /**
     * A node is a bean, so it injects whatever it needs.
     *
     * <p>The one thing that does <b>not</b> work is putting a resource into a channel. The
     * state crosses the network, so channels carry data; a repository, a connection or a
     * file handle stays in the bean, where each replica has its own.
     */
    // @Component
    public static class Validate extends GraphNode {

        // private final OrderRepository orders;   // injected as usual

        @Override
        public Map<String, Object> execute(GraphState state) {
            String orderId = state.getString("orderId");
            // orders.findById(orderId) ...
            return Map.of("steps", List.of("validated"), "amount", 4200L);
        }

    }

    // @Component
    public static class Reserve extends GraphNode {
        @Override
        public Map<String, Object> execute(GraphState state) {
            return Map.of("steps", List.of("stock-reserved"));
        }
    }

    // @Component
    public static class Charge extends GraphNode {
        @Override
        public Map<String, Object> execute(GraphState state) {
            long amount = state.getLong("amount");
            return Map.of("steps", List.of("charged:" + amount));
        }
    }

    // @Component
    public static class RiskScore extends GraphNode {

        @Override
        public Map<String, Object> execute(GraphState state) {
            long amount = state.getLong("amount");
            return Map.of("risk", amount > 1000 ? 90 : 10,
                    "steps", List.of("scored"));
        }
    }

    // @Component
    public static class HumanReview extends GraphNode {
        @Override
        public Map<String, Object> execute(GraphState state) {
            return Map.of("steps", List.of("queued-for-review"));
        }
    }

    // @Component
    public static class Notify extends GraphNode {
        @Override
        public Map<String, Object> execute(GraphState state) {
            // Reached by whichever branch finished first; see onAny(). It runs once
            return Map.of("steps", List.of("notified"));
        }
    }

    // ==================================================================
    // A node that is itself a graph
    // ==================================================================

    /**
     * Fulfilment is a workflow of its own, and appears in the order flow as one node.
     *
     * <p>That is the answer to a graph growing past what one picture can hold: name the
     * pieces, and let each be a graph that can be drawn and tested on its own.
     *
     * <p>The inner graph is built <b>once, in a field</b>. Building it inside
     * {@link #graph()} would recompile and revalidate the whole subgraph on every execution.
     */
    // @Component
    public static class Fulfilment extends SubGraph {

        private final CompiledGraph inner;

        public Fulfilment(ProcessingDag dagger) {
            this.inner = dagger.bind(StateGraph.create("fulfilment")
                    .channel("steps", Reducers.concatList())
                    .from(PickStock.class).to(PackBox.class, PrintLabel.class)
                    .from(PackBox.class, PrintLabel.class).to(HandToCourier.class)
                    .entry(PickStock.class)
                    .compile());
        }

        @Override
        protected CompiledGraph graph() {
            return inner;
        }
    }

    // @Component
    public static class PickStock extends GraphNode {
        @Override
        public Map<String, Object> execute(GraphState state) {
            return Map.of("steps", List.of("picked"));
        }
    }

    // @Component
    public static class PackBox extends GraphNode {
        @Override
        public Map<String, Object> execute(GraphState state) {
            return Map.of("steps", List.of("packed"));
        }
    }

    // @Component
    public static class PrintLabel extends GraphNode {
        @Override
        public Map<String, Object> execute(GraphState state) {
            return Map.of("steps", List.of("labelled"));
        }
    }

    // @Component
    public static class HandToCourier extends GraphNode {
        @Override
        public Map<String, Object> execute(GraphState state) {
            return Map.of("steps", List.of("collected"));
        }
    }
}
