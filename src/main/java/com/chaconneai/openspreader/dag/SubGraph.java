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

import java.util.Map;

/**
 * A node that is itself a whole graph.
 *
 * <pre>{@code
 * @Component
 * public class Fulfilment extends SubGraph {
 *
 *     private final CompiledGraph inner = StateGraph.create("fulfilment")
 *             .channel("parcels", Reducers.concatList())
 *             .from(PickStock.class).to(PackBox.class, PrintLabel.class)
 *             .from(PackBox.class, PrintLabel.class).to(HandToCourier.class)
 *             .entry(PickStock.class)
 *             .compile();
 *
 *     @Override
 *     protected CompiledGraph graph() {
 *         return inner;
 *     }
 * }
 * }</pre>
 *
 * <p>Composition is the point: a workflow that has grown past what one picture can hold gets
 * split into named pieces, each of which is a graph in its own right and can be tested on its
 * own.
 *
 * <h2>One channel namespace, shared</h2>
 * The inner graph sees the outer state and its updates merge into the outer state by the
 * outer graph's reducers. There is deliberately <b>no input/output mapping</b> in this
 * version: one namespace is one mental model, and a second one would have to be explained,
 * validated and rendered everywhere the first is. Where isolation really is needed, give the
 * inner channels distinct names.
 *
 * <h2>The inner graph is built locally, and that is what makes it work</h2>
 * A {@link CompiledGraph} holds lambdas (reducers, routers) and cannot cross the network. It
 * does not have to: this is a bean, so <b>every replica builds its own identical copy</b>.
 * Whichever replica runs this node becomes the coordinator of the inner run and dispatches
 * the inner nodes onward in the ordinary way. Nesting therefore costs nothing structurally.
 *
 * <p>Build it once, in a field, as above. Building it inside {@link #graph()} would recompile
 * and revalidate the whole subgraph on every execution.
 *
 * <h2>How deep is safe</h2>
 * A subgraph node occupies an inbound pool thread on the replica running it, and blocks there
 * while the inner nodes are dispatched. Those inner nodes can land back on the same replica,
 * which means nesting consumes inbound threads in proportion to its depth.
 *
 * <p>One or two levels against the default pool size is comfortable. Deep nesting is not, and
 * the failure mode is a stall rather than an error: every inbound thread waiting for inner
 * work that has nowhere to run. If a graph needs many levels, raise
 * {@code spring.spreader.multiprocessing.pooling.parallelism} to match, or flatten the middle
 * levels into ordinary nodes.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public abstract class SubGraph extends GraphNode {

    /** The graph this node stands for. Called once per execution, so hold it in a field. */
    protected abstract CompiledGraph graph();

    /**
     * Runs the inner graph and reports what it changed.
     *
     * <p>A failure inside the inner graph surfaces as this node failing, carrying the inner
     * node's exception as its cause, so the outer {@code RunResult} points at the subgraph
     * and the stack trace points inside it.
     */
    @Override
    public final Map<String, Object> execute(GraphState state) throws Exception {
        RunResult inner = graph().invoke(state);
        if (inner.failed()) {
            throw new DagException("subgraph " + graph().name() + " failed at node "
                    + inner.failedNode(), inner.failure());
        }
        return inner.state().asMap();
    }
}
