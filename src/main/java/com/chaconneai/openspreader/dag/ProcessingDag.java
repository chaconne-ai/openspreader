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

import java.util.Map;

/**
 * The entry point for running graphs, alongside {@code ProcessingPool} and
 * {@code ProcessingMapReduce}.
 *
 * <pre>{@code
 * @Service
 * public class OrderService {
 *
 *     private final CompiledGraph flow;
 *
 *     public OrderService(ProcessingDag dag) {
 *         // Built once, in the constructor: compiling validates the whole graph, and doing
 *         // it per request would revalidate it per request
 *         this.flow = dag.bind(StateGraph.create("order-flow")
 *                 .from(Validate.class).to(Reserve.class, Charge.class)
 *                 .from(Reserve.class, Charge.class).to(Ship.class)
 *                 .entry(Validate.class)
 *                 .compile());
 *     }
 *
 *     public void place(String orderId) {
 *         RunResult r = flow.invoke(Map.of("orderId", orderId));
 *         if (r.failed()) {
 *             throw new IllegalStateException(r.describe(), r.failure());
 *         }
 *     }
 * }
 * }</pre>
 *
 * <h2>Why binding is a separate step</h2>
 * A {@link CompiledGraph} is a value: it can be built in a static field, in a unit test, or
 * anywhere there is no Spring context and no cluster, and it can be rendered and validated
 * there. {@link #bind} is what attaches the ability to actually run, and it returns a copy
 * rather than mutating, so the unbound graph stays usable as a value.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public interface ProcessingDag {

    /** The same graph, able to run. */
    CompiledGraph bind(CompiledGraph graph);

    /** Binds and runs in one call, with the configured default timeout. */
    RunResult invoke(CompiledGraph graph, Map<String, Object> input);

    /** Binds and runs in one call, with the configured default timeout. */
    RunResult invoke(CompiledGraph graph, GraphState input);

    /** The runtime, for a caller assembling graphs by hand. */
    DagRuntime runtime();

    /**
     * Asks a run going on <b>this instance</b> to stop.
     *
     * <p>No more nodes are dispatched and the run stops waiting for the ones already out. It
     * does not reach into another replica to interrupt work already running there: that node
     * may be halfway through a payment. Whatever finished is still reported, and
     * {@code RunResult.completed()} still says what a resume should skip.
     *
     * @return whether there was such a run to ask. False for one that has already finished,
     *         which is an ordinary race rather than an error
     */
    default boolean cancel(String runId) {
        return runtime().runs().cancel(runId);
    }

    /**
     * What the engine has been doing on this instance, counted.
     *
     * <p>The same numbers the metrics endpoint publishes, for an application that would
     * rather read them itself.
     */
    default DagStats stats() {
        return runtime().stats();
    }
}
