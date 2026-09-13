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
 * A node whose work happens somewhere outside this application: an HTTP API, a gRPC service,
 * a queue, a database procedure.
 *
 * <pre>{@code
 * @Component
 * public class FetchRates extends ExternalNode {
 *
 *     private final RestClient http;      // ordinary injection
 *
 *     @Override
 *     protected Map<String, Object> call(GraphState state) {
 *         String day = state.getString("day");
 *         return Map.of("rates", http.get().uri("/rates/{d}", day).retrieve().body(Rates.class));
 *     }
 * }
 * }</pre>
 *
 * <p>Then mark it as running in place when the graph is built:
 *
 * <pre>{@code
 * .local(FetchRates.class)
 * }</pre>
 *
 * <h2>Why an external call should not be dispatched</h2>
 * Calling someone else's API from replica A and from replica B is the same call. Shipping it
 * to another replica adds a network hop, a serialisation of the state, and a second thing
 * that can fail, and buys nothing: the work was never going to happen locally anyway.
 *
 * <p>So an external node runs on the instance coordinating the run. That also means it does
 * <b>not</b> have to exist as a bean on every replica, which is the one constraint ordinary
 * nodes carry.
 *
 * <h2>It is a marker, not machinery</h2>
 * This class adds no behaviour beyond naming the intent; {@link #call} is
 * {@link NodeInvoker#execute} under a name that reads better at an API boundary. Extending it
 * does not by itself make a node local: {@code StateGraph.local(...)} does, for the reason
 * given on {@link NodeInvoker}. Extending this class and forgetting that call gets an
 * ordinary dispatched node, which still works, merely with a pointless hop.
 *
 * <h2>Two things worth remembering at the boundary</h2>
 * <ul>
 *   <li><b>Give it a timeout of its own.</b> The engine's ceiling is the whole run; a hung
 *       HTTP call with no client timeout will sit inside it</li>
 *   <li><b>Whatever comes back goes into a channel, so it crosses the network later.</b>
 *       Parse the response into something serialisable rather than putting a live response
 *       object in</li>
 * </ul>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 13/09/2026
 */
public abstract class ExternalNode extends GraphNode {

    /** The outward call. Named for what it is; see {@link NodeInvoker#execute}. */
    protected abstract Map<String, Object> call(GraphState state) throws Exception;

    @Override
    public final Map<String, Object> execute(GraphState state) throws Exception {
        return call(state);
    }
}
