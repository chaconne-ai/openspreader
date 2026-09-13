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
 * What a node actually does. The one interface the engine calls.
 *
 * <p>It exists so that a step is not required to be local Java. A node may be:
 *
 * <ul>
 *   <li><b>a Spring bean</b>: extend {@link GraphNode}, inject what it needs, and the engine
 *       dispatches it to whichever replica has capacity</li>
 *   <li><b>a call to something outside</b>: an HTTP API, a gRPC service, a queue, a stored
 *       procedure. Extend {@link ExternalNode}, which runs on the coordinating instance
 *       because shipping a remote call to another replica only adds a hop</li>
 *   <li><b>anything else</b>: implement this interface directly and the engine does not
 *       care what is behind it</li>
 * </ul>
 *
 * <h2>The contract is the same either way</h2>
 * Read the state, return <b>only the channels that changed</b>. That is what lets branches
 * run at once, and it does not stop being true because the work happened in another process
 * or another company's data centre.
 *
 * <h2>Where it runs is declared on the graph, not here</h2>
 * There is no {@code mode()} method on this interface, and that is deliberate. The
 * coordinator decides where to send a node and <b>has no instance to ask</b>, the node being
 * a bean that lives on the executing replica. A declaration here could only be read by
 * constructing one reflectively, which fails on anything with a dependency and fails
 * silently. So it is {@code StateGraph.local(...)} that marks a node as running in place.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 13/09/2026
 */
@FunctionalInterface
public interface NodeInvoker {

    /**
     * Does the step's work.
     *
     * @param state the run's channels as they stand. Read-only
     * @return the channels this step changed, or null / an empty map for none
     * @throws Exception anything at all. It reaches {@code RunResult#failure()} unwrapped
     */
    Map<String, Object> execute(GraphState state) throws Exception;
}
