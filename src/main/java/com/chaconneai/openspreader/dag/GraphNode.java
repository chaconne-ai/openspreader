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
import java.util.Set;

/**
 * One step of a graph. Extend it, register the subclass as a Spring bean, and put the class
 * into a {@link StateGraph}.
 *
 * <pre>{@code
 * @Component
 * public class ChargeCard extends GraphNode {
 *
 *     private final PaymentClient payments;   // ordinary injection; this is a bean
 *
 *     @Override
 *     public Map<String, Object> execute(GraphState s) {
 *         String orderId = s.getString("orderId");
 *         return Map.of("receipt", payments.charge(orderId));
 *     }
 * }
 * }</pre>
 *
 * <h2>Why an abstract class and not a lambda</h2>
 * Because a node runs <b>in another process</b>. A lambda cannot be addressed across the
 * cluster; a bean can. The engine sends the class name, and whichever replica picks the work
 * up resolves its own bean of that type. So a node is free to inject repositories, clients
 * and configuration as any bean does, and none of that has to cross the network.
 *
 * <p>The consequence is worth stating plainly: <b>every replica needs this class, at the
 * same version</b>. That is the same constraint {@code ProcessingPool} and
 * {@code MapReduceJob} carry, and for the same reason.
 *
 * <h2>Return only what changed</h2>
 * {@link #execute} returns a <b>partial update</b>, not a whole state. That is what lets two
 * branches run at once: they touch different channels and cannot conflict, and where they do
 * touch one channel the graph had to declare a {@link Reducer} for it before it was allowed
 * to run.
 *
 * <p>Returning {@code null} or an empty map is perfectly legitimate: a node that only has a
 * side effect changes no channel.
 *
 * <h2>The whole state travels</h2>
 * A node is handed every channel, not a declared subset. There was a {@code reads()} hook for
 * that and it has been removed, because it could not work: the coordinator decides what to
 * send and <b>has no instance of this class</b> to ask, the node being a bean that lives on
 * the executing replica. The only way it could have read the declaration was to construct one
 * reflectively, which fails on every node with a dependency, and failed silently.
 *
 * <p>So keep the state small, and keep large payloads out of channels. Declaring the slice on
 * the graph, where the coordinator can see it, is the way to bring the optimisation back.
 *
 * <h2>Values cross the network</h2>
 * Channel values are serialised by the configured {@code ObjectCodec}
 * ({@code spring.spreader.multiprocessing.serialization}), so under the default JDK
 * serialisation they must implement {@link java.io.Serializable}. Do not put a file handle,
 * a database connection or a Spring bean into a channel; put the data in, and let each node
 * reach its own resources.
 *
 * <h2>It is one of two shapes</h2>
 * This one is a bean that the engine <b>dispatches across the cluster</b>. The other is
 * {@link ExternalNode}, for work that happens outside the application and therefore has no
 * reason to be shipped anywhere. Both are {@link NodeInvoker}s, and the engine only ever sees
 * that interface.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public abstract class GraphNode implements NodeInvoker {

    /**
     * Does this step's work.
     *
     * @param state the run's channels, as they stand at the moment this node became ready.
     *              Read-only
     * @return the channels this node changed, or null / an empty map for none
     * @throws Exception anything at all. It is handed back on {@code RunResult#failure()}
     *                   <b>unwrapped</b>, so the caller catches what was really thrown
     */
    @Override
    public abstract Map<String, Object> execute(GraphState state) throws Exception;

    /** The name this node carries in a graph unless one is given explicitly. */
    public String defaultName() {
        return getClass().getSimpleName();
    }
}
