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

import java.util.HashMap;
import java.util.Map;

/**
 * Supplies the things a written-out graph cannot carry, so that it can be read back.
 *
 * <p>A graph is a structure plus some <b>code</b>: node classes, reducers, conditions. The
 * structure survives being written to JSON or a database column; the code does not. This is
 * how it comes back.
 *
 * <pre>{@code
 * GraphCatalog catalog = GraphCatalog.builder()
 *         .node(Validate.class)                 // by its own class name
 *         .node("Charge", ChargeCard.class)     // or under another name
 *         .reducer(Reducers.concatList())       // named reducers, looked up by name
 *         .build();
 *
 * StateGraph graph = RendererType.JSON.renderer().load(json, catalog);
 * CompiledGraph flow = dag.bind(graph.compile());
 * }</pre>
 *
 * <h2>Why classes are looked up and not resolved</h2>
 * There is no {@code Class.forName} behind this. A definition read from a database is
 * <b>data</b>, and turning a string in a data row into a loaded class is how data becomes
 * code. What the catalog holds is what the application has already decided it is willing to
 * run, which is the same reasoning the node dispatcher uses for the cluster.
 *
 * <h2>What cannot come back at all</h2>
 * <ul>
 *   <li><b>Anonymous reducers.</b> A lambda has no name to store. {@link Reducers}' factories
 *       all name themselves, and {@link Reducer#named} names one of your own</li>
 *   <li><b>Lambda conditions.</b> {@code switchOn(state -> ...)} is code. Write conditions as
 *       SpEL, {@code switchOn("#risk > 80")}, and they are text and survive</li>
 *   <li><b>Listeners.</b> Deliberately: they are a run-time concern, attached where the graph
 *       is used rather than where it is stored</li>
 * </ul>
 * Loading a graph that needs one of the first two fails with a message naming the channel or
 * the node, rather than producing a graph that is quietly missing a rule.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 13/09/2026
 */
public interface GraphCatalog {

    /** @return the class for this node type name, or null when the application does not offer it */
    Class<? extends GraphNode> nodeClass(String typeName);

    /** @return the reducer under this name, or null */
    Reducer<?> reducer(String name);

    static Builder builder() {
        return new Builder();
    }

    /** Collects what an application is willing to load. */
    class Builder {

        private final Map<String, Class<? extends GraphNode>> nodes = new HashMap<>();
        private final Map<String, Reducer<?>> reducers = new HashMap<>();

        private Builder() {
            // The built-in reducers are always available: a graph that used one of them would
            // otherwise need every application to re-register the same eight names
            for (Reducer<?> reducer : new Reducer<?>[]{
                    Reducers.lastWins(), Reducers.firstWins(), Reducers.concatList(),
                    Reducers.unionSet(), Reducers.mergeMap(), Reducers.sumLong(),
                    Reducers.sumInt(), Reducers.writeOnce("")}) {
                reducers.put(reducer.name(), reducer);
            }
        }

        /** Offers a node class under its own fully qualified name. */
        public Builder node(Class<? extends GraphNode> type) {
            nodes.put(type.getName(), type);
            return this;
        }

        /** Offers a node class under a name of your choosing, for a definition that uses one. */
        public Builder node(String typeName, Class<? extends GraphNode> type) {
            nodes.put(typeName, type);
            return this;
        }

        /** Offers a reducer. It must have a {@link Reducer#name()}. */
        public Builder reducer(Reducer<?> reducer) {
            if (reducer.name() == null) {
                throw new IllegalArgumentException("a catalog reducer needs a name; wrap it "
                        + "with Reducer.named(\"...\", reducer)");
            }
            reducers.put(reducer.name(), reducer);
            return this;
        }

        public GraphCatalog build() {
            Map<String, Class<? extends GraphNode>> frozenNodes = Map.copyOf(nodes);
            Map<String, Reducer<?>> frozenReducers = Map.copyOf(reducers);
            return new GraphCatalog() {
                @Override
                public Class<? extends GraphNode> nodeClass(String typeName) {
                    return frozenNodes.get(typeName);
                }

                @Override
                public Reducer<?> reducer(String name) {
                    return frozenReducers.get(name);
                }
            };
        }
    }
}
