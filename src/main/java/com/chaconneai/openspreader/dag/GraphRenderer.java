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
 * Writes a DAG definition out and reads one back: the graph's serialisation, the equivalent of
 * {@code ObjectCodec} for objects.
 *
 * <pre>{@code
 * // out, to a file or a database column
 * repository.save(workflowId, JsonRenderer.INSTANCE.render(flow));
 *
 * // and back
 * StateGraph definition = JsonRenderer.INSTANCE.load(repository.load(workflowId), catalog);
 * CompiledGraph flow = dag.bind(definition.compile());
 * }</pre>
 *
 * <h2>Two implementations, and why not four</h2>
 * JSON and YAML. Mermaid and DOT are <b>not</b> here, and that is deliberate: they are
 * pictures, they cannot be read back, and putting them behind this interface would mean a
 * {@code load} that throws. An interface whose implementations cannot do what it says is
 * worse than two smaller ideas. They live in {@link PrintUtils} instead, reached through
 * {@code CompiledGraph.toMermaid()} and {@code toDot()}.
 *
 * <h2>Loading gives back a builder, not a compiled graph</h2>
 * {@link #load} returns a {@link StateGraph}, so whatever the definition could not carry can
 * be added before {@code compile()}. Listeners above all: they are a run-time concern and
 * deliberately not serialised.
 *
 * <h2>What a definition cannot carry</h2>
 * Lambdas. A reducer written as one, a {@code switchOn(state -> ...)} router, a listener.
 * Named reducers and SpEL conditions are text and survive; see {@link GraphCatalog}, which is
 * also where the round-trip's limits are set out in full.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 13/09/2026
 */
public interface GraphRenderer {

    /**
     * The definition as text.
     *
     * @param statuses what became of each node, or null for the definition alone. A stored
     *                 definition wants null; an audit record of one particular run wants the
     *                 statuses, and they come back as a field rather than being lost
     */
    String render(CompiledGraph graph, Map<String, NodeStatus> statuses);

    /** The definition alone, with no run attached. This is what gets stored. */
    default String render(CompiledGraph graph) {
        return render(graph, null);
    }

    /**
     * Reads a definition back.
     *
     * @param catalog supplies what text cannot carry: node classes and named reducers
     * @throws DagException when the text is malformed, or needs something the catalog does not
     *                      offer. The message names what is missing rather than handing back a
     *                      graph that is quietly incomplete
     */
    StateGraph load(String text, GraphCatalog catalog);
}
