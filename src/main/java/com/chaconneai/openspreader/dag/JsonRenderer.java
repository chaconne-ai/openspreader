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

import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

/**
 * A graph, or a finished run, as JSON.
 *
 * <p>For programs rather than people: a front end that draws its own picture, an audit record
 * of what a workflow looked like when it ran, a diff between two versions of one.
 *
 * <pre>{@code
 * {
 *   "graph" : "order-flow",
 *   "entries" : [ "Validate" ],
 *   "nodes" : [ {
 *     "name" : "Validate", "type" : "com.acme.Validate", "kind" : "node",
 *     "entry" : true, "local" : false, "trigger" : "ALL", "retries" : 0
 *   } ],
 *   "edges" : [ {
 *     "from" : "Validate", "to" : "Charge", "kind" : "plain", "condition" : "ON_SUCCESS"
 *   } ]
 * }
 * }</pre>
 *
 * <h2>Jackson, not a parser of our own</h2>
 * An earlier version of this class wrote and parsed JSON by hand, to avoid putting a
 * dependency on every application that uses this engine. That reasoning does not survive
 * contact with the facts: <b>Jackson is standard equipment in a Spring Boot application</b>,
 * carried in by web, by the actuator, by half the starters. The dependency was theoretical
 * and the hand-written parser was real, and a parser is exactly the kind of code that is
 * fine until somebody hand-edits a file.
 *
 * <p>It is declared {@code optional} all the same, so nothing is forced on an application
 * that never writes a graph out.
 *
 * <p>The <b>shape</b> of a definition is not decided here: {@link GraphModels#toTree} lays it
 * out and this class only chooses the syntax. That is what keeps JSON and YAML honest with
 * each other.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 13/09/2026
 */
public class JsonRenderer implements GraphRenderer {

    /** Stateless, so one shared instance is enough. */
    public static final JsonRenderer INSTANCE = new JsonRenderer();

    /**
     * One mapper, configured once.
     *
     * <p>Nothing about the application's own Jackson configuration reaches this: a definition
     * is not somebody's REST payload, and a naming strategy or a global date format set for
     * an API would silently change what a stored graph looks like.
     */
    private static final ObjectMapper MAPPER = JsonMapper.builder()
            // Jackson keeps this off so that a malformed payload cannot leak into an
            // exception. Here the text is a workflow definition somebody is editing, and
            // without it a parse error says only "unexpected end of input" with no position,
            // which is no help at all. The content itself still never reaches the message;
            // see load(), which takes the line and column and leaves the source alone
            .enable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
            .build();

    @Override
    public String render(CompiledGraph graph, Map<String, NodeStatus> statuses) {
        return MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(GraphModels.toTree(GraphModel.of(graph, statuses)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public StateGraph load(String text, GraphCatalog catalog) {
        if (text == null || text.isBlank()) {
            throw new DagException("there is no definition here to load");
        }
        Map<String, Object> tree;
        try {
            tree = MAPPER.readValue(text, Map.class);
        } catch (JacksonException e) {
            // The position, not the source. A definition may hold an API key in a header,
            // and a parse error is no reason to copy it into a log
            throw new DagException("this is not valid JSON: " + e.getOriginalMessage()
                    + ", at line " + e.getLocation().getLineNr()
                    + " column " + e.getLocation().getColumnNr(), e);
        }
        return GraphModels.toStateGraph(GraphModels.fromTree(tree), catalog);
    }
}
