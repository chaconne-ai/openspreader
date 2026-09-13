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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A graph, or a finished run, as JSON.
 *
 * <p>For programs rather than people: a front end that draws its own picture, an audit record
 * of what a workflow looked like when it ran, a diff between two versions of one.
 *
 * <pre>{@code
 * {
 *   "graph": "order-flow",
 *   "entries": ["Validate"],
 *   "inputs": ["orderId"],
 *   "nodes": [
 *     {"name":"Validate","type":"com.acme.Validate","kind":"node","entry":true,
 *      "local":false,"trigger":"ALL","retries":0,"status":"SUCCESS"}
 *   ],
 *   "edges": [
 *     {"from":"Validate","to":"Charge","kind":"plain","condition":"ON_SUCCESS"},
 *     {"from":"Charge","to":"Refund","kind":"plain","condition":"ON_FAILURE"},
 *     {"from":"Score","to":"Manual","kind":"conditional","condition":"ON_SUCCESS",
 *      "branch":"manual"}
 *   ]
 * }
 * }</pre>
 *
 * <h2>Written by hand, and on purpose</h2>
 * No JSON library is used, and none is added to this project's dependencies for it. What is
 * being written is a closed structure of strings, enum names, integers and booleans, all of
 * it produced here rather than supplied by a caller, so the only real work is escaping the
 * strings. Pulling in a mapper to do that would put a version of somebody else's library into
 * every application that uses this engine, to save thirty lines.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 13/09/2026
 */
public class JsonRenderer implements GraphRenderer {

    /** Stateless, so one shared instance is enough. */
    public static final JsonRenderer INSTANCE = new JsonRenderer();

    @Override
    public String render(CompiledGraph graph, Map<String, NodeStatus> statuses) {
        GraphModel model = GraphModel.of(graph, statuses);
        StringBuilder sb = new StringBuilder(512);

        sb.append("{\n");
        sb.append("  \"graph\": ").append(quote(model.graph())).append(",\n");
        sb.append("  \"entries\": ").append(array(model.entries())).append(",\n");
        sb.append("  \"inputs\": ").append(array(model.inputs())).append(",\n");

        sb.append("  \"channels\": [\n");
        for (int i = 0; i < model.channels().size(); i++) {
            GraphModel.ChannelView channel = model.channels().get(i);
            sb.append("    {")
                    .append("\"name\": ").append(quote(channel.name()))
                    .append(", \"reducer\": ").append(quote(channel.reducer()))
                    .append('}').append(i < model.channels().size() - 1 ? ",\n" : "\n");
        }
        sb.append("  ],\n");

        sb.append("  \"nodes\": [\n");
        for (int i = 0; i < model.nodes().size(); i++) {
            GraphModel.NodeView node = model.nodes().get(i);
            sb.append("    {")
                    .append("\"name\": ").append(quote(node.name()))
                    .append(", \"type\": ").append(quote(node.type()))
                    .append(", \"kind\": ").append(quote(node.kind()))
                    .append(", \"entry\": ").append(node.entry())
                    .append(", \"local\": ").append(node.local())
                    .append(", \"trigger\": ").append(quote(node.trigger()))
                    .append(", \"retries\": ").append(node.retries());
            if (node.status() != null) {
                sb.append(", \"status\": ").append(quote(node.status().name()));
            }
            sb.append('}').append(i < model.nodes().size() - 1 ? ",\n" : "\n");
        }
        sb.append("  ],\n");

        sb.append("  \"edges\": [\n");
        for (int i = 0; i < model.edges().size(); i++) {
            GraphModel.EdgeView edge = model.edges().get(i);
            sb.append("    {")
                    .append("\"from\": ").append(quote(edge.from()))
                    .append(", \"to\": ").append(quote(edge.to()))
                    .append(", \"kind\": ").append(quote(edge.kind()))
                    .append(", \"condition\": ").append(quote(edge.condition()));
            if (edge.branch() != null) {
                sb.append(", \"branch\": ").append(quote(edge.branch()));
            }
            sb.append('}').append(i < model.edges().size() - 1 ? ",\n" : "\n");
        }
        sb.append("  ],\n");

        // The switches, kept whole. The edges above have the same branches flattened, which is
        // what a drawing wants; this is what reading one back wants
        sb.append("  \"conditionals\": [\n");
        for (int i = 0; i < model.conditionals().size(); i++) {
            GraphModel.ConditionalView conditional = model.conditionals().get(i);
            sb.append("    {")
                    .append("\"sources\": ").append(array(conditional.sources()))
                    .append(", \"form\": ").append(quote(conditional.form()))
                    .append(", \"expression\": ").append(quote(conditional.expression()))
                    .append(", \"predicates\": ").append(array(conditional.predicates()))
                    .append(", \"branches\": [");
            int b = 0;
            for (Map.Entry<String, List<String>> branch : conditional.branches().entrySet()) {
                sb.append(b++ > 0 ? ", " : "")
                        .append("{\"key\": ").append(quote(branch.getKey()))
                        .append(", \"targets\": ").append(array(branch.getValue()))
                        .append('}');
            }
            sb.append(']')
                    .append(", \"else\": ").append(array(conditional.elseTargets()))
                    .append('}').append(i < model.conditionals().size() - 1 ? ",\n" : "\n");
        }
        sb.append("  ]\n");

        sb.append("}\n");
        return sb.toString();
    }

    @Override
    public StateGraph load(String text, GraphCatalog catalog) {
        if (text == null || text.isBlank()) {
            throw new DagException("there is no definition here to load");
        }
        Object tree = new Parser(text).parse();
        if (!(tree instanceof Map<?, ?>)) {
            throw new DagException("a graph definition is a JSON object, found "
                    + (tree == null ? "null" : tree.getClass().getSimpleName()));
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> root = (Map<String, Object>) tree;
        return GraphModels.toStateGraph(GraphModels.fromTree(root), catalog);
    }

    private static String array(List<String> values) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            sb.append(quote(values.get(i)));
            if (i < values.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.append(']').toString();
    }

    /**
     * A JSON string literal.
     *
     * <p>The control-character branch is not decoration: node names come from the
     * string-named overloads of the builder, which accept anything, and one stray newline
     * would produce output that parses as something else entirely.
     */
    private static String quote(String text) {
        if (text == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(text.length() + 2).append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    /**
     * Just enough JSON to read back what is written above.
     *
     * <p>Objects, arrays, strings, numbers, booleans and null, which is the whole of JSON's
     * grammar bar the parts nothing here emits. Hand-written for the same reason the writer
     * is: a mapper would put a version of somebody else's library into every application that
     * uses this engine, and the alternative is this one page.
     *
     * <p>It is strict on purpose. A definition that has been hand-edited into something
     * malformed should say where, not be half-read into a graph missing an edge.
     */
    private static final class Parser {

        private final String text;
        private int at;

        Parser(String text) {
            this.text = text;
        }

        Object parse() {
            Object value = value();
            skipSpace();
            if (at < text.length()) {
                throw error("unexpected trailing text");
            }
            return value;
        }

        private Object value() {
            skipSpace();
            if (at >= text.length()) {
                throw error("the definition ends early");
            }
            char c = text.charAt(at);
            return switch (c) {
                case '{' -> object();
                case '[' -> array();
                case '"' -> string();
                case 't', 'f' -> bool();
                case 'n' -> literal("null", null);
                default -> number();
            };
        }

        private Map<String, Object> object() {
            Map<String, Object> map = new LinkedHashMap<>();
            expect('{');
            skipSpace();
            if (peek() == '}') {
                at++;
                return map;
            }
            while (true) {
                skipSpace();
                String key = string();
                skipSpace();
                expect(':');
                map.put(key, value());
                skipSpace();
                char c = next();
                if (c == '}') {
                    return map;
                }
                if (c != ',') {
                    throw error("expected , or } in an object");
                }
            }
        }

        private List<Object> array() {
            List<Object> list = new ArrayList<>();
            expect('[');
            skipSpace();
            if (peek() == ']') {
                at++;
                return list;
            }
            while (true) {
                list.add(value());
                skipSpace();
                char c = next();
                if (c == ']') {
                    return list;
                }
                if (c != ',') {
                    throw error("expected , or ] in an array");
                }
            }
        }

        private String string() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = next();
                if (c == '"') {
                    return sb.toString();
                }
                if (c != '\\') {
                    sb.append(c);
                    continue;
                }
                char escaped = next();
                switch (escaped) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'u' -> {
                        if (at + 4 > text.length()) {
                            throw error("a truncated \\u escape");
                        }
                        sb.append((char) Integer.parseInt(text.substring(at, at + 4), 16));
                        at += 4;
                    }
                    default -> throw error("unknown escape \\" + escaped);
                }
            }
        }

        private Object bool() {
            return peek() == 't' ? literal("true", Boolean.TRUE) : literal("false", Boolean.FALSE);
        }

        private Object literal(String word, Object value) {
            if (!text.startsWith(word, at)) {
                throw error("expected " + word);
            }
            at += word.length();
            return value;
        }

        private Object number() {
            int start = at;
            while (at < text.length() && "+-.eE0123456789".indexOf(text.charAt(at)) >= 0) {
                at++;
            }
            String literal = text.substring(start, at);
            if (literal.isEmpty()) {
                throw error("expected a value");
            }
            try {
                return literal.contains(".") || literal.contains("e") || literal.contains("E")
                        ? (Object) Double.valueOf(literal)
                        : (Object) Long.valueOf(literal);
            } catch (NumberFormatException e) {
                throw error("\"" + literal + "\" is not a number");
            }
        }

        private void skipSpace() {
            while (at < text.length() && Character.isWhitespace(text.charAt(at))) {
                at++;
            }
        }

        private char peek() {
            if (at >= text.length()) {
                throw error("the definition ends early");
            }
            return text.charAt(at);
        }

        private char next() {
            char c = peek();
            at++;
            return c;
        }

        private void expect(char c) {
            if (next() != c) {
                at--;
                throw error("expected " + c);
            }
        }

        /** Says where, because a hand-edited definition is exactly where this gets used. */
        private DagException error(String what) {
            int line = 1;
            for (int i = 0; i < Math.min(at, text.length()); i++) {
                if (text.charAt(i) == '\n') {
                    line++;
                }
            }
            return new DagException("this is not valid JSON: " + what + ", at line " + line);
        }
    }
}
