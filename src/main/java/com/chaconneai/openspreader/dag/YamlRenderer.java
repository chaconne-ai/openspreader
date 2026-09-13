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
 * A graph, or a finished run, as YAML.
 *
 * <p>The same structure {@link JsonRenderer} writes, in a shape a person can read without a
 * tool. It suits a workflow definition kept in version control, where the diff between two
 * revisions has to be legible in a pull request.
 *
 * <pre>{@code
 * graph: order-flow
 * entries:
 *   - Validate
 * inputs:
 *   - orderId
 * nodes:
 *   - name: Validate
 *     type: com.acme.Validate
 *     kind: node
 *     entry: true
 *     local: false
 *     trigger: ALL
 *     retries: 0
 *     status: SUCCESS
 * edges:
 *   - from: Charge
 *     to: Refund
 *     kind: plain
 *     condition: ON_FAILURE
 * }</pre>
 *
 * <p>Written by hand for the reason given on {@link JsonRenderer}: the structure is closed and
 * produced here, so the only real work is quoting, and a library would cost every downstream
 * application a dependency.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 13/09/2026
 */
public class YamlRenderer implements GraphRenderer {

    /** Stateless, so one shared instance is enough. */
    public static final YamlRenderer INSTANCE = new YamlRenderer();

    @Override
    public String render(CompiledGraph graph, Map<String, NodeStatus> statuses) {
        GraphModel model = GraphModel.of(graph, statuses);
        StringBuilder sb = new StringBuilder(512);

        sb.append("graph: ").append(scalar(model.graph())).append('\n');
        appendList(sb, "entries", model.entries());
        appendList(sb, "inputs", model.inputs());

        sb.append("channels:\n");
        for (GraphModel.ChannelView channel : model.channels()) {
            sb.append("  - name: ").append(scalar(channel.name())).append('\n');
            sb.append("    reducer: ").append(scalar(channel.reducer())).append('\n');
        }

        sb.append("nodes:\n");
        for (GraphModel.NodeView node : model.nodes()) {
            sb.append("  - name: ").append(scalar(node.name())).append('\n');
            sb.append("    type: ").append(scalar(node.type())).append('\n');
            sb.append("    kind: ").append(scalar(node.kind())).append('\n');
            sb.append("    entry: ").append(node.entry()).append('\n');
            sb.append("    local: ").append(node.local()).append('\n');
            sb.append("    trigger: ").append(scalar(node.trigger())).append('\n');
            sb.append("    retries: ").append(node.retries()).append('\n');
            if (node.status() != null) {
                sb.append("    status: ").append(node.status().name()).append('\n');
            }
        }

        sb.append("edges:\n");
        for (GraphModel.EdgeView edge : model.edges()) {
            sb.append("  - from: ").append(scalar(edge.from())).append('\n');
            sb.append("    to: ").append(scalar(edge.to())).append('\n');
            sb.append("    kind: ").append(scalar(edge.kind())).append('\n');
            sb.append("    condition: ").append(edge.condition()).append('\n');
            if (edge.branch() != null) {
                sb.append("    branch: ").append(scalar(edge.branch())).append('\n');
            }
        }

        // The switches, kept whole. The edges above have the same branches flattened, which is
        // what a drawing wants; this is what reading one back wants
        sb.append("conditionals:\n");
        for (GraphModel.ConditionalView conditional : model.conditionals()) {
            sb.append("  - sources:\n");
            for (String source : conditional.sources()) {
                sb.append("      - ").append(scalar(source)).append('\n');
            }
            sb.append("    form: ").append(scalar(conditional.form())).append('\n');
            sb.append("    expression: ").append(scalar(conditional.expression())).append('\n');
            appendList(sb, "    predicates", conditional.predicates());
            sb.append("    branches:\n");
            conditional.branches().forEach((key, targets) -> {
                sb.append("      - key: ").append(scalar(key)).append('\n');
                sb.append("        targets:\n");
                for (String target : targets) {
                    sb.append("          - ").append(scalar(target)).append('\n');
                }
            });
            appendList(sb, "    else", conditional.elseTargets());
        }

        return sb.toString();
    }

    @Override
    public StateGraph load(String text, GraphCatalog catalog) {
        if (text == null || text.isBlank()) {
            throw new DagException("there is no definition here to load");
        }
        Object tree = new Parser(text).parse();
        if (!(tree instanceof Map<?, ?>)) {
            throw new DagException("a graph definition is a mapping, found "
                    + (tree == null ? "nothing" : tree.getClass().getSimpleName()));
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> root = (Map<String, Object>) tree;
        return GraphModels.toStateGraph(GraphModels.fromTree(root), catalog);
    }

    /**
     * A list under a key, or {@code []} when there is nothing in it.
     *
     * <p>{@code key} carries its own leading spaces, so the items line up two further in
     * whatever depth it is written at.
     */
    private static void appendList(StringBuilder sb, String key, List<String> values) {
        sb.append(key).append(':');
        if (values.isEmpty()) {
            sb.append(" []\n");
            return;
        }
        sb.append('\n');
        String indent = " ".repeat(key.length() - key.stripLeading().length() + 2);
        for (String value : values) {
            sb.append(indent).append("- ").append(scalar(value)).append('\n');
        }
    }

    /**
     * A YAML scalar, quoted when it has to be.
     *
     * <p>Node names come from class names and are nearly always plain, but the string-named
     * overloads of the builder accept anything. An unquoted value containing a colon or
     * starting with a dash parses as something else entirely, which is the kind of output
     * that looks right and is not.
     */
    private static String scalar(String text) {
        if (text == null) {
            return "null";
        }
        if (text.isEmpty()) {
            return "\"\"";
        }
        boolean needsQuoting = text.chars().anyMatch(c ->
                c == ':' || c == '#' || c == '\'' || c == '"' || c == '\n' || c == '\r'
                        || c == '\t' || c == '{' || c == '}' || c == '[' || c == ']'
                        || c == ',' || c == '&' || c == '*' || c == '%' || c == '@'
                        || c == '`' || c == '|' || c == '>' || c == '!');
        needsQuoting |= text.startsWith("-") || text.startsWith(" ") || text.endsWith(" ");
        // Otherwise a node actually called "null" would read back as an absent value, and one
        // called "true" as a boolean
        needsQuoting |= "null".equals(text) || "true".equals(text) || "false".equals(text)
                || "[]".equals(text);
        if (!needsQuoting) {
            return text;
        }
        return '"' + text.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + '"';
    }

    /**
     * Just enough YAML to read back what is written above.
     *
     * <p>Block mappings, block sequences and scalars, nested to any depth: the shape this
     * renderer emits, and the shape a person editing that output by hand would keep. Flow
     * collections, anchors, multi-document files and the rest of YAML are not here, and a
     * definition using them is rejected rather than half-understood.
     *
     * <p>Hand-written for the reason given on {@link JsonRenderer}: a parser library would
     * cost every downstream application a dependency, and this is one page.
     */
    private static final class Parser {

        /** One meaningful line: how far in it starts, and what it says. */
        private record Line(int indent, String text, int number) {
        }

        private final List<Line> lines = new ArrayList<>();
        private int at;

        Parser(String text) {
            String[] raw = text.split("\n", -1);
            for (int i = 0; i < raw.length; i++) {
                String line = raw[i];
                String stripped = line.stripLeading();
                if (stripped.isEmpty() || stripped.startsWith("#")) {
                    continue;
                }
                if (line.indexOf('\t') >= 0 && line.indexOf('\t') < line.length()
                        - stripped.length()) {
                    throw new DagException("this is not valid YAML: line " + (i + 1)
                            + " is indented with a tab, which YAML does not allow");
                }
                lines.add(new Line(line.length() - stripped.length(), stripped, i + 1));
            }
        }

        Object parse() {
            if (lines.isEmpty()) {
                throw new DagException("there is nothing in this definition");
            }
            Object value = block(lines.get(0).indent());
            if (at < lines.size()) {
                throw new DagException("this is not valid YAML: line " + lines.get(at).number()
                        + " is indented back out to somewhere nothing was opened");
            }
            return value;
        }

        /** A mapping or a sequence, whichever starts at this depth. */
        private Object block(int indent) {
            if (at >= lines.size()) {
                return null;
            }
            return lines.get(at).text().startsWith("-") ? sequence(indent) : mapping(indent);
        }

        private Map<String, Object> mapping(int indent) {
            Map<String, Object> map = new LinkedHashMap<>();
            while (at < lines.size() && lines.get(at).indent() == indent
                    && !lines.get(at).text().startsWith("-")) {
                Line line = lines.get(at++);
                int colon = line.text().indexOf(':');
                if (colon < 0) {
                    throw new DagException("this is not valid YAML: line " + line.number()
                            + " is neither \"key: value\" nor a list item");
                }
                String key = line.text().substring(0, colon).trim();
                String rest = line.text().substring(colon + 1).trim();
                map.put(key, rest.isEmpty() ? nested(indent) : scalar(rest));
            }
            return map;
        }

        private List<Object> sequence(int indent) {
            List<Object> list = new ArrayList<>();
            while (at < lines.size() && lines.get(at).indent() == indent
                    && lines.get(at).text().startsWith("-")) {
                Line line = lines.get(at++);
                String rest = line.text().substring(1).trim();
                if (rest.isEmpty()) {
                    list.add(nested(indent));
                } else if (rest.startsWith("\"") || rest.indexOf(':') < 0) {
                    list.add(scalar(rest));
                } else {
                    // "- name: Validate" opens a mapping whose first pair sits on the dash.
                    // Putting it back as an ordinary line at the depth its siblings use lets
                    // one piece of code read the whole entry
                    int inner = indent + 2;
                    lines.add(at, new Line(inner, rest, line.number()));
                    list.add(mapping(inner));
                }
            }
            return list;
        }

        /** Whatever is written further in than this, or nothing at all. */
        private Object nested(int indent) {
            if (at >= lines.size() || lines.get(at).indent() <= indent) {
                return null;
            }
            return block(lines.get(at).indent());
        }

        private Object scalar(String raw) {
            if ("[]".equals(raw)) {
                return List.of();
            }
            if ("null".equals(raw) || "~".equals(raw)) {
                return null;
            }
            if (!raw.startsWith("\"")) {
                return raw;
            }
            if (raw.length() < 2 || !raw.endsWith("\"")) {
                throw new DagException("this is not valid YAML: an unterminated quoted value "
                        + raw);
            }
            String body = raw.substring(1, raw.length() - 1);
            StringBuilder sb = new StringBuilder(body.length());
            for (int i = 0; i < body.length(); i++) {
                char c = body.charAt(i);
                if (c != '\\' || i + 1 >= body.length()) {
                    sb.append(c);
                    continue;
                }
                char escaped = body.charAt(++i);
                switch (escaped) {
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    default -> sb.append(escaped);
                }
            }
            return sb.toString();
        }
    }
}
