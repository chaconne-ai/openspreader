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

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.representer.Representer;

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
 * - Validate
 * nodes:
 * - name: Validate
 *   type: com.acme.Validate
 *   kind: node
 *   entry: true
 *   trigger: ALL
 *   retries: 0
 * }</pre>
 *
 * <h2>SnakeYAML, and specifically its safe constructor</h2>
 * An earlier version parsed a small subset of YAML by hand. That was the weaker half of a
 * weak idea: the whole point of YAML here is that a person edits it, and a person editing
 * YAML will sooner or later use an anchor, a flow collection or a folded scalar, none of
 * which a subset parser understands.
 *
 * <p>SnakeYAML is standard equipment in a Spring Boot application, which reads
 * {@code application.yml} with it. It is declared {@code optional} all the same.
 *
 * <p><b>{@link SafeConstructor}, and that is not a detail.</b> SnakeYAML's default
 * constructor will instantiate whatever class a {@code !!com.acme.Thing} tag names. A
 * definition read back from a database is <b>data</b>, and letting data name a class to
 * instantiate is the same door {@link GraphCatalog} exists to keep shut. The safe
 * constructor builds maps, lists and scalars, and nothing else.
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
        DumperOptions options = new DumperOptions();
        // Block style: the whole reason to choose YAML over JSON is that it diffs line by
        // line, and flow style would put a node back on one line
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        // Not prettyFlow: it spreads an empty list over two lines, and "inputs: []" said on
        // one line is both shorter and clearer
        options.setSplitLines(false);
        Yaml yaml = new Yaml(new Representer(options), options);
        return yaml.dump(GraphModels.toTree(GraphModel.of(graph, statuses)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public StateGraph load(String text, GraphCatalog catalog) {
        if (text == null || text.isBlank()) {
            throw new DagException("there is no definition here to load");
        }
        Object tree;
        try {
            tree = new Yaml(new SafeConstructor(new LoaderOptions())).load(text);
        } catch (YAMLException e) {
            throw new DagException("this is not valid YAML: " + e.getMessage(), e);
        }
        if (!(tree instanceof Map<?, ?>)) {
            throw new DagException("a graph definition is a mapping, found "
                    + (tree == null ? "nothing" : tree.getClass().getSimpleName()));
        }
        return GraphModels.toStateGraph(
                GraphModels.fromTree((Map<String, Object>) tree), catalog);
    }
}
