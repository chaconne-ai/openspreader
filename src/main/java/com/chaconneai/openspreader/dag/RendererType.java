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

/**
 * The serialisation formats that ship, for {@code CompiledGraph.render(GraphRenderer)}.
 *
 * <p>{@code CompiledGraph.render()} with no argument writes JSON. A format of your own is one
 * {@link GraphRenderer} implementation away, passed to {@code render(GraphRenderer)}.
 *
 * <p><b>Mermaid and DOT are not here</b>, because neither is a serialisation: they draw a
 * picture and cannot be read back. {@link PrintUtils} holds them, and
 * {@code CompiledGraph.toMermaid()} and {@code toDot()} reach them directly.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 13/09/2026
 */
public enum RendererType {

    /** For programs: a front end, an audit record, a diff between two workflow revisions. */
    JSON(JsonRenderer.INSTANCE),

    /** The same structure, legible in a pull request without a tool. */
    YAML(YamlRenderer.INSTANCE);

    private final GraphRenderer renderer;

    RendererType(GraphRenderer renderer) {
        this.renderer = renderer;
    }

    public GraphRenderer renderer() {
        return renderer;
    }
}
