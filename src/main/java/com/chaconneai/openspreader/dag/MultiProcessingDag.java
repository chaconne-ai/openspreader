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

import com.chaconneai.openspreader.dag.DagRuntime;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A {@link ProcessingDag}. Thin: it holds the runtime and the configured default timeout, and
 * everything else is {@link CompiledGraph}'s and the runner's.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class MultiProcessingDag implements ProcessingDag {

    private final DagRuntime runtime;
    private final long defaultTimeoutMs;

    public MultiProcessingDag(DagRuntime runtime, long defaultTimeoutMs) {
        this.runtime = runtime;
        this.defaultTimeoutMs = defaultTimeoutMs;
    }

    @Override
    public CompiledGraph bind(CompiledGraph graph) {
        return graph.withRuntime(runtime);
    }

    @Override
    public RunResult invoke(CompiledGraph graph, Map<String, Object> input) {
        return invoke(graph, GraphState.of(input));
    }

    @Override
    public RunResult invoke(CompiledGraph graph, GraphState input) {
        return bind(graph).invoke(input, defaultTimeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public DagRuntime runtime() {
        return runtime;
    }

    @Override
    public String toString() {
        return "ProcessingDag[default timeout="
                + (defaultTimeoutMs == 0 ? "none" : defaultTimeoutMs + "ms") + "]";
    }
}
