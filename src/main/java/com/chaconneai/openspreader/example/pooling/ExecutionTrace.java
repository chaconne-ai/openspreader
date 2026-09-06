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
package com.chaconneai.openspreader.example.pooling;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records which ranges this process actually computed, so that the spread of work across
 * processes can be observed.
 *
 * <p>Why it is static: task objects arrive serialised and never pass through the Spring
 * container, so nothing can be injected into them. That is fine for an example; real
 * applications collecting metrics should reach their own collector through a static entry
 * point inside {@code compute()}.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class ExecutionTrace {

    private static final List<String> SEGMENTS = new CopyOnWriteArrayList<>();

    private ExecutionTrace() {
    }

    static void record(int from, int to, int depth) {
        SEGMENTS.add(from + "~" + to + "(d" + depth + ")");
    }

    public static List<String> segments() {
        return List.copyOf(SEGMENTS);
    }

    public static void reset() {
        SEGMENTS.clear();
    }
}
