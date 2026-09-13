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
 * Where one node of one run ended up.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public enum NodeStatus {

    /** Declared, not yet eligible to run. */
    PENDING,

    /** Dispatched, and running somewhere in the cluster. */
    RUNNING,

    /** Finished, and its update merged. */
    SUCCESS,

    /** Threw. By default the run stops; see {@code RunResult#failure()}. */
    FAILED,

    /**
     * Never ran, and never will in this run.
     *
     * <p>Two ways in: a conditional upstream chose another branch, or every inbound edge
     * was itself skipped. <b>Skipped is not a failure</b>: downstream nodes on
     * {@link Trigger#ALL_SUCCESS} treat a skipped predecessor as satisfied and carry on.
     * Treating it as a failure would make every conditional fail the whole graph; treating
     * it as "keep waiting" would hang it. This third state is what keeps conditionals and
     * fan-ins able to coexist.
     */
    SKIPPED;

    /** Whether this node will never produce anything more, one way or the other. */
    public boolean isTerminal() {
        return this == SUCCESS || this == FAILED || this == SKIPPED;
    }
}
