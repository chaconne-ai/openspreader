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
 * What has to become of a node before an edge out of it carries.
 *
 * <p>The other half of {@link Trigger}: this decides whether one edge carries, the trigger
 * decides how many carrying edges are enough. Keeping them apart is what lets three edge
 * conditions and three triggers cover the shapes that would otherwise need a trigger mode
 * each.
 *
 * <h2>A skipped source never carries, whatever the condition</h2>
 * All three conditions below are about a node that <b>ran</b>. A node passed over by a
 * conditional did not run, did not succeed and did not fail, so none of its outbound edges
 * carry, {@link #ON_COMPLETE} included.
 *
 * <p>That is deliberate and worth stating, because "on complete" reads as "always". If a
 * skipped source made its ON_COMPLETE edges carry, a cleanup node behind an untaken branch
 * would run for a branch that never happened.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public enum EdgeCondition {

    /** The source succeeded. The default, and what an ordinary edge means. */
    ON_SUCCESS,

    /**
     * The source failed.
     *
     * <p>A compensation or fallback path. Declaring one changes something else too: a failure
     * with somewhere to go <b>no longer halts the run</b>. See {@code RunResult#failed()}.
     */
    ON_FAILURE,

    /**
     * The source finished, either way.
     *
     * <p>For a cleanup or notification step that has to happen whatever the outcome. Combined
     * with {@link Trigger#all()} this is the "wait for everything upstream to be over" that
     * other engines spell as a trigger mode.
     */
    ON_COMPLETE;

    /** Whether this edge carries when its source ended in the given state. */
    public boolean carriesOn(NodeStatus status) {
        return switch (status) {
            case SUCCESS -> this == ON_SUCCESS || this == ON_COMPLETE;
            case FAILED -> this == ON_FAILURE || this == ON_COMPLETE;
            // Skipped, and anything not yet terminal, carries nothing. See the class comment
            default -> false;
        };
    }

    /** Whether this condition gives a failure somewhere to go, which is what stops a failure
     *  from halting the whole run. */
    public boolean handlesFailure() {
        return this == ON_FAILURE || this == ON_COMPLETE;
    }
}
