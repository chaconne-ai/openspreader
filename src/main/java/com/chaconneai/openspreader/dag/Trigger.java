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

import java.util.Objects;

/**
 * How many inbound edges must carry before a node runs.
 *
 * <h2>Two dimensions, kept apart</h2>
 * A node with several inbound edges raises two separate questions, and running them together
 * is what makes workflow engines grow a dozen near-identical trigger modes:
 *
 * <table border="1">
 *   <caption>What decides whether a node runs</caption>
 *   <tr><th></th><th>Question</th><th>Answered by</th></tr>
 *   <tr><td>Per edge</td><td>does <b>this</b> edge carry?</td>
 *       <td>{@link EdgeCondition}: on success, on failure, or either</td></tr>
 *   <tr><td>Per node</td><td><b>how many</b> carrying edges are enough?</td>
 *       <td>This class</td></tr>
 * </table>
 *
 * <p>So "run the cleanup step once everything upstream has finished, successfully or not" is
 * not a trigger mode of its own. It is {@link EdgeCondition#ON_COMPLETE} edges plus
 * {@link #all()}, and it falls out of the two dimensions rather than being bolted on.
 *
 * <h2>It belongs to the node, not the edge</h2>
 * Two edges into one node cannot disagree about it, and {@code compile()} refuses a graph
 * where they do: a contradiction settled by declaration order would be a graph whose
 * behaviour depended on the order its edges happened to be written in.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class Trigger {

    private static final Trigger ALL = new Trigger(0, "ALL");
    private static final Trigger ANY = new Trigger(1, "ANY");

    /** 0 means "every inbound edge"; otherwise the number of carrying edges required. */
    private final int required;
    private final String label;

    private Trigger(int required, String label) {
        this.required = required;
        this.label = label;
    }

    /**
     * Every inbound edge must have resolved, and at least one must have carried. The default,
     * and what a fan-in means.
     *
     * <p><b>Skipped predecessors count as resolved.</b> A conditional upstream leaves one
     * branch unchosen, and waiting for it would hang the graph for ever. What a skipped edge
     * does not do is carry, so a node whose every inbound edge was skipped is skipped in
     * turn. See {@link NodeStatus#SKIPPED}.
     */
    public static Trigger all() {
        return ALL;
    }

    /**
     * The first carrying edge is enough.
     *
     * <p><b>The node still runs exactly once.</b> Predecessors finishing later do not trigger
     * it again, though their updates are still merged into the final state, so nothing they
     * computed is thrown away. This is the one place where the engine's behaviour is easy to
     * guess wrongly, so it is stated here and again on {@code StateGraph.To#onAny()}.
     */
    public static Trigger any() {
        return ANY;
    }

    /**
     * At least {@code n} inbound edges must carry.
     *
     * <p>The general case that {@link #any()} and {@link #all()} are the two ends of. What it
     * is for is a quorum: three price feeds, and two agreeing is enough to proceed without
     * waiting for the third, which may be down.
     *
     * <p>Like {@link #any()}, the node runs <b>once</b>, as soon as the nth edge carries.
     * Edges carrying afterwards still contribute their updates.
     *
     * @param n how many. 1 is exactly {@link #any()}; a number above the inbound edge count
     *          is refused by {@code compile()}, because a quorum that can never be reached is
     *          a graph that can never finish, and saying so beats hanging
     */
    public static Trigger atLeast(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("atLeast(" + n + ") makes no sense; a node "
                    + "needs at least one carrying edge to run");
        }
        return n == 1 ? ANY : new Trigger(n, "AT_LEAST(" + n + ")");
    }

    /** Whether every inbound edge has to resolve before this node can be decided. */
    public boolean needsAll() {
        return required == 0;
    }

    /** The fixed requirement, or 0 for {@link #all()}. Used by validation and rendering. */
    public int required() {
        return required;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Trigger other && other.required == required;
    }

    @Override
    public int hashCode() {
        return Objects.hash(required);
    }

    @Override
    public String toString() {
        return label;
    }
}
