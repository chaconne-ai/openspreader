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
 * The extension point: told what a run is doing, without being part of it.
 *
 * <p>For tracing, metrics, audit trails, a progress bar. Every method has a default, so an
 * implementation overrides only what it cares about:
 *
 * <pre>{@code
 * flow = dag.bind(StateGraph.create("order-flow")
 *         .listener(new GraphListener() {
 *             @Override
 *             public void onTransition(String runId, String from, String to, GraphState s) {
 *                 tracer.currentSpan().event(from + " -> " + to);
 *             }
 *
 *             @Override
 *             public void onFailure(RunResult result, Throwable cause) {
 *                 alerts.send(result.describe(), cause);
 *             }
 *         })
 *         ...
 *         .compile());
 * }</pre>
 *
 * <h2>Every callback carries the run's identity</h2>
 * A listener is attached to the <b>graph</b>, so one instance sees every run of it, and two
 * runs can be in flight at once. Without the {@code runId} on each call, a listener building
 * a record would interleave them and there would be no way to tell afterwards. It is the same
 * identity {@link RunResult#runId()} reports, so what a listener wrote and what the caller
 * got back line up.
 *
 * <h2>Not to be confused with the failure edge</h2>
 * {@link #onFailure} here is an <b>observer</b>: it is told, and changes nothing.
 * {@code StateGraph.From#onFailure()} is a <b>route</b>: it decides where the run goes next.
 * Compensation belongs to the second; watching belongs to this one. A listener that tried to
 * compensate would be doing it outside the graph, where nothing records that it happened.
 *
 * <h2>A listener cannot break a run</h2>
 * Anything thrown here is caught and logged, and the run carries on. A graph's correctness
 * must not depend on whether somebody's metrics client is reachable.
 *
 * <h2>It runs on the coordinating thread</h2>
 * So it must be quick. A listener that blocks on a network call holds up the whole run, and
 * every node waiting to be dispatched waits with it. Hand slow work to an executor of your
 * own.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 13/09/2026
 */
public interface GraphListener {

    /**
     * An edge carried: the run moved from one node to the next.
     *
     * <p>Called once per <b>carrying</b> edge, so a fan-out of three is three calls and a
     * fan-in of three is three calls. Edges that did not carry, a conditional's unchosen
     * branches among them, are not reported here; {@code RunResult.statuses()} has them.
     *
     * @param runId this run's identity, the same one {@link RunResult#runId()} carries
     * @param from  the node that finished
     * @param to    the node this edge leads to. It may not have started yet, and with
     *              {@code Trigger.all()} it may still be waiting on other edges
     * @param state the channels as they stood when the edge resolved
     */
    default void onTransition(String runId, String from, String to, GraphState state) {
    }

    /**
     * The run is about to start.
     *
     * <p>Where an application that keeps its own record writes the opening entry: the run's
     * identity, the definition it is running, and what it was given.
     *
     * @param graph   the graph, whose {@code render()} is the definition as text
     * @param initial the channels the caller supplied, after the declared inputs have been
     *                checked and the optional ones filled in
     */
    default void onStart(String runId, CompiledGraph graph, GraphState initial) {
    }

    /**
     * One node finished, for good: it succeeded, or it failed with its retries used up.
     *
     * <p><b>This is the hook to persist against.</b> {@link #onTransition} reports edges, and
     * a node with no outgoing edge produces none, so a record built from transitions alone
     * would be missing every terminal node. This one reports the node itself, with what it
     * wrote and where it ran.
     *
     * <p>Writing {@code (runId, outcome.node(), status, outcome.updates())} away as it
     * arrives is what makes {@code CompiledGraph.resume(...)} usable later: those rows are
     * exactly what it takes back.
     *
     * <p>A node being retried is <b>not</b> reported here; {@link #onRetry} is. So one node
     * produces exactly one of these per run.
     */
    default void onNodeFinished(String runId, NodeOutcome outcome) {
    }

    /**
     * The run finished without an unhandled failure.
     *
     * <p>Note that a run whose failure was routed to a compensation path arrives <b>here</b>,
     * not at {@link #onFailure}: it did what the graph said to do. {@code result.failures()}
     * is not empty in that case.
     */
    default void onSuccess(RunResult result) {
    }

    /**
     * The run was stopped by something it could not handle.
     *
     * @param result what had happened up to that point: the state as it stood, and the status
     *               of every node
     * @param cause  what actually went wrong, the same throwable as {@link RunResult#failure()}.
     *               It is a parameter of its own because this is nearly always what a listener
     *               wants: an alert, a log line, a span to mark as errored. Handing over only
     *               the result would have every implementation begin by digging it out again
     */
    default void onFailure(RunResult result, Throwable cause) {
    }

    /**
     * A node is about to be tried again after failing.
     *
     * @param node    which node
     * @param attempt the attempt about to start, counting the first as 1
     * @param cause   what the previous attempt threw
     */
    default void onRetry(String runId, String node, int attempt, Throwable cause) {
    }
}
