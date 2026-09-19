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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Drives one run of one graph. Created per run, used once, thrown away.
 *
 * <h2>It owns no threads</h2>
 * Dispatch returns a {@code CompletableFuture} from the pool, and completions are pushed onto
 * a queue that this one thread drains. So a run costs the caller's thread and nothing else:
 * no executor to size, no pool to shut down, no thread leak when a graph is abandoned. The
 * parallelism is the cluster's, which is the whole point of the exercise.
 *
 * <h2>The three rules that are easy to get wrong</h2>
 *
 * <h3>1. An unchosen branch is skipped, and skipping spreads</h3>
 * When a conditional picks one branch, the others did not fail and will not run. If they were
 * left pending, a fan-in downstream would wait for them for ever; if they were failed, every
 * conditional would fail its graph. So they become {@link NodeStatus#SKIPPED}, and a node all
 * of whose inbound edges are skipped is skipped in turn, all the way down.
 *
 * <p>The bookkeeping is per <b>edge</b>, not per node, and that matters: a node can be the
 * unchosen branch of one conditional and still be reached by an ordinary edge from somewhere
 * else. Marking the node would skip it wrongly; marking the edge does not.
 *
 * <h3>2. A quorum trigger fires once, and loses nothing</h3>
 * The edge that meets the requirement dispatches the node, whether that is the first of
 * several ({@code any()}) or the nth ({@code atLeast(n)}). Later ones do not dispatch it
 * again, but their updates are still merged, so a branch that lost the race has not had its
 * work thrown away.

 * <h3>4. A failure with somewhere to go does not stop the graph</h3>
 * A node that throws normally halts the run, which is the right default. But an
 * {@code onFailure()} or {@code onComplete()} edge out of it means the graph has a plan for
 * that case, so the run carries on down it and finishes. {@code RunResult.failed()} is then
 * false while the node's status is still FAILED and {@code failures()} still holds what it
 * threw: "refused and refunded" is a run that worked.
 *
 * <h3>3. Merging is in node-name order, not arrival order</h3>
 * Two branches finishing at once would otherwise merge in whatever order the network happened
 * to deliver them, and a graph would give different answers on different days. Completions
 * are therefore held in a sorted buffer, and one is folded into the state only once <b>no
 * node that could sort before it is still running</b>. Two parallel branches writing one
 * channel are folded in their names' order however the replies arrive.
 *
 * <p>There is one thing that outranks this, and it has to: a node is always shown what its
 * own ancestors wrote. A buffered update is therefore folded ahead of its turn when the node
 * that produced it comes before the node about to be dispatched, or before a router about to
 * read the state. Holding it back for the sake of ordering would mean showing a node a state
 * from before the step it waited for, which is not a subtler kind of correct.
 *
 * <p>So the guarantee is exactly this: <b>updates from nodes that race each other are folded
 * in name order</b>. Updates from nodes that do not race are folded in the order the graph
 * puts them in, which is the only order that means anything.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class GraphRunner {

    private static final Logger log = LoggerFactory.getLogger(GraphRunner.class);

    /** Where an edge stands. Resolved once and never revisited. */
    private enum EdgeState {
        /** The source has not finished, or a conditional has not chosen yet. */
        PENDING,
        /** The source succeeded and this edge carries. */
        TAKEN,
        /** The source failed or was skipped, or a conditional chose elsewhere. */
        NOT_TAKEN
    }

    private final CompiledGraph graph;
    private final DagRuntime runtime;

    private final Map<String, NodeStatus> status = new LinkedHashMap<>();
    private final Map<String, Map<String, EdgeState>> edgeStates = new LinkedHashMap<>();
    private final Map<String, NodeOutcome> outcomes = new LinkedHashMap<>();

    /** Completed but not yet folded into the state, kept in node-name order. See rule 3. */
    private final TreeMap<String, NodeOutcome> pendingMerge = new TreeMap<>();

    /** Which node last wrote each channel, so a lost write can be told from an overwrite. */
    private final Map<String, String> lastWriterOf = new LinkedHashMap<>();

    private final BlockingQueue<NodeOutcome> completions = new LinkedBlockingQueue<>();

    /** Nodes already sent out. Guards rule 2: ANY_SUCCESS must not dispatch twice. */
    private final Set<String> dispatched = new LinkedHashSet<>();

    private GraphState state = GraphState.empty();
    private String runId;
    private int inflight;

    /**
     * Nodes waiting out a backoff, by the moment each becomes due.
     *
     * <p>A sorted map, so the head is the next thing to do and the loop below knows exactly
     * how long it may sleep. This is what makes waiting free: no timer is started and no
     * thread sleeps, because the loop is already waiting for completions and simply wakes no
     * later than the head of this map.
     */
    private final TreeMap<Long, List<String>> dueRetries = new TreeMap<>();

    /** Nodes counted as in flight because they are waiting to be retried. */
    private int awaitingRetry;

    /** What is running right now, so that a cancellation can stop waiting for it. */
    private final Map<String, CompletableFuture<NodeOutcome>> running = new LinkedHashMap<>();

    /** Set from another thread; see {@link #cancel}. */
    private volatile boolean cancelled;
    private String failedNode;
    private Throwable failure;

    /**
     * Every node that threw, handled or not.
     *
     * <p>Kept apart from {@link #failure}, which is only the unhandled one that stopped the
     * run. A failure routed to a compensation path is still a failure worth reporting, and
     * losing it because the graph recovered would hide exactly the thing somebody is looking
     * for when they ask why the compensation ran.
     */
    private final Map<String, Throwable> failures = new LinkedHashMap<>();

    /** How many attempts each node has had. 1 after the first dispatch. */
    private final Map<String, Integer> attempts = new LinkedHashMap<>();

    private final DagStats stats;

    public GraphRunner(CompiledGraph graph, DagRuntime runtime) {
        this.graph = graph;
        this.runtime = runtime;
        this.stats = runtime.stats();
    }

    /**
     * Runs to completion.
     *
     * @param timeoutMs 0 or less waits indefinitely
     */
    public RunResult run(GraphState initial, long timeoutMs) {
        return run(initial, Map.of(), UUID.randomUUID().toString(), timeoutMs);
    }

    /**
     * Runs to completion, optionally carrying on from where an earlier run stopped.
     *
     * @param completed nodes that have already finished and must not run again, with what
     *                  became of each. Empty for a fresh run
     * @param runId     this run's identity, carried on every listener callback and on the
     *                  result
     * @param timeoutMs 0 or less waits indefinitely
     */
    public RunResult run(GraphState initial, Map<String, NodeStatus> completed, String runId,
                         long timeoutMs) {
        long startedAt = System.currentTimeMillis();
        this.state = initial == null ? GraphState.empty() : initial;
        this.runId = runId;
        stats.runStarted();

        for (String node : graph.nodeNames()) {
            status.put(node, NodeStatus.PENDING);
            Map<String, EdgeState> out = new LinkedHashMap<>();
            graph.successors(node).forEach(to -> out.put(to, EdgeState.PENDING));
            edgeStates.put(node, out);
        }

        restore(completed);
        notify(l -> l.onStart(runId, graph, state));

        // Every root at once. Two independent roots run in parallel with no edge between
        // them, which is the reason multiple entries exist at all. A root that is already
        // done is skipped: dispatch() refuses a node that has been dispatched before, and
        // restore() put every finished node into that set
        graph.entries().forEach(this::dispatch);

        if (!completed.isEmpty()) {
            // Whatever the restored statuses have made ready. A fresh run reaches this
            // through the first completions instead
            cascade();
        }

        long deadline = timeoutMs > 0 ? startedAt + timeoutMs : Long.MAX_VALUE;
        while (inflight > 0 || awaitingRetry > 0) {
            if (cancelled) {
                failure = new DagException("graph " + graph.name() + " run " + runId
                        + " was cancelled with " + inflight + " node(s) still running");
                stopEverythingInFlight();
                break;
            }
            // Anything whose backoff has run out goes now, before the loop waits again
            dispatchDueRetries();

            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                // A partial result beats none: whatever finished is reported, and the
                // failure says what was still running when time ran out
                failure = new DagException("graph " + graph.name() + " ran out of time after "
                        + timeoutMs + "ms with " + inflight + " node(s) still running");
                stopEverythingInFlight();
                break;
            }
            // Wake for whichever comes first: a completion, the deadline, or the next retry
            // falling due. Without the last one a graph whose only pending work is a backoff
            // would sit here until the deadline
            long wait = Math.min(remaining, untilNextRetry());

            NodeOutcome outcome;
            try {
                outcome = completions.poll(wait, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failure = new DagException("graph " + graph.name()
                        + " was interrupted while running", e);
                stopEverythingInFlight();
                break;
            }
            if (outcome == null) {
                continue;
            }
            if (isWakeUp(outcome)) {
                // cancel() put this on the queue purely to wake this thread. Counting it as
                // a completion would take inflight to zero and end the loop through the
                // ordinary door, leaving the run looking successful rather than cancelled
                continue;
            }
            inflight--;
            running.remove(outcome.node());
            settle(outcome);
        }

        // Anything still buffered belongs in the final state, including the updates of
        // branches that lost an ANY_SUCCESS race. Nothing more can arrive, so nothing is
        // held back for ordering any longer
        drainRemaining();

        RunResult result = RunResult.of(runId, graph, state, status, outcomes, failedNode,
                failure, failures, System.currentTimeMillis() - startedAt, cancelled);
        stats.runFinished(result, cancelled);
        // A run whose failure was routed to a compensation path arrives at onSuccess: it did
        // what the graph said to do. result.failures() is how a listener tells the two apart
        notify(l -> {
            if (result.failed()) {
                l.onFailure(result, result.failure());
            } else {
                l.onSuccess(result);
            }
        });
        return result;
    }

    /**
     * Takes back what an earlier run finished, so that this one carries on rather than
     * starting over.
     *
     * <p>Three things happen for each restored node, and all three are needed:
     * <ol>
     *   <li>its status is set, so the readiness of everything downstream is judged against
     *       it;</li>
     *   <li>it joins the dispatched set, which is what stops it running a second time. This
     *       is the whole point: re-running a step that has already moved money is worse than
     *       not resuming at all;</li>
     *   <li>its outbound edges are resolved, <b>without announcing them</b>. Those
     *       transitions were reported when they first happened, and reporting them again
     *       would have an application's record show the same move twice.</li>
     * </ol>
     *
     * <p>A conditional on a restored node is <b>evaluated again</b> against the restored
     * state rather than being stored. For an expression that is a pure function of the
     * channels, which is what SpEL conditions are, that gives the same branch. A router
     * written as a lambda reading something outside the state could differ, and then so
     * would the run, which is one more reason to write conditions as expressions.
     */
    private void restore(Map<String, NodeStatus> completed) {
        if (completed.isEmpty()) {
            return;
        }
        completed.forEach((node, was) -> {
            status.put(node, was);
            dispatched.add(node);
            if (was == NodeStatus.FAILED) {
                failures.put(node, new DagException("node " + node + " had already failed "
                        + "when this run was resumed"));
            }
        });

        // Every status is in place before any edge is resolved, so a router reads the whole
        // restored state rather than half of it
        for (String node : completed.keySet()) {
            resolveOutgoing(node, false);
        }
        for (Map.Entry<String, NodeStatus> entry : completed.entrySet()) {
            if (entry.getValue() == NodeStatus.FAILED && !tolerated(entry.getKey())
                    && failedNode == null) {
                // The same judgement a fresh run would have made: a failure the graph has no
                // plan for stops the run, resumed or not
                failedNode = entry.getKey();
                failure = failures.get(entry.getKey());
            }
        }
    }

    /**
     * Asks this run to stop.
     *
     * <p>Called from another thread, which is the only way it could be: the thread that
     * started the run is inside the loop. So the flag is volatile and nothing else is shared.
     *
     * <p>What stopping means is deliberately modest. <b>No more nodes are dispatched</b>, and
     * the run stops waiting for the ones already out. It does not reach into another replica
     * and interrupt work that is already running there: the node may be halfway through a
     * payment, and pretending otherwise would be worse than waiting. Whatever did finish is
     * still reported, and {@link RunResult#completed()} still says what to skip on a resume.
     */
    public void cancel() {
        cancelled = true;
        // The loop may be waiting on the queue with a long timeout; this wakes it at once
        completions.offer(NodeOutcome.failure("", new DagException("cancelled"), "", 0L));
    }

    /** Whether this run was stopped by {@link #cancel()}. */
    public boolean isCancelled() {
        return cancelled;
    }

    /** The placeholder {@link #cancel()} uses to wake the loop. It is not a node. */
    private static boolean isWakeUp(NodeOutcome outcome) {
        return outcome.node() == null || outcome.node().isEmpty();
    }

    /** Sends out every node whose backoff has run out. */
    private void dispatchDueRetries() {
        long now = System.currentTimeMillis();
        while (!dueRetries.isEmpty() && dueRetries.firstKey() <= now) {
            List<String> due = dueRetries.pollFirstEntry().getValue();
            for (String node : due) {
                awaitingRetry--;
                dispatch(node);
            }
        }
    }

    /** How long the loop may wait before the next backoff falls due. */
    private long untilNextRetry() {
        if (dueRetries.isEmpty()) {
            return Long.MAX_VALUE;
        }
        return Math.max(1L, dueRetries.firstKey() - System.currentTimeMillis());
    }

    /**
     * Stops waiting for everything still out.
     *
     * <p>For the local ones this interrupts the work, since the thread is ours. For a node
     * running on another replica it cancels only <b>this side's</b> waiting: the far end has
     * no idea a cancellation happened, which is the honest thing to say about it.
     */
    private void stopEverythingInFlight() {
        running.forEach((node, future) -> future.cancel(true));
        running.clear();
        dueRetries.clear();
        awaitingRetry = 0;
    }

    // ------------------------------------------------------------------

    /** One node finished: record it, resolve its outbound edges, then look for new work. */
    private void settle(NodeOutcome outcome) {
        String node = outcome.node();
        if (isWakeUp(outcome)) {
            return;
        }
        outcomes.put(node, outcome);
        stats.nodeFinished(outcome.ok());

        if (outcome.ok()) {
            status.put(node, NodeStatus.SUCCESS);
            // Held rather than merged now, so that concurrent completions fold in name order
            pendingMerge.put(node, outcome);
        } else {
            Throwable thrown = outcome.failure() != null ? outcome.failure()
                    : new DagException("node " + node + " failed: " + outcome.failureText());

            // Retries come first, and before any failure edge is even considered. Inverting
            // the two turns one network wobble into a compensation, which is why the order
            // lives here rather than in everybody's node code
            int used = attempts.getOrDefault(node, 1);
            // Three questions, in this order: is there an attempt left, is this failure the
            // kind worth repeating, and is the run still wanted. "Insufficient funds" is a
            // complete answer, and asking it twice more only costs two backoffs
            if (used <= graph.retriesOf(node) && graph.worthRetrying(node, thrown) && !cancelled) {
                int next = used + 1;
                attempts.put(node, next);
                long delay = graph.backoffOf(node).delayMsBefore(next);
                log.debug("Graph {} node {} failed, retrying (attempt {} of {}) in {}ms: {}",
                        graph.name(), node, next, graph.retriesOf(node) + 1, delay,
                        thrown.toString());
                notify(l -> l.onRetry(runId, node, next, thrown));
                stats.retried();
                // The pool picks a replica afresh on every call, so the next attempt may well
                // land somewhere else. That is what makes retrying worth anything against
                // "one instance is unwell"
                dispatched.remove(node);
                status.put(node, NodeStatus.PENDING);
                if (delay <= 0L) {
                    dispatch(node);
                } else {
                    // Put aside until it is due. It still counts as in flight, or the loop
                    // would decide the run had finished and leave a node half-retried
                    awaitingRetry++;
                    dueRetries.computeIfAbsent(System.currentTimeMillis() + delay,
                            at -> new ArrayList<>(1)).add(node);
                }
                return;
            }

            status.put(node, NodeStatus.FAILED);
            failures.put(node, thrown);
            notify(l -> l.onNodeFinished(runId, outcome));

            // The edges have to be resolved before it can be said whether this failure
            // actually cost anything; see tolerated()
            resolveOutgoing(node);
            if (!tolerated(node) && failedNode == null) {
                failedNode = node;
                failure = thrown;
            }
            cascade();
            return;
        }

        // Which edges carry is decided by each edge's condition against what became of the
        // node, rather than by success alone
        notify(l -> l.onNodeFinished(runId, outcome));
        resolveOutgoing(node);
        cascade();
    }

    /**
     * Decides which of a successful node's outbound edges carry.
     *
     * <p>A conditional's router runs <b>here, exactly once</b>, against the state as it
     * stands the moment the node finished. Running it later, or once per target, would let
     * two branches of one switch see different states and both be chosen.
     */
    private void resolveOutgoing(String node) {
        resolveOutgoing(node, true);
    }

    private void resolveOutgoing(String node, boolean announce) {
        NodeStatus ended = status.get(node);
        List<StateGraph.ConditionalSpec> conditionals = graph.conditionalsFrom(node);

        Set<String> chosen = new LinkedHashSet<>();
        Set<String> offered = new LinkedHashSet<>();

        if (!conditionals.isEmpty()) {
            conditionals.forEach(c -> offered.addAll(c.allTargets()));
        }

        // A router only runs when the node succeeded. Asking "which branch" of a node that
        // threw is meaningless, and the state it would read is the state from before the node
        // ran, so every branch is left untaken and the failure routes by edge condition
        // instead
        if (!conditionals.isEmpty() && ended == NodeStatus.SUCCESS) {
            // A switch on the value this node just wrote has to see that value, so the
            // router reads a view that includes it. Folding it in instead would put this
            // node's update into the state ahead of a parallel branch that sorts before it
            drainPendingMerge();
            GraphState seen = visibleTo(node);

            for (StateGraph.ConditionalSpec c : conditionals) {
                String key;
                try {
                    key = c.router().apply(seen);
                } catch (RuntimeException e) {
                    throw new DagException("the conditional router on node " + node + " threw", e);
                }
                List<String> targets = c.branches().get(key);
                if (targets == null) {
                    // An unknown key takes the default, and without one takes nothing. "None
                    // of the above, carry on" is a legitimate workflow; see Switch#orElse
                    targets = c.defaultTargets();
                }
                chosen.addAll(targets);
                if (log.isDebugEnabled()) {
                    log.debug("Graph {} conditional at {} chose \"{}\" -> {}",
                            graph.name(), node, key, targets);
                }
            }
        }

        Map<String, EdgeState> out = edgeStates.get(node);
        out.replaceAll((target, was) -> {
            if (offered.contains(target)) {
                // A switch's own target: only the chosen branch carries
                return chosen.contains(target) ? EdgeState.TAKEN : EdgeState.NOT_TAKEN;
            }
            // An ordinary edge carries when what became of the source is what the edge asked
            // for. That one line is the whole of failure routing
            EdgeCondition condition = graph.conditionOf(node, target);
            return condition.carriesOn(ended) ? EdgeState.TAKEN : EdgeState.NOT_TAKEN;
        });

        // Only the edges that carried are a transition; an unchosen branch is not a move from
        // anywhere to anywhere. The listener is shown the same view the router saw, so that
        // what the node produced is in it
        if (!announce) {
            return;
        }
        drainPendingMerge();
        GraphState seen = visibleTo(node);
        out.forEach((target, edge) -> {
            if (edge == EdgeState.TAKEN) {
                notify(l -> l.onTransition(runId, node, target, seen));
            }
        });
    }

    /**
     * Whether a failure cost the graph anything.
     *
     * <p>The first version of this asked a different question: <b>did the author declare a
     * failure edge</b>. That reads well and is wrong, and a quorum is what shows it. Three
     * price feeds with {@code atLeast(2)} are a graph that already tolerates one being down;
     * the third failing declares nothing and costs nothing, yet the declaration test would
     * stop the whole run.
     *
     * <p>So the question is about <b>consequences</b>, not declarations:
     *
     * <ul>
     *   <li>An outbound edge <b>carried</b>: the failure was routed somewhere, which is what
     *       an {@code onFailure()} or {@code onComplete()} edge is for</li>
     *   <li>Nothing carried, but <b>no successor was blocked</b> by it: somebody else can
     *       still satisfy them, as the two surviving feeds do. Nothing was lost</li>
     *   <li>Nothing carried and <b>a successor can now never run</b>: work the graph was
     *       asked to do will not happen, so the run failed</li>
     * </ul>
     *
     * <p>A failing leaf falls into the third case, correctly: it has no successors, so
     * nothing carried and nothing is left to make up for it.
     */
    private boolean tolerated(String node) {
        boolean carried = edgeStates.get(node).containsValue(EdgeState.TAKEN);
        if (carried) {
            return true;
        }
        Set<String> successors = graph.successors(node);
        if (successors.isEmpty()) {
            // A leaf that threw did the graph no good at all, and there is nothing downstream
            // that could make up for it
            return false;
        }
        for (String successor : successors) {
            if (status.get(successor) == NodeStatus.PENDING
                    && !dispatched.contains(successor)
                    && readinessOf(successor) == Readiness.SKIP) {
                return false;
            }
        }
        return true;
    }

    private void markAllOutgoing(String node, EdgeState value) {
        edgeStates.get(node).replaceAll((target, was) -> value);
    }

    /**
     * Works out what became runnable, or unrunnable, and keeps going until nothing changes.
     *
     * <p>A loop rather than recursion because skipping spreads: skipping one node resolves
     * its outbound edges, which can skip the next, and a deep chain of unchosen branches
     * would otherwise be a deep stack.
     */
    private void cascade() {
        Deque<String> worklist = new ArrayDeque<>(graph.nodeNames());
        while (!worklist.isEmpty()) {
            String node = worklist.poll();
            if (status.get(node) != NodeStatus.PENDING || dispatched.contains(node)) {
                continue;
            }
            Readiness readiness = readinessOf(node);
            if (readiness == Readiness.WAIT) {
                continue;
            }
            if (readiness == Readiness.RUN) {
                if (failedNode == null) {
                    dispatch(node);
                }
                continue;
            }
            // SKIP: it will never run, so its own outbound edges can be resolved now, which
            // may make the next node skippable. Everything is queued again because a skip
            // anywhere can unblock a decision anywhere downstream
            status.put(node, NodeStatus.SKIPPED);
            markAllOutgoing(node, EdgeState.NOT_TAKEN);
            worklist.addAll(graph.successors(node));
        }
    }

    private enum Readiness { RUN, WAIT, SKIP }

    /**
     * Whether a node can run, must wait, or will never run.
     *
     * <p>The decision is made on <b>inbound edges</b> rather than on predecessor nodes, for
     * the reason given in the class comment: one node can be an unchosen conditional branch
     * and still be reached by an ordinary edge.
     */
    private Readiness readinessOf(String node) {
        Set<String> predecessors = graph.predecessors(node);
        if (predecessors.isEmpty()) {
            // Only the entry node, and it was dispatched before the loop started
            return Readiness.WAIT;
        }
        int taken = 0;
        int pending = 0;
        for (String from : predecessors) {
            EdgeState edge = edgeStates.get(from).get(node);
            if (edge == EdgeState.TAKEN) {
                taken++;
            } else if (edge == EdgeState.PENDING) {
                pending++;
            }
        }

        Trigger trigger = graph.triggerOf(node);

        if (trigger.needsAll()) {
            if (pending > 0) {
                return Readiness.WAIT;
            }
            // Every inbound edge is resolved. At least one has to have carried, or there is
            // nothing upstream of this node that ran at all.
            //
            // Note this is not "every edge carried": a skipped branch resolves without
            // carrying, and demanding that it carry would hang every graph that has both a
            // conditional and a fan-in
            return taken > 0 ? Readiness.RUN : Readiness.SKIP;
        }

        // A quorum, of which any() is the case n == 1. Enough carrying edges and it goes,
        // whatever the rest are still doing
        if (taken >= trigger.required()) {
            return Readiness.RUN;
        }
        return pending > 0 ? Readiness.WAIT : Readiness.SKIP;
    }

    /** Sends a node out to the cluster. */
    private void dispatch(String node) {
        if (cancelled || !dispatched.add(node)) {
            return;
        }
        // Whatever can be committed is, and the slice then adds anything still buffered from
        // this node's own ancestors: it must see what the steps it waited for produced
        drainPendingMerge();
        GraphState slice = visibleTo(node);

        // The bean name when the node has one, else the class name. Whichever it is, the far
        // side looks it up in its registry; nothing is ever resolved into a class from a name
        // off the wire
        String target = graph.targetOf(node);
        // Captured per dispatch rather than once per run: a tracer's current context is a
        // property of this moment on this thread, and a run may take minutes
        NodeContext context = new NodeContext(graph.name(), runId, node, graph.configOf(node),
                runtime.tracing().capture());

        attempts.putIfAbsent(node, 1);
        status.put(node, NodeStatus.RUNNING);
        inflight++;

        boolean local = graph.isLocal(node);
        stats.nodeDispatched(local);
        CompletableFuture<NodeOutcome> future = local
                ? runLocally(node, target, context, slice)
                : runtime.pool().<NodeOutcome>submit(
                        NodeDispatcher.BEAN_NAME, NodeDispatcher.METHOD_NAME,
                        graph.name(), node, target, slice, context);
        running.put(node, future);

        future.whenComplete((outcome, error) -> {
            if (outcome != null) {
                completions.add(outcome);
                return;
            }
            // The dispatch itself failed: no peer, a timeout, serialisation. The node never
            // ran, so there is nothing to report but the dispatch error, and it is turned
            // into an outcome so the main loop has one shape of thing to handle
            Throwable cause = error instanceof CompletionException
                    ? error.getCause() : error;
            completions.add(NodeOutcome.failure(node,
                    new DagException("node " + node + " could not be dispatched", cause),
                    "unknown", 0L));
        });
    }

    /**
     * Runs a node here rather than sending it anywhere.
     *
     * <p>For work that is <b>already outside</b> this application: an HTTP API, a queue,
     * another company's service. Shipping such a call to a peer adds a hop, a serialisation
     * and a second thing that can fail, and the work was never going to happen locally
     * anyway.
     *
     * <p>It goes onto a pool of its own rather than running on this loop's thread, because
     * two external calls in a fan-out have to overlap. Running them here in turn would make
     * the shape of the graph a lie.
     *
     * <p>That pool <b>rejects</b> when full rather than discarding; a discarded node would be
     * one the run waits for for ever. A rejection arrives as a dispatch failure and the node
     * fails visibly.
     */
    private CompletableFuture<NodeOutcome> runLocally(String node, String target,
                                                      NodeContext context,
                                                      GraphState slice) {
        if (!runtime.canRunLocally()) {
            return CompletableFuture.completedFuture(NodeOutcome.failure(node,
                    new DagException("node " + node + " is declared local(), but this runtime "
                            + "cannot run nodes in place. It needs the node dispatcher and an "
                            + "executor, which the auto-configuration supplies; a hand-built "
                            + "DagRuntime(pool) does not"), "local", 0L));
        }
        return CompletableFuture.supplyAsync(
                () -> runtime.local().runNode(graph.name(), node, target, slice, context),
                runtime.localExecutor());
    }

    /**
     * Tells every listener, and lets none of them break the run.
     *
     * <p>A graph's correctness must not depend on whether somebody's metrics client is
     * reachable, so anything thrown here is logged and swallowed. The one thing a listener
     * can still do is be slow: it runs on this thread, and every node waiting to be
     * dispatched waits with it.
     */
    private void notify(Consumer<GraphListener> call) {
        for (GraphListener listener : graph.listeners()) {
            try {
                call.accept(listener);
            } catch (Throwable t) {
                log.warn("A listener on graph {} threw, and was ignored: {}",
                        graph.name(), t.toString());
            }
        }
    }

    // ------------------------------------------------------------------

    /**
     * Folds in every buffered completion that it is already safe to fold, in node-name order.
     *
     * <p>Safe means no node that would sort before it is still in flight. The head of the
     * buffer blocks the rest: folding a later one past a blocked one is the very reordering
     * this is here to prevent.
     */
    private void drainPendingMerge() {
        while (!pendingMerge.isEmpty() && settledAheadOf(pendingMerge.firstKey())) {
            Map.Entry<String, NodeOutcome> first = pendingMerge.pollFirstEntry();
            merge(first.getKey(), first.getValue().updates());
        }
    }

    /**
     * Whether every node that races this one and sorts before it has finished.
     *
     * <p>A node that never runs at all counts as finished at the end of the run, which is
     * what {@link #drainRemaining()} is for: nothing is held back for ever waiting on a
     * branch that was never taken.
     */
    private boolean settledAheadOf(String node) {
        for (String other : graph.nodeNames()) {
            if (other.compareTo(node) >= 0 || !graph.concurrent(other, node)) {
                continue;
            }
            NodeStatus otherStatus = status.get(other);
            if (otherStatus == NodeStatus.PENDING || otherStatus == NodeStatus.RUNNING) {
                return false;
            }
        }
        return true;
    }

    /**
     * The state as this node is entitled to see it: what is committed, plus whatever is still
     * buffered from the node itself and from its ancestors.
     *
     * <p>A <b>view</b>, not a fold. The two jobs pull opposite ways: a node must be shown what
     * the steps it waited for produced, and the committed order must stay the nodes' names.
     * Folding early would satisfy the first at the cost of the second, so the reader gets a
     * copy and the buffer keeps its order.
     *
     * <p>What is deliberately <b>not</b> in here is a buffered update from a node that races
     * this one. It may or may not have arrived yet, so including it would make what a node
     * reads depend on the network, and the graph never said this node comes after that one.
     */
    private GraphState visibleTo(String node) {
        Map<String, Object> preview = null;
        // The buffer is sorted, so this walks in node-name order too
        for (Map.Entry<String, NodeOutcome> buffered : pendingMerge.entrySet()) {
            String from = buffered.getKey();
            Map<String, Object> updates = buffered.getValue().updates();
            if (updates == null || updates.isEmpty()) {
                continue;
            }
            if (!from.equals(node) && !graph.precedes(from, node)) {
                continue;
            }
            if (preview == null) {
                preview = new LinkedHashMap<>(state.asMap());
            }
            for (Map.Entry<String, Object> update : updates.entrySet()) {
                Reducer<Object> reducer = graph.reducerOf(update.getKey());
                preview.put(update.getKey(), reducer == null ? update.getValue()
                        : reducer.reduce(preview.get(update.getKey()), update.getValue()));
            }
        }
        return preview == null ? state : GraphState.of(preview);
    }

    /** At the end of the run, when nothing more can arrive, everything left is folded in. */
    private void drainRemaining() {
        while (!pendingMerge.isEmpty()) {
            Map.Entry<String, NodeOutcome> first = pendingMerge.pollFirstEntry();
            merge(first.getKey(), first.getValue().updates());
        }
    }

    /**
     * Applies one node's update.
     *
     * <p>The check below is the whole reason state is channelled. Two nodes that can run at
     * the same time writing one channel with no reducer means one of the two writes is about
     * to vanish, and vanishing quietly is the failure mode this design exists to prevent. A
     * node that writes a channel an <b>ancestor</b> wrote is a different thing entirely: that
     * is an ordinary overwrite, in a defined order, and perfectly legitimate.
     */
    private void merge(String node, Map<String, Object> updates) {
        if (updates == null || updates.isEmpty()) {
            return;
        }
        Map<String, Object> merged = new LinkedHashMap<>(state.asMap());
        updates.forEach((channel, incoming) -> {
            String previous = lastWriterOf.get(channel);
            Reducer<Object> reducer = graph.reducerOf(channel);

            if (reducer == null && previous != null && graph.concurrent(previous, node)) {
                throw new DagException("nodes " + previous + " and " + node + " run at the "
                        + "same time and both write channel \"" + channel + "\", but no "
                        + "reducer is declared for it, so one of the two writes would be "
                        + "lost. Add channel(\"" + channel + "\", Reducers.…) to the graph");
            }

            merged.put(channel, reducer == null ? incoming
                    : reducer.reduce(merged.get(channel), incoming));
            lastWriterOf.put(channel, node);
        });
        state = GraphState.of(merged);
    }
}
