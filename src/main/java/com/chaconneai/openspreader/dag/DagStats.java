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

import java.util.concurrent.atomic.AtomicLong;

/**
 * What the engine has been doing, counted.
 *
 * <p>Every other component in this library reports to Micrometer, and until now the DAG
 * engine was the one that did not: a run's numbers could only be had from its own
 * {@link RunResult}, which is no use to anybody watching a dashboard at three in the morning.
 *
 * <h2>Why counters rather than a registry</h2>
 * The same shape the other components use: plain counters here, and one binder in the metrics
 * package that reads them. That keeps Micrometer an <b>optional</b> dependency and keeps the
 * engine from taking a hard line on how an application measures things.
 *
 * <h2>What is counted, and what deliberately is not</h2>
 * Runs, nodes, retries, failures, and time. Not <b>per-node</b> timings: a graph may have
 * hundreds of nodes and a metric per node name would multiply every series by that, which is
 * how a cardinality problem starts. A run's own node timings are on
 * {@link RunResult#millisOf}, where they cost nothing.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/09/2026
 */
public class DagStats {

    private final AtomicLong runsStarted = new AtomicLong();
    private final AtomicLong runsSucceeded = new AtomicLong();
    private final AtomicLong runsFailed = new AtomicLong();
    private final AtomicLong runsCancelled = new AtomicLong();
    private final AtomicLong runMillis = new AtomicLong();

    private final AtomicLong nodesRun = new AtomicLong();
    private final AtomicLong nodesFailed = new AtomicLong();
    private final AtomicLong nodesSkipped = new AtomicLong();
    private final AtomicLong nodesDispatched = new AtomicLong();
    private final AtomicLong nodesLocal = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();

    private final AtomicLong runsInFlight = new AtomicLong();

    void runStarted() {
        runsStarted.incrementAndGet();
        runsInFlight.incrementAndGet();
    }

    void runFinished(RunResult result, boolean cancelled) {
        runsInFlight.decrementAndGet();
        runMillis.addAndGet(result.millis());
        if (cancelled) {
            runsCancelled.incrementAndGet();
        } else if (result.failed()) {
            runsFailed.incrementAndGet();
        } else {
            runsSucceeded.incrementAndGet();
        }
        result.statuses().forEach((node, status) -> {
            if (status == NodeStatus.SKIPPED) {
                nodesSkipped.incrementAndGet();
            }
        });
    }

    void nodeDispatched(boolean local) {
        nodesDispatched.incrementAndGet();
        if (local) {
            nodesLocal.incrementAndGet();
        }
    }

    void nodeFinished(boolean ok) {
        nodesRun.incrementAndGet();
        if (!ok) {
            nodesFailed.incrementAndGet();
        }
    }

    void retried() {
        retries.incrementAndGet();
    }

    /** Runs begun on this instance, this one included while it is running. */
    public long runsStarted() {
        return runsStarted.get();
    }

    /** Runs that finished with no unhandled failure. A compensated run counts here. */
    public long runsSucceeded() {
        return runsSucceeded.get();
    }

    /** Runs stopped by something the graph had no plan for. */
    public long runsFailed() {
        return runsFailed.get();
    }

    /** Runs stopped because somebody asked them to stop. */
    public long runsCancelled() {
        return runsCancelled.get();
    }

    /** Runs in flight on this instance right now. */
    public long runsInFlight() {
        return runsInFlight.get();
    }

    /** Total time spent in runs, in milliseconds. With the run count, an average. */
    public long runMillis() {
        return runMillis.get();
    }

    /** Node executions that finished, successful or not. Retries count separately. */
    public long nodesRun() {
        return nodesRun.get();
    }

    /** Node executions that threw, retried ones included. */
    public long nodesFailed() {
        return nodesFailed.get();
    }

    /** Nodes a conditional passed over, or whose upstream never arrived. */
    public long nodesSkipped() {
        return nodesSkipped.get();
    }

    /** Nodes sent out to be run, wherever they ran. */
    public long nodesDispatched() {
        return nodesDispatched.get();
    }

    /**
     * Of those, the ones that ran here because the graph said {@code local(...)}.
     *
     * <p>Against {@link #nodesDispatched()} this is the ratio worth watching: a graph whose
     * work is nearly all local is not being spread, which is the reason this engine exists.
     */
    public long nodesLocal() {
        return nodesLocal.get();
    }

    /** Retries begun. A rising number is a service degrading before it fails outright. */
    public long retries() {
        return retries.get();
    }

    @Override
    public String toString() {
        return "DagStats[runs=" + runsStarted + " ok=" + runsSucceeded + " failed="
                + runsFailed + " cancelled=" + runsCancelled + " inFlight=" + runsInFlight
                + ", nodes=" + nodesRun + " failed=" + nodesFailed + " skipped=" + nodesSkipped
                + " local=" + nodesLocal + " retries=" + retries + "]";
    }
}
