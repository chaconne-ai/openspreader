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
package com.chaconneai.openspreader.example.dag;

import com.chaconneai.openspreader.aggregation.Emitter;
import com.chaconneai.openspreader.aggregation.MapReduceJob;
import com.chaconneai.openspreader.aggregation.ProcessingMapReduce;
import com.chaconneai.openspreader.dag.CompiledGraph;
import com.chaconneai.openspreader.dag.ExternalNode;
import com.chaconneai.openspreader.dag.GraphListener;
import com.chaconneai.openspreader.dag.GraphNode;
import com.chaconneai.openspreader.dag.GraphState;
import com.chaconneai.openspreader.dag.NodeOutcome;
import com.chaconneai.openspreader.dag.NodeStatus;
import com.chaconneai.openspreader.dag.ProcessingDag;
import com.chaconneai.openspreader.dag.Reducers;
import com.chaconneai.openspreader.dag.RunResult;
import com.chaconneai.openspreader.dag.ShardedNode;
import com.chaconneai.openspreader.dag.StateGraph;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A day's settlement, end to end.
 *
 * <p>Where {@link OrderFlowBestPractice} shows the five shapes on their own, this one is
 * what a workflow looks like once the things that go wrong are in it too: money that must
 * not move twice, an outside system that fails now and then, a batch whose size is not known
 * until it runs, and an operator who has to be told when none of it worked.
 *
 * <pre>
 *                  LoadMerchants                  the day's merchants
 *                        |
 *                  ComputePayouts                 sharded across the cluster
 *                        |
 *                      Total
 *                     /     \                     conditional
 *      total &gt; 0     /       \  otherwise
 *          TransferFunds     ArchiveOnly          a call to the bank, run in place
 *            |       \        /
 *  on failure|        \      /                    any-of: whichever path ran
 *    FlagForOperator   Archive
 * </pre>
 *
 * <h2>What this adds over the order example</h2>
 * <table border="1">
 *   <caption>The parts a real workflow needs</caption>
 *   <tr><th>Need</th><th>How</th></tr>
 *   <tr><td>The caller must supply the day, and may ask for a rehearsal</td>
 *       <td>{@code input("day")} and {@code input("dryRun", false)}</td></tr>
 *   <tr><td>Thousands of merchants, count unknown until it runs</td>
 *       <td>{@link ShardedNode} over the aggregation component</td></tr>
 *   <tr><td>The bank's API fails now and then</td>
 *       <td>{@code retry(TransferFunds.class, 2)}</td></tr>
 *   <tr><td>When it fails for good, a person has to hear about it</td>
 *       <td>{@code onFailure().to(FlagForOperator.class)}</td></tr>
 *   <tr><td>Calling another company's endpoint from a random replica buys nothing</td>
 *       <td>{@link ExternalNode} plus {@code local(...)}</td></tr>
 *   <tr><td>Audit: what ran, where, and what it cost</td>
 *       <td>a {@link GraphListener}, with the run's identity on every callback</td></tr>
 *   <tr><td>The coordinating instance died at 3am and this has to carry on</td>
 *       <td>{@code onNodeFinished} writes each step away, {@link #resumeFrom} takes it
 *       back</td></tr>
 * </table>
 *
 * <h2>Where the record lives is not this engine's business</h2>
 * There is no store here and no scheduler. The listener below writes to a map because an
 * example has to run somewhere; yours writes to a table, and that is the whole difference.
 * What the engine owes you is the seam, which is {@code onStart} / {@code onNodeFinished} on
 * the way out and {@link #resumeFrom} on the way back.
 *
 * <h2>The one thing this engine cannot do for you</h2>
 * <b>A retried node runs again in full.</b> The pool's reply cache covers "the answer was
 * lost, so the request went twice"; it does not cover "the node genuinely failed, so it is
 * being run again". A transfer that failed <b>after</b> the bank took the money will take it
 * again. So {@code TransferFunds} below sends an idempotency key derived from the day and
 * the merchant, which is what makes {@code retry(...)} safe to write on that line at all.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 13/09/2026
 */
public class SettlementFlowBestPractice {

    private final CompiledGraph flow;

    /**
     * Built once, in the constructor.
     *
     * <p>Compiling validates the whole graph, so a mistake in the shape is a startup failure
     * rather than a nightly one. Building it per run would also pay that cost per run.
     */
    public SettlementFlowBestPractice(ProcessingDag dag, SettlementAudit audit) {
        this.flow = dag.bind(StateGraph.create("daily-settlement")

                // ------------------------------------------------------
                // What the caller must supply
                // ------------------------------------------------------

                // Required. Missing it fails before a single node runs, naming the channel,
                // rather than surfacing as a null halfway down the graph
                .input("day")

                // Optional, with a default. A rehearsal computes every payout and sends no
                // money, which is what gets run in a staging environment
                .input("dryRun", false)

                // ------------------------------------------------------
                // Channels: how data merges
                // ------------------------------------------------------

                // Several nodes append to the trail, so it needs a reducer
                .channel("steps", Reducers.concatList())

                // Written by exactly one node each. writeOnce makes a second writer an
                // error rather than a silent overwrite, which on a money figure is worth
                // the one line
                .channel("payouts", Reducers.writeOnce("payouts"))
                .channel("total", Reducers.writeOnce("total"))

                // ------------------------------------------------------
                // The shape
                // ------------------------------------------------------

                .from(LoadMerchants.class).to(ComputePayouts.class)
                .from(ComputePayouts.class).to(Total.class)

                // Nothing to pay out is an ordinary day, not an error: skip the bank
                // entirely and archive. Note the guard on null, because SpEL evaluates
                // a missing channel to null rather than throwing
                .from(Total.class)
                    .when("#total != null and #total > 0").to(TransferFunds.class)
                    .otherwise(ArchiveOnly.class)

                // Three attempts in all. Safe only because the call carries an idempotency
                // key; see the class javadoc
                .retry(TransferFunds.class, 2)

                // Compensation. It runs only after the retries are used up, which is the
                // order this engine fixes so that a single network wobble cannot raise a
                // ticket for a transfer that then succeeded
                .from(TransferFunds.class).onFailure().to(FlagForOperator.class)

                // The bank call happens outside this application, so shipping it to a peer
                // adds a hop, a serialisation and a second thing that can fail
                .local(TransferFunds.class)

                // Whichever of the two paths ran, archive once. Without onAny() this would
                // wait for a branch that is never going to arrive
                .from(TransferFunds.class, ArchiveOnly.class).to(Archive.class).onAny()

                .listener(audit)
                .entry(LoadMerchants.class)
                .compile());
    }

    /**
     * Settles one day.
     *
     * @return what happened, whether or not it worked. A failure is reported rather than
     *         thrown, because "the transfer failed and the operator was told" is a run that
     *         did what the graph said to do, and the caller wants the numbers either way
     */
    public SettlementReport settle(String day) {
        // The day is the natural identity for a nightly run: one settlement per day, and the
        // record filed under the same key the business uses
        return settle(day, "settlement-" + day);
    }

    /** The same, under an identity of the application's choosing. */
    public SettlementReport settle(String day, String runId) {
        return report(flow.invoke(GraphState.of(Map.of("day", day)), runId));
    }

    /**
     * The definition as text, in whatever format is configured.
     *
     * <p>Not a picture: this is the graph's <b>serialisation</b>, and it reads back. A
     * workflow kept in a database column rather than in a constructor is stored with this
     * and rebuilt with {@code JsonRenderer.INSTANCE.load(text, catalog)}.
     *
     * <p>What it cannot carry is the code: node classes come back through a
     * {@code GraphCatalog} rather than by name, and the listener is deliberately left out,
     * being a run-time concern. Conditions survive because they are written as SpEL.
     */
    public String definition() {
        return flow.render();
    }

    /**
     * Carries on where an earlier run stopped.
     *
     * <p>The two arguments come out of the application's own store, written as the first run
     * proceeded; see {@code SettlementAudit.onNodeFinished}. Nodes named in {@code completed}
     * do not run again, which on this graph is the difference between a resumed settlement
     * and a second transfer.
     *
     * <p>Nothing here is the engine's: no store, no scheduler, no retention policy. It takes
     * back what it was given.
     */
    public SettlementReport resumeFrom(GraphState stored, Map<String, NodeStatus> completed,
                                       String runId) {
        return report(flow.resume(stored, completed, runId));
    }

    /** The same, computing everything and sending no money. */
    public SettlementReport rehearse(String day) {
        return report(flow.invoke(Map.of("day", day, "dryRun", true)));
    }

    private static SettlementReport report(RunResult result) {
        @SuppressWarnings("unchecked")
        Map<String, Long> payouts = (Map<String, Long>) result.state().get("payouts");
        return new SettlementReport(
                result.runId(),
                result.state().getString("day"),
                payouts == null ? Map.of() : payouts,
                result.state().contains("total") ? result.state().getLong("total") : 0L,
                !result.failed(),
                result.failedNode(),
                // Not the same question as "did it work": a run can succeed having had a
                // failure that the graph handled. This is how the caller tells the two apart
                result.failures().keySet(),
                result.state(),
                result.completed(),
                result.describe());
    }

    /**
     * What a day's settlement came to, and what it takes to carry on.
     *
     * <p>The last three are the record: an application files {@code runId}, {@code state}
     * and {@code completed} away, and {@link #resumeFrom} takes them back. They are on the
     * report rather than fished out of the engine, because <b>this</b> is the object the
     * application already stores.
     *
     * @param settled     whether the run finished without an unhandled failure
     * @param stoppedAt   the node that stopped it, or null when nothing did
     * @param recovered   nodes that failed and whose failure the graph had a plan for.
     *                    Empty on a clean run; holding {@code TransferFunds} means the money
     *                    did not move and an operator was told
     * @param state       the channels as they stood. Store it to resume later
     * @param completed   what actually ran, and how. Store it to resume later. A node that
     *                    should be tried again is removed from this map before handing it
     *                    back
     * @param description one line per node, for the audit log
     */
    public record SettlementReport(String runId, String day, Map<String, Long> payouts,
                                   long total, boolean settled, String stoppedAt,
                                   java.util.Set<String> recovered, GraphState state,
                                   Map<String, NodeStatus> completed, String description) {
    }

    // ==================================================================
    // The nodes. Each is an ordinary Spring bean, on every replica.
    // ==================================================================

    /**
     * The day's merchants.
     *
     * <p>A node injects whatever it needs, like any other bean. What must <b>not</b> go into
     * a channel is the repository itself: channels cross the network, so they carry data,
     * while a connection or a client stays in the bean where each replica has its own.
     */
    // @Component
    public static class LoadMerchants extends GraphNode {

        private final MerchantDirectory merchants;

        public LoadMerchants(MerchantDirectory merchants) {
            this.merchants = merchants;
        }

        @Override
        public Map<String, Object> execute(GraphState state) {
            List<String> ids = merchants.activeOn(state.getString("day"));
            return Map.of("merchantIds", ids,
                    "steps", List.of("loaded " + ids.size() + " merchant(s)"));
        }
    }

    /**
     * One payout per merchant, computed across the cluster.
     *
     * <p>Three lines of configuration and no scatter-gather of its own: the splitting, the
     * dispatch, the gathering and the timeout all belong to the aggregation component. Ten
     * thousand merchants do not become ten thousand dispatches either, because the job's
     * {@code split} decides how many shards there are.
     *
     * <p>An empty day is not a failure: nothing to shard writes an empty result and the
     * graph carries on.
     */
    // @Component
    public static class ComputePayouts extends ShardedNode {

        public ComputePayouts(ProcessingMapReduce mapReduce) {
            super(mapReduce);
        }

        @Override
        protected String jobBeanName() {
            return "payoutJob";
        }

        @Override
        protected String inputChannel() {
            return "merchantIds";
        }

        @Override
        protected String outputChannel() {
            return "payouts";
        }
    }

    /** Adds the payouts up. One writer, hence {@code writeOnce} on the channel. */
    // @Component
    public static class Total extends GraphNode {

        @Override
        public Map<String, Object> execute(GraphState state) {
            @SuppressWarnings("unchecked")
            Map<String, Long> payouts = (Map<String, Long>) state.get("payouts");
            long total = payouts == null ? 0L
                    : payouts.values().stream().mapToLong(Long::longValue).sum();
            return Map.of("total", total, "steps", List.of("total " + total));
        }
    }

    /**
     * The money, and the only step that talks to anyone outside.
     *
     * <p><b>{@link ExternalNode} rather than {@link GraphNode}</b>, and marked
     * {@code local(...)} in the graph: an HTTP call to another company is the same call from
     * whichever replica makes it, so sending it to a peer first buys nothing and adds two
     * ways to fail.
     *
     * <p>The idempotency key is what makes {@code retry(...)} legitimate on this node. A
     * retry runs the node again in full, and without a key the second attempt would be a
     * second transfer.
     */
    // @Component
    public static class TransferFunds extends ExternalNode {

        private final BankGateway bank;

        public TransferFunds(BankGateway bank) {
            this.bank = bank;
        }

        @Override
        protected Map<String, Object> call(GraphState state) throws Exception {
            String day = state.getString("day");
            long total = state.getLong("total");

            if (state.getBoolean("dryRun")) {
                // The rehearsal stops here, having computed everything above it
                return Map.of("steps", List.of("dry run, no transfer"));
            }

            // Derived from the day rather than generated, so the retry sends the same key
            // and the bank can recognise the repeat
            String reference = bank.transfer("settlement-" + day, total);
            return Map.of("bankReference", reference,
                    "steps", List.of("transferred " + total));
        }
    }

    /** Nothing to pay today. Reached by the conditional's else branch. */
    // @Component
    public static class ArchiveOnly extends GraphNode {
        @Override
        public Map<String, Object> execute(GraphState state) {
            return Map.of("steps", List.of("nothing to settle"));
        }
    }

    /**
     * The compensation.
     *
     * <p>It runs only once the retries are exhausted. Note what it does <b>not</b> do: it
     * does not try the transfer again and it does not reverse anything, because nothing has
     * been reversed. Money that may or may not have moved is a question for a person.
     */
    // @Component
    public static class FlagForOperator extends GraphNode {

        private final OperatorQueue operators;

        public FlagForOperator(OperatorQueue operators) {
            this.operators = operators;
        }

        @Override
        public Map<String, Object> execute(GraphState state) {
            operators.raise("settlement " + state.getString("day") + " could not be transferred",
                    state.getLong("total"));
            return Map.of("steps", List.of("operator notified"));
        }
    }

    /** The record of the day, whichever way it went. */
    // @Component
    public static class Archive extends GraphNode {
        @Override
        public Map<String, Object> execute(GraphState state) {
            return Map.of("steps", List.of("archived"));
        }
    }

    // ==================================================================
    // The per-shard work: a MapReduceJob bean, not a GraphNode
    // ==================================================================

    /**
     * What runs per shard.
     *
     * <p>It is a {@link MapReduceJob} rather than a node, which is the one constraint a
     * sharded step carries: a shard cannot have conditional edges or a fan-in of its own. It
     * <b>can</b> call {@code invoke} on a graph of its own, though, so per-shard work with
     * internal dependencies is expressible; it simply lives in the job.
     */
    // @Component("payoutJob")
    public static class PayoutJob implements MapReduceJob<List<String>, String, Long, Long> {

        private final LedgerReader ledger;

        public PayoutJob(LedgerReader ledger) {
            this.ledger = ledger;
        }

        /**
         * How many pieces, decided here rather than by the engine.
         *
         * <p>{@code suggestedShards} is the cluster size. Shards are capped at the number of
         * merchants, because a shard with nothing in it is a dispatch that buys nothing.
         */
        @Override
        public List<List<String>> split(List<String> merchantIds, int suggestedShards) {
            int shards = Math.max(1, Math.min(suggestedShards, merchantIds.size()));
            int perShard = (merchantIds.size() + shards - 1) / shards;
            List<List<String>> out = new ArrayList<>(shards);
            for (int i = 0; i < merchantIds.size(); i += perShard) {
                out.add(new ArrayList<>(
                        merchantIds.subList(i, Math.min(i + perShard, merchantIds.size()))));
            }
            return out;
        }

        /** Runs on whichever replica got this shard. */
        @Override
        public void map(List<String> shard, Emitter<String, Long> emitter) {
            for (String merchantId : shard) {
                emitter.emit(merchantId, ledger.owedTo(merchantId));
            }
        }

        /** One merchant's pieces, added up. */
        @Override
        public Long reduce(String merchantId, List<Long> amounts) {
            return amounts.stream().mapToLong(Long::longValue).sum();
        }
    }

    // ==================================================================
    // The audit trail
    // ==================================================================

    /**
     * A listener: told what the run is doing, and part of none of it.
     *
     * <p>Two things about it are worth stating, because both are easy to get wrong:
     *
     * <p><b>It must be quick.</b> It runs on the coordinating thread, so a listener that
     * blocks on a network call holds up every node waiting to be dispatched. Hand slow work
     * to an executor of your own.
     *
     * <p><b>It must not compensate.</b> {@code onFailure} here is an observer; the failure
     * <b>edge</b> is what decides where the run goes. A listener that tried to put things
     * right would be doing it outside the graph, where nothing records that it happened.
     */
    // @Component
    public static class SettlementAudit implements GraphListener {

        private final Map<String, String> trail = new LinkedHashMap<>();

        @Override
        public void onStart(String runId, CompiledGraph graph, GraphState initial) {
            // Where an application that keeps its own record opens the entry: the identity,
            // the definition (graph.render() is the text), and what the caller supplied
            trail.put("started " + runId, initial.getString("day"));
        }

        /**
         * The hook to persist against.
         *
         * <p>An application storing {@code (runId, node, ok, updates)} as each row arrives
         * has, at any moment, everything {@code CompiledGraph.resume(...)} needs. That is the
         * whole of "persistence is the application's job": the engine says what happened, the
         * application decides where it goes and how long it lives.
         */
        @Override
        public void onNodeFinished(String runId, NodeOutcome outcome) {
            // runs.record(runId, outcome.node(), outcome.ok(), outcome.updates());
            trail.put(outcome.node(), outcome.ok() ? "ok" : "failed");
        }

        @Override
        public void onTransition(String runId, String from, String to, GraphState state) {
            trail.put(from + "->" + to, state.getString("day"));
        }

        @Override
        public void onRetry(String runId, String node, int attempt, Throwable cause) {
            // Worth a metric of its own: a rising retry count is the bank degrading, and it
            // shows up here long before anything fails outright
            trail.put(node + "#" + attempt, cause.toString());
        }

        @Override
        public void onSuccess(RunResult result) {
            // A run that was compensated arrives here, not at onFailure: it did what the
            // graph said to do. result.failures() is how the two are told apart
            trail.put("finished", result.failures().isEmpty() ? "clean" : "compensated");
        }

        @Override
        public void onFailure(RunResult result, Throwable cause) {
            trail.put("failed at " + result.failedNode(), String.valueOf(cause));
        }

        /** What the run did, in order. */
        public Map<String, String> trail() {
            return trail;
        }
    }

    // ==================================================================
    // The collaborators. Your own interfaces, injected as usual.
    // ==================================================================

    /** The merchants trading on a given day. */
    public interface MerchantDirectory {
        List<String> activeOn(String day);
    }

    /** What one merchant is owed. Read on whichever replica holds the shard. */
    public interface LedgerReader {
        long owedTo(String merchantId);
    }

    /** The bank. The idempotency key is the first argument, and it matters; see above. */
    public interface BankGateway {
        String transfer(String idempotencyKey, long amount) throws Exception;
    }

    /** Where a human being finds out. */
    public interface OperatorQueue {
        void raise(String what, long amount);
    }
}
