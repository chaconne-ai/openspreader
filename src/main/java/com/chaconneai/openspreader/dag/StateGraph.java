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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Builds a graph, one chained call at a time.
 *
 * <p>The aim of the shape below is that the code and the picture are the same thing:
 *
 * <pre>{@code
 * CompiledGraph flow = StateGraph.create("order-flow")
 *
 *         .channel("items", Reducers.concatList())
 *
 *         .from(Validate.class).to(Reserve.class, Charge.class)   // fan-out, in parallel
 *         .from(Reserve.class, Charge.class).to(Ship.class)       // fan-in, waits for both
 *
 *         .entry(Validate.class)
 *         .compile();
 * }</pre>
 *
 * <h2>The five shapes</h2>
 * <table border="1">
 *   <caption>Every edge semantics this engine has</caption>
 *   <tr><th>Shape</th><th>Written as</th></tr>
 *   <tr><td>Dependency, A then B</td><td>{@code .from(A.class).to(B.class)}</td></tr>
 *   <tr><td>Fan-out, B and C in parallel</td>
 *       <td>{@code .from(A.class).to(B.class, C.class)}</td></tr>
 *   <tr><td>Fan-in, D after all of them</td>
 *       <td>{@code .from(A.class, B.class, C.class).to(D.class)}</td></tr>
 *   <tr><td>Any-of, D after the first of them</td>
 *       <td>{@code .from(A.class, B.class, C.class).to(D.class).onAny()}</td></tr>
 *   <tr><td>Conditional, one branch of several</td>
 *       <td>{@code .from(A.class).switchOn(router).caseOf("big", B.class)...}</td></tr>
 * </table>
 *
 * <h2>Classes, not strings</h2>
 * Nodes are named by their class, so a typo is a compile error rather than a run that dies
 * halfway through with "no such node". The node's name in the graph is the class's simple
 * name unless {@link #node(String, Class)} gives it one, which is how the same class can
 * appear twice under different names.
 *
 * <p>Declaring nodes separately is optional: any class mentioned in {@link #from} or
 * {@code to} is registered on the spot. {@link #node} exists for the cases that need more
 * than a mention, such as a second copy of a class or an explicit trigger.
 *
 * <h2>Nothing is checked until compile()</h2>
 * Deliberately. Edges arrive in whatever order reads best, and a forward reference to a node
 * declared three lines later is normal. {@link #compile()} is where a graph is either
 * pronounced sound or refused; see {@link CompiledGraph} for the list of what it refuses.
 *
 * <h2>This builder is not thread-safe, and does not need to be</h2>
 * A graph is built once, on one thread, usually in a bean's constructor. The
 * {@link CompiledGraph} it produces is immutable and is the thing that gets shared.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class StateGraph {

    private final String name;

    /** Channel name to how it merges. A channel with no reducer here takes the default. */
    private final Map<String, Reducer<?>> reducers = new LinkedHashMap<>();

    /** Node name to what it is. Insertion-ordered, so rendering is stable. */
    private final Map<String, NodeSpec> nodes = new LinkedHashMap<>();

    /**
     * Plain edges: source, then target, then what has to become of the source for the edge to
     * carry.
     *
     * <p>A map of maps rather than a set of targets, because an edge now has a property of
     * its own. See {@link EdgeCondition}.
     */
    private final Map<String, Map<String, EdgeCondition>> edges = new LinkedHashMap<>();

    /** Conditional edges, in declaration order. */
    private final List<ConditionalSpec> conditionals = new ArrayList<>();

    /**
     * Triggers that were asked for explicitly.
     *
     * <p>Kept apart from {@link NodeSpec} so that asking for two different ones can be
     * <b>detected</b> rather than silently resolved by declaration order. See
     * {@link #requireTrigger}.
     */
    private final Map<String, Trigger> declaredTriggers = new LinkedHashMap<>();

    /**
     * Where runs start. A set, because a DAG may legitimately have several roots: two
     * independent sources feeding one join is an ordinary shape, and forcing a fake node in
     * front of them to make a single entry would put a step in the picture that does nothing.
     */
    private final Set<String> entries = new LinkedHashSet<>();

    /**
     * Nodes that run on the coordinating instance instead of being dispatched.
     *
     * <p>Declared here rather than on the node, because the coordinator is the one that has to
     * know and it has no instance to ask. See {@link NodeInvoker}.
     */
    private final Set<String> localNodes = new LinkedHashSet<>();

    /** How many further attempts each node gets after a failure. Absent means none. */
    private final Map<String, Integer> retries = new LinkedHashMap<>();
    private final Map<String, Backoff> backoffs = new LinkedHashMap<>();
    private final Map<String, Predicate<Throwable>> retryable = new LinkedHashMap<>();

    /** Observers, in declaration order. */
    private final List<GraphListener> listeners = new ArrayList<>();

    /**
     * Channels a run has to be given, and what to fill in when it is optional.
     *
     * <p>A value of {@link #REQUIRED} marks one that has no default and must be supplied.
     */
    private final Map<String, Object> inputs = new LinkedHashMap<>();

    /** Marks an input that has no default. A sentinel, because null is a legitimate default. */
    static final Object REQUIRED = new Object();

    private StateGraph(String name) {
        this.name = name;
    }

    /** @param name the graph's name. It appears in logs, metrics and rendered pictures */
    public static StateGraph create(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("a graph needs a name");
        }
        return new StateGraph(name.trim());
    }

    // ------------------------------------------------------------------
    // Channels
    // ------------------------------------------------------------------

    /**
     * Declares how a channel merges when more than one node writes it.
     *
     * <p>A channel only one node ever writes needs no declaration. One that <b>two parallel
     * branches</b> write does, and {@link #compile()} refuses the graph without it, because
     * the alternative is one of the two writes disappearing with nothing reported.
     *
     * @see Reducers
     */
    public <T> StateGraph channel(String channel, Reducer<T> reducer) {
        if (channel == null || channel.isBlank()) {
            throw new IllegalArgumentException("a channel needs a name");
        }
        if (reducer == null) {
            throw new IllegalArgumentException("channel " + channel + " was given a null reducer");
        }
        reducers.put(channel.trim(), reducer);
        return this;
    }

    /**
     * Declares a channel the run must be given.
     *
     * <pre>{@code
     * .input("orderId")                 // required
     * .input("currency", "USD")         // optional, filled in when absent
     * }</pre>
     *
     * <h2>Why this is not just documentation</h2>
     * Without it, forgetting an input is not an error: the entry node is simply handed a state
     * without it, reads null, and either throws somewhere deep or, worse, carries on with
     * null and produces a plausible wrong answer. The stack trace then points at a node three
     * steps in, and the actual mistake was at the call site.
     *
     * <p>Declared, {@code invoke} refuses <b>before any node runs</b>, naming what is missing.
     *
     * <h2>It is checked at invoke, not at compile</h2>
     * Unlike everything else {@code compile()} refuses, this one cannot be: what a caller will
     * pass is not known until it passes it. So this is the one check that happens per run.
     *
     * <p>It also does not say anything about channels <b>nodes</b> write for each other. Those
     * are between the nodes, and a node that needs one and finds it missing should say so
     * itself, as {@link ShardedNode} does.
     *
     * @param channel the channel that must be supplied
     */
    public StateGraph input(String channel) {
        if (channel == null || channel.isBlank()) {
            throw new IllegalArgumentException("an input needs a channel name");
        }
        inputs.put(channel.trim(), REQUIRED);
        return this;
    }

    /**
     * Declares an optional input and what to use when it is absent.
     *
     * <p>{@code null} is a legitimate default and means "leave the channel unset but do not
     * complain", which is different from not declaring the input at all only in that it is
     * written down.
     */
    public StateGraph input(String channel, Object defaultValue) {
        if (channel == null || channel.isBlank()) {
            throw new IllegalArgumentException("an input needs a channel name");
        }
        inputs.put(channel.trim(), defaultValue);
        return this;
    }

    // ------------------------------------------------------------------
    // Nodes
    // ------------------------------------------------------------------

    /** Registers a node under its class's simple name. */
    public StateGraph node(Class<? extends GraphNode> type) {
        register(simpleNameOf(type), type);
        return this;
    }

    /**
     * Registers a node under a name of your choosing.
     *
     * <p>What this is for: putting the <b>same class in twice</b>. A graph that validates at
     * the start and validates again after enrichment wants one class and two nodes, and
     * without a name they would be one node with a cycle through it.
     */
    public StateGraph node(String nodeName, Class<? extends GraphNode> type) {
        register(nodeName, type);
        return this;
    }

    /** Registers a node and fixes its trigger in one go. */
    public StateGraph node(String nodeName, Class<? extends GraphNode> type, Trigger trigger) {
        register(nodeName, type);
        requireTrigger(nodeName, trigger);
        return this;
    }

    /**
     * Registers a node with settings of its own.
     *
     * <p>The settings reach the node as {@link NodeContext}, which is how <b>one class can
     * serve as many steps</b>: the class says how to do the work, and each node says what to
     * do it to.
     *
     * <pre>{@code
     * .node("FetchRate", HttpGraphNode.class, Map.of(
     *         "url", "https://fx.internal/rates/${currency}",
     *         "method", "GET",
     *         "writeTo", "rate"))
     * }</pre>
     *
     * <p>Values must be the shapes a definition can hold: text, numbers, booleans, lists and
     * maps of those. They travel with the node to whichever replica runs it, and they are
     * written out with the graph.
     */
    public StateGraph node(String nodeName, Class<? extends GraphNode> type,
                           Map<String, Object> config) {
        register(nodeName, type, null, config);
        return this;
    }

    /**
     * Registers a node by <b>bean name</b>, with no class at all.
     *
     * <p>This is the form a graph built from data uses: the rows say which bean and with what
     * settings, and nothing has to be compiled against a class. The node's name is the bean's
     * name.
     *
     * <pre>{@code
     * for (StepRow row : repository.stepsOf(workflowId)) {
     *     graph.node(row.name(), row.beanName(), row.config());
     * }
     * }</pre>
     *
     * <p>It is also how <b>one class has several beans</b>: a class name cannot tell two
     * beans of one class apart, and a bean name can.
     */
    public StateGraph node(String beanName) {
        register(beanName, null, beanName, null);
        return this;
    }

    /** The same, with settings. The node's name is the bean's name. */
    public StateGraph node(String beanName, Map<String, Object> config) {
        register(beanName, null, beanName, config);
        return this;
    }

    /**
     * The same, under a node name of its own.
     *
     * <p>For using one bean as several steps: the name comes first, as it does in
     * {@link #node(String, Class)}.
     */
    public StateGraph node(String nodeName, String beanName, Map<String, Object> config) {
        register(nodeName, null, beanName, config);
        return this;
    }

    /** The same, with no settings. */
    public StateGraph nodeOfBean(String nodeName, String beanName) {
        register(nodeName, null, beanName, null);
        return this;
    }

    /**
     * Decides, per node, which failures are worth trying again.
     *
     * <pre>{@code
     * // your own rule
     * .retryWhen(Charge.class, thrown -> !(thrown instanceof InsufficientFunds))
     *
     * // or the classifier your application already configures for Spring Retry
     * .retryWhen(Charge.class, classifier::classify)
     * }</pre>
     *
     * <h2>Why this is a predicate and not a retry framework</h2>
     * Deciding <b>whether</b> an exception is worth another attempt is a pure question about
     * that exception, and Spring Retry's {@code BinaryExceptionClassifier} answers it well.
     * A method reference is all it takes to use one, and nothing here has to know it exists.
     *
     * <p>What could <b>not</b> be handed over is the retrying itself. {@code RetryTemplate}
     * loops in the calling thread and sleeps between attempts; here an attempt is a
     * <b>dispatch</b>, the work runs on another replica, and the coordinating thread must
     * stay free to settle every other branch of the graph. A run that slept through its
     * backoff would stall the branches that had nothing to do with it.
     *
     * <p>Without this, every failure is retried, which is right for a timeout and wasteful
     * for "insufficient funds": three more attempts, two more backoffs, and the same answer.
     *
     * <p>A predicate is code, so it is <b>not</b> written out with the definition; see
     * {@link GraphCatalog} for the same limit on lambda conditions and reducers.
     */
    public StateGraph retryWhen(Class<? extends GraphNode> type, Predicate<Throwable> worthRetrying) {
        return retryWhen(register(simpleNameOf(type), type), worthRetrying);
    }

    /** The same, by node name. */
    public StateGraph retryWhen(String nodeName, Predicate<Throwable> worthRetrying) {
        if (worthRetrying == null) {
            throw new IllegalArgumentException("retryWhen() needs a predicate");
        }
        retryable.put(nodeName, worthRetrying);
        return this;
    }

    /**
     * Fixes a node's trigger by name.
     *
     * <p>The form a graph built from data needs: there is no class to hang it off, and
     * declaring the node and declaring how it fires are two separate rows.
     */
    public StateGraph trigger(String nodeName, Trigger trigger) {
        requireTrigger(nodeName, trigger);
        return this;
    }

    /**
     * Marks nodes as running <b>on the coordinating instance</b>, not dispatched.
     *
     * <pre>{@code
     * .local(FetchRates.class, PostToSlack.class)
     * }</pre>
     *
     * <p>What it is for is work that happens <b>outside this application</b>: an HTTP API, a
     * queue, someone else's service. Calling another company's endpoint from replica A and
     * from replica B is the same call, so shipping it to another replica adds a hop, a
     * serialisation and a second thing that can fail, and buys nothing.
     *
     * <p>A local node also does not have to be a bean on every replica, which is the one
     * constraint dispatched nodes carry.
     *
     * <p>Marking a <b>compute</b> step local is almost always a mistake: it pins the work to
     * one instance and gives up the reason this engine exists.
     *
     * @see ExternalNode
     */
    @SafeVarargs
    public final StateGraph local(Class<? extends GraphNode>... types) {
        for (Class<? extends GraphNode> type : types) {
            localNodes.add(register(simpleNameOf(type), type));
        }
        return this;
    }

    /** The same, by node name. */
    public StateGraph local(String... nodeNames) {
        localNodes.addAll(Arrays.asList(nodeNames));
        return this;
    }

    /**
     * Tries a node again when it fails. <b>Off by default</b>, for every node.
     *
     * <pre>{@code
     * .retry(Charge.class, 2)                              // three attempts in all
     * .from(Charge.class).onFailure().to(Refund.class)     // only after all three fail
     * }</pre>
     *
     * <h2>The order matters, and it is the reason this lives on the graph</h2>
     * Retries are exhausted <b>before</b> a failure edge is taken. Written by hand inside a
     * node, the two get inverted about as often as not, and an inverted one turns a single
     * network wobble into a refund.
     *
     * <h2>The node must be idempotent</h2>
     * <b>This is on the caller, and the engine cannot check it.</b> The process pool's reply
     * cache protects against "the reply was lost, so the request was sent twice"; it does not
     * protect against "the node genuinely failed, so it is run again". A charge that fails
     * after taking the money will take it again.
     *
     * <p>Retry a read, a lookup, a call to something with an idempotency key. Think twice
     * about anything else.
     *
     * <h2>There is no backoff in this version</h2>
     * The next attempt starts at once. Against a service that is struggling rather than
     * flapping, an immediate retry is not much of a favour, so keep the count low. A delay
     * would need a scheduler, and this engine does not own one.
     *
     * @param times how many <b>further</b> attempts after the first. 0 removes the retry
     */
    public StateGraph retry(Class<? extends GraphNode> type, int times) {
        return retry(register(simpleNameOf(type), type), times);
    }

    /** The same, by node name. */
    public StateGraph retry(String nodeName, int times) {
        return retry(nodeName, times, Backoff.none());
    }

    /**
     * Tries a node again, waiting between attempts.
     *
     * <pre>{@code
     * .retry(Charge.class, 2, Backoff.exponential(Duration.ofMillis(200)))
     * }</pre>
     *
     * <p>Against a service that is struggling rather than flapping, three attempts inside a
     * few milliseconds are three requests it did not need. See {@link Backoff}, including why
     * the waiting costs no thread.
     */
    public StateGraph retry(Class<? extends GraphNode> type, int times, Backoff backoff) {
        return retry(register(simpleNameOf(type), type), times, backoff);
    }

    /** The same, by node name. */
    public StateGraph retry(String nodeName, int times, Backoff backoff) {
        if (times < 0) {
            throw new IllegalArgumentException("retry count must not be negative: " + times);
        }
        if (times == 0) {
            retries.remove(nodeName);
            backoffs.remove(nodeName);
            return this;
        }
        retries.put(nodeName, times);
        if (backoff != null && !backoff.isNone()) {
            backoffs.put(nodeName, backoff);
        } else {
            backoffs.remove(nodeName);
        }
        return this;
    }

    /**
     * Adds an observer. Several may be added; they are called in declaration order.
     *
     * @see GraphListener
     */
    public StateGraph listener(GraphListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("a listener must not be null");
        }
        listeners.add(listener);
        return this;
    }

    // ------------------------------------------------------------------
    // Edges
    // ------------------------------------------------------------------

    /**
     * Starts an edge. One source is a dependency or a fan-out; several are a fan-in.
     *
     * <pre>{@code
     * .from(A.class).to(B.class)                    // A then B
     * .from(A.class).to(B.class, C.class)           // B and C in parallel
     * .from(A.class, B.class).to(C.class)           // C waits for both
     * }</pre>
     */
    @SafeVarargs
    public final From from(Class<? extends GraphNode>... sources) {
        if (sources == null || sources.length == 0) {
            throw new IllegalArgumentException("from() needs at least one source node");
        }
        List<String> names = new ArrayList<>(sources.length);
        for (Class<? extends GraphNode> type : sources) {
            names.add(register(simpleNameOf(type), type));
        }
        return new From(names);
    }

    /** The same, by node name. For graphs assembled at run time from configuration. */
    public From from(String... sources) {
        if (sources == null || sources.length == 0) {
            throw new IllegalArgumentException("from() needs at least one source node");
        }
        return new From(Arrays.asList(sources));
    }

    // ------------------------------------------------------------------
    // Entry and compilation
    // ------------------------------------------------------------------

    /**
     * Where a run starts. Call it more than once, or pass several classes, for a graph with
     * several roots.
     *
     * <p>Every entry is dispatched at once when the run begins, so two independent roots run
     * in parallel without an edge between them.
     */
    @SafeVarargs
    public final StateGraph entry(Class<? extends GraphNode>... types) {
        for (Class<? extends GraphNode> type : types) {
            entries.add(register(simpleNameOf(type), type));
        }
        return this;
    }

    /** The same, by node name. */
    public StateGraph entry(String... nodeNames) {
        entries.addAll(Arrays.asList(nodeNames));
        return this;
    }

    /**
     * Checks the graph over and freezes it.
     *
     * @throws DagException with a message naming the offending nodes or channels. See
     *                      {@link CompiledGraph} for everything that is checked
     */
    public CompiledGraph compile() {
        return CompiledGraph.of(name, entries, nodes, edges, conditionals, reducers,
                declaredTriggers, localNodes, retries, backoffs, retryable, listeners,
                inputs);
    }

    // ------------------------------------------------------------------
    // Chained edge builders
    // ------------------------------------------------------------------

    /**
     * The continuation methods every chained builder carries.
     *
     * <p>Without it each of {@link From}, {@link To}, {@link Switch} and {@link When} would
     * hand-copy the same delegations, and the copies would drift: the first version of this
     * class left {@code node(...)} off {@code To} alone, and the only symptom was that one
     * particular chain would not compile while three others would.
     *
     * <p>They are one-line delegations to the enclosing builder, and their whole purpose is
     * that a chain never has to be broken to get back to the graph.
     */
    public abstract class Chain {

        /** Continues from one or more source nodes. */
        @SafeVarargs
        public final From from(Class<? extends GraphNode>... sources) {
            return StateGraph.this.from(sources);
        }

        /** Continues from one or more source nodes, by name. */
        public final From from(String... sources) {
            return StateGraph.this.from(sources);
        }

        /** Declares how a channel merges. */
        public final <T> StateGraph channel(String channel, Reducer<T> reducer) {
            return StateGraph.this.channel(channel, reducer);
        }

        /** Registers a node that no edge happens to mention yet. */
        public final StateGraph node(Class<? extends GraphNode> type) {
            return StateGraph.this.node(type);
        }

        /** Registers a node under a name of its own. */
        public final StateGraph node(String nodeName, Class<? extends GraphNode> type) {
            return StateGraph.this.node(nodeName, type);
        }

        /** Registers a node and fixes its trigger. */
        public final StateGraph node(String nodeName, Class<? extends GraphNode> type,
                                     Trigger trigger) {
            return StateGraph.this.node(nodeName, type, trigger);
        }

        /** Sets an entry node. Call it more than once for a graph with several roots. */
        @SafeVarargs
        public final StateGraph entry(Class<? extends GraphNode>... types) {
            return StateGraph.this.entry(types);
        }

        /** Sets entry nodes by name. */
        public final StateGraph entry(String... nodeNames) {
            return StateGraph.this.entry(nodeNames);
        }

        /** Marks nodes as running on the coordinating instance. */
        @SafeVarargs
        public final StateGraph local(Class<? extends GraphNode>... types) {
            return StateGraph.this.local(types);
        }

        /** The same, by node name. A graph built by name needs this to stay one chain. */
        public final StateGraph local(String... nodeNames) {
            return StateGraph.this.local(nodeNames);
        }

        /** Tries a node again when it fails. Off by default. */
        public final StateGraph retry(Class<? extends GraphNode> type, int times) {
            return StateGraph.this.retry(type, times);
        }

        /** The same, by node name. */
        public final StateGraph retry(String nodeName, int times) {
            return StateGraph.this.retry(nodeName, times);
        }

        /** Retries with a wait between attempts; see {@link Backoff}. */
        public final StateGraph retry(Class<? extends GraphNode> type, int times,
                                      Backoff backoff) {
            return StateGraph.this.retry(type, times, backoff);
        }

        /** The same, by node name. */
        public final StateGraph retry(String nodeName, int times, Backoff backoff) {
            return StateGraph.this.retry(nodeName, times, backoff);
        }

        /** Which failures are worth another attempt; see {@link StateGraph#retryWhen}. */
        public final StateGraph retryWhen(Class<? extends GraphNode> type,
                                          Predicate<Throwable> worthRetrying) {
            return StateGraph.this.retryWhen(type, worthRetrying);
        }

        /** The same, by node name. */
        public final StateGraph retryWhen(String nodeName, Predicate<Throwable> worthRetrying) {
            return StateGraph.this.retryWhen(nodeName, worthRetrying);
        }

        /** Adds an observer. */
        public final StateGraph listener(GraphListener listener) {
            return StateGraph.this.listener(listener);
        }

        /** Declares a required input channel. */
        public final StateGraph input(String channel) {
            return StateGraph.this.input(channel);
        }

        /** Declares an optional input channel and its default. */
        public final StateGraph input(String channel, Object defaultValue) {
            return StateGraph.this.input(channel, defaultValue);
        }

        /** Checks the graph over and freezes it. */
        public final CompiledGraph compile() {
            return StateGraph.this.compile();
        }
    }

    /**
     * What {@link StateGraph#from} returns: the sources are fixed, the targets are not yet.
     *
     * <p>It carries the continuation methods ({@code from}, {@code channel}, {@code entry},
     * {@code compile}) so that a chain never has to be broken to get back to the graph. That
     * is a handful of one-line delegations, and it buys a DSL that reads as one sentence per
     * edge.
     */
    public class From extends Chain {

        private final List<String> sources;
        private final EdgeCondition condition;

        private From(List<String> sources) {
            this(sources, EdgeCondition.ON_SUCCESS);
        }

        private From(List<String> sources, EdgeCondition condition) {
            this.sources = sources;
            this.condition = condition;
        }

        /**
         * The edges declared next carry when the source <b>fails</b> rather than succeeds.
         *
         * <pre>{@code
         * .from(Charge.class).to(Ship.class)                     // the happy path
         * .from(Charge.class).onFailure().to(Refund.class)       // the other one
         * }</pre>
         *
         * <p>Declaring one changes something beyond routing: a failure that <b>has somewhere
         * to go no longer halts the run</b>. Without it, any node throwing stops the graph,
         * which is the right default but leaves no way to compensate.
         */
        public From onFailure() {
            return new From(sources, EdgeCondition.ON_FAILURE);
        }

        /**
         * The edges declared next carry however the source ended, succeeded or failed.
         *
         * <pre>{@code
         * .from(Ship.class, Refund.class).onComplete().to(Notify.class)
         * }</pre>
         *
         * <p>With {@link Trigger#all()}, which is the default, this is the "wait for
         * everything upstream to be over, whatever happened" that other engines spell as a
         * trigger mode of its own. Here it falls out of the two dimensions instead.
         *
         * <p><b>A skipped source still carries nothing.</b> "Complete" means the node ran; a
         * branch a conditional passed over did not.
         */
        public From onComplete() {
            return new From(sources, EdgeCondition.ON_COMPLETE);
        }

        /** Plain edges from every source to every target. */
        @SafeVarargs
        public final To to(Class<? extends GraphNode>... targets) {
            List<String> names = new ArrayList<>(targets.length);
            for (Class<? extends GraphNode> type : targets) {
                names.add(register(simpleNameOf(type), type));
            }
            return link(names);
        }

        /** The same, by node name. */
        public To to(String... targets) {
            return link(Arrays.asList(targets));
        }

        private To link(List<String> targets) {
            for (String source : sources) {
                Map<String, EdgeCondition> out =
                        edges.computeIfAbsent(source, k -> new LinkedHashMap<>());
                for (String target : targets) {
                    EdgeCondition existing = out.putIfAbsent(target, condition);
                    if (existing != null && existing != condition) {
                        // Declaring one edge twice with two conditions is more likely a
                        // mistake than an intent, and the intent it might be has a spelling
                        // of its own
                        throw new DagException("edge " + source + " -> " + target
                                + " is declared both " + existing + " and " + condition
                                + ". For an edge that carries either way, declare it once "
                                + "with onComplete()");
                    }
                }
            }
            return new To(targets);
        }

        /**
         * Turns this into a conditional: the router reads the state and names one branch.
         *
         * <p>The switch is evaluated <b>once, after the source finishes</b>, and exactly one
         * branch is taken. Every other declared branch is marked
         * {@link NodeStatus#SKIPPED}, which is what lets a fan-in further downstream
         * complete instead of waiting for a branch that will never arrive.
         *
         * <pre>{@code
         * .from(Score.class)
         *     .switchOn(s -> s.getInt("risk") > 80 ? "manual" : "auto")
         *     .caseOf("manual", HumanReview.class)
         *     .caseOf("auto",   AutoApprove.class)
         * }</pre>
         *
         * <p>With several sources this reads "after all of them, decide once".
         */
        public Switch switchOn(Function<GraphState, String> router) {
            if (router == null) {
                throw new IllegalArgumentException("switchOn() needs a router");
            }
            ConditionalSpec spec = new ConditionalSpec(sources, router);
            conditionals.add(spec);
            return new Switch(spec);
        }

        /**
         * The same, written as a Spring expression whose value is the branch key.
         *
         * <pre>{@code
         * .from(Classify.class)
         *     .switchOn("#kind")                  // the "kind" channel names the branch
         *     .caseOf("book",  ShipMedia.class)
         *     .caseOf("fresh", ShipCold.class)
         * }</pre>
         *
         * <p>Every channel is bound as a variable of its own name, and the whole state as
         * {@code #state}. The expression is parsed now, so a malformed one fails here rather
         * than part-way through the first run. See {@link Expressions} for when a string is
         * the right choice and when a lambda is.
         */
        public Switch switchOn(String spel) {
            Switch result = switchOn(Expressions.router(spel));
            // Kept so the graph can be written out and read back; a lambda router cannot be
            result.spec.setExpression(spel);
            return result;
        }

        /**
         * The predicate form of a switch. Predicates are tried <b>in declaration order</b>
         * and the first true one wins.
         *
         * <pre>{@code
         * .from(Score.class)
         *     .when(s -> s.getInt("risk") > 80).to(HumanReview.class)
         *     .when(s -> s.getInt("risk") > 40).to(SecondLook.class)
         *     .otherwise(AutoApprove.class)
         * }</pre>
         *
         * <p>It compiles down to the same thing {@link #switchOn} builds, so the two forms
         * behave identically and render identically.
         */
        public When when(Predicate<GraphState> predicate) {
            if (predicate == null) {
                throw new IllegalArgumentException("when() needs a predicate");
            }
            PredicateSpec spec = new PredicateSpec(sources);
            conditionals.add(spec.conditional());
            spec.conditional().addPredicateExpression(null);
            return new When(spec, predicate);
        }

        /**
         * The same, written as a Spring expression that evaluates to a boolean.
         *
         * <pre>{@code
         * .from(Score.class)
         *     .when("#risk > 80").to(HumanReview.class)
         *     .when("#risk > 40").to(SecondLook.class)
         *     .otherwise(AutoApprove.class)
         * }</pre>
         *
         * <p>Parsed now, so a typo in the expression is a build error. A channel that may be
         * absent needs a guard: {@code "#risk != null and #risk > 80"}.
         */
        public When when(String spel) {
            When result = when(Expressions.predicate(spel));
            // Kept so the graph can be written out and read back, as switchOn(String) does
            result.spec.conditional().replaceLastPredicateExpression(spel);
            return result;
        }

    }

    /**
     * What {@code to(...)} returns. Its only job beyond continuing the chain is
     * {@link #onAny()}.
     */
    public class To extends Chain {

        private final List<String> targets;

        private To(List<String> targets) {
            this.targets = targets;
        }

        /**
         * The targets fire as soon as the <b>first</b> source succeeds, rather than waiting
         * for all of them.
         *
         * <p>Two things about it are easy to guess wrongly, so both are stated here:
         * <ul>
         *   <li>The node still runs <b>exactly once</b>. Sources finishing later do not
         *       trigger it again</li>
         *   <li>Those later updates are <b>still merged</b> into the final state. Nothing
         *       that was computed is thrown away just because it arrived after the race was
         *       decided</li>
         * </ul>
         *
         * <p>It sets a property of the <b>target node</b>, not of this edge. Declaring the
         * same node with both triggers in different places is refused by {@code compile()}.
         */
        public StateGraph onAny() {
            for (String target : targets) {
                requireTrigger(target, Trigger.any());
            }
            return StateGraph.this;
        }

        /**
         * The targets fire once {@code n} of their inbound edges have carried.
         *
         * <p>A quorum. Three price feeds and {@code atLeast(2)} proceeds on two agreeing
         * without waiting for a third that may be down.
         *
         * <p>{@code atLeast(1)} is {@link #onAny()}; a number above the node's inbound edge
         * count is refused by {@code compile()}, because a quorum that can never be reached
         * is a graph that can never finish.
         */
        public StateGraph atLeast(int n) {
            for (String target : targets) {
                requireTrigger(target, Trigger.atLeast(n));
            }
            return StateGraph.this;
        }

    }

    /** The switch/case form of a conditional. */
    public class Switch extends Chain {

        private final ConditionalSpec spec;

        private Switch(ConditionalSpec spec) {
            this.spec = spec;
        }

        /** One branch. The same key twice is a build error rather than a silent overwrite. */
        @SafeVarargs
        public final Switch caseOf(String key, Class<? extends GraphNode>... targets) {
            List<String> names = new ArrayList<>(targets.length);
            for (Class<? extends GraphNode> type : targets) {
                names.add(register(simpleNameOf(type), type));
            }
            spec.addCase(key, names);
            return this;
        }

        /**
         * Where to go when no case matched.
         *
         * <p>Optional. Without it, a router returning an unknown key leaves <b>every</b>
         * branch skipped, which is legitimate: "none of the above, carry on" is a real
         * workflow. It is called out here because the alternative reading, "unknown key is an
         * error", is just as reasonable and this engine does not take it.
         */
        @SafeVarargs
        public final StateGraph orElse(Class<? extends GraphNode>... targets) {
            List<String> names = new ArrayList<>(targets.length);
            for (Class<? extends GraphNode> type : targets) {
                names.add(register(simpleNameOf(type), type));
            }
            spec.setDefault(names);
            return StateGraph.this;
        }

        /** The same, by node name. For a graph built at run time, or read back from a store. */
        public Switch caseOf(String key, String... targets) {
            spec.addCase(key, Arrays.asList(targets));
            return this;
        }

        /** The same, by node name. */
        public StateGraph orElse(String... targets) {
            spec.setDefault(Arrays.asList(targets));
            return StateGraph.this;
        }

    }

    /** The predicate form of a conditional. */
    public class When extends Chain {

        private final PredicateSpec spec;
        private final Predicate<GraphState> pending;

        private When(PredicateSpec spec, Predicate<GraphState> pending) {
            this.spec = spec;
            this.pending = pending;
        }

        /** The branch this predicate selects. */
        @SafeVarargs
        public final When to(Class<? extends GraphNode>... targets) {
            List<String> names = new ArrayList<>(targets.length);
            for (Class<? extends GraphNode> type : targets) {
                names.add(register(simpleNameOf(type), type));
            }
            spec.add(pending, names);
            return new When(spec, null);
        }

        /** Another predicate, tried only when the ones before it were false. */
        public When when(Predicate<GraphState> predicate) {
            // A null placeholder keeps the expression list aligned with the branch indices, so
            // that one lambda among several SpEL conditions makes the whole conditional
            // unloadable rather than silently shifting the others
            spec.conditional().addPredicateExpression(null);
            return new When(spec, predicate);
        }

        /** Another condition, as a Spring expression. Tried only after the ones before it. */
        public When when(String spel) {
            spec.conditional().addPredicateExpression(spel);
            return new When(spec, Expressions.predicate(spel));
        }

        /** Where to go when no predicate was true. Optional; see {@link Switch#orElse}. */
        @SafeVarargs
        public final StateGraph otherwise(Class<? extends GraphNode>... targets) {
            List<String> names = new ArrayList<>(targets.length);
            for (Class<? extends GraphNode> type : targets) {
                names.add(register(simpleNameOf(type), type));
            }
            spec.conditional().setDefault(names);
            return StateGraph.this;
        }

        /** The same, by node name. For a graph built at run time, or read back from a store. */
        public When to(String... targets) {
            spec.add(pending, Arrays.asList(targets));
            return new When(spec, null);
        }

        /** The same, by node name. */
        public StateGraph otherwise(String... targets) {
            spec.conditional().setDefault(Arrays.asList(targets));
            return StateGraph.this;
        }

    }

    // ------------------------------------------------------------------

    private String register(String nodeName, Class<? extends GraphNode> type) {
        return register(nodeName, type, null, null);
    }

    /**
     * Records a node, and refuses a second one that disagrees with the first.
     *
     * <p>A name is the node's identity, so letting a later declaration quietly win would mean
     * the graph that runs is not the graph that was drawn. Mentioning the same node again
     * with the same implementation is ordinary, though: every edge does it.
     */
    private String register(String nodeName, Class<? extends GraphNode> type, String beanName,
                            Map<String, Object> config) {
        NodeSpec existing = nodes.get(nodeName);
        if (existing != null) {
            if (type != null && existing.type() != null && !existing.type().equals(type)) {
                throw new DagException("node " + nodeName + " is already registered as "
                        + existing.type().getName() + " and cannot also be " + type.getName()
                        + ". Give one of them a name of its own with node(name, class)");
            }
            if (beanName != null && existing.beanName() != null
                    && !existing.beanName().equals(beanName)) {
                throw new DagException("node " + nodeName + " is already registered against "
                        + "bean \"" + existing.beanName() + "\" and cannot also be \""
                        + beanName + "\". Give one of them a name of its own");
            }
            // A later mention may fill in what the first left out, which is what makes
            // "declare the node, then draw the edges" work in either order
            nodes.put(nodeName, new NodeSpec(nodeName,
                    type != null ? type : existing.type(),
                    beanName != null ? beanName : existing.beanName(),
                    config != null && !config.isEmpty() ? frozen(config) : existing.config()));
            return nodeName;
        }
        nodes.put(nodeName, new NodeSpec(nodeName, type, beanName, frozen(config)));
        return nodeName;
    }

    /**
     * An unmodifiable copy of a node's settings that keeps their order.
     *
     * <p>Not {@code Map.copyOf}, whose iteration order is unspecified: these are written out
     * with the definition, and an order that changes by itself makes every stored graph look
     * edited when it is not.
     */
    private static Map<String, Object> frozen(Map<String, Object> config) {
        return config == null || config.isEmpty() ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(config));
    }

    /**
     * Fixes a node's trigger, and refuses a second, different one.
     *
     * <p>The alternative would be letting declaration order decide, which makes a graph's
     * behaviour depend on the order its edges happen to be written in. That is precisely the
     * kind of thing nobody would look for while debugging.
     */
    private void requireTrigger(String nodeName, Trigger trigger) {
        Trigger already = declaredTriggers.putIfAbsent(nodeName, trigger);
        if (already != null && already != trigger) {
            throw new DagException("node " + nodeName + " is declared with trigger " + already
                    + " in one place and " + trigger + " in another. A trigger belongs to the "
                    + "node, so the two declarations have to agree");
        }
    }

    private static String simpleNameOf(Class<? extends GraphNode> type) {
        if (type == null) {
            throw new IllegalArgumentException("a node class must not be null");
        }
        return type.getSimpleName();
    }

    // ------------------------------------------------------------------
    // Internal specifications, handed to CompiledGraph
    // ------------------------------------------------------------------

    /**
     * A node as declared.
     *
     * @param name     its name in this graph, and the key everything else refers to it by
     * @param type     the class that does the work, or null when the node was declared by
     *                 bean name alone, which is what a graph built from data does
     * @param beanName the bean to dispatch to, or null to dispatch by class. Declaring it is
     *                 what lets <b>one class have several beans</b>, since a class name can
     *                 no longer tell them apart
     * @param config   this node's own settings, handed to it as {@link NodeContext}. Empty
     *                 for a node that needs none
     */
    record NodeSpec(String name, Class<? extends GraphNode> type, String beanName,
                    Map<String, Object> config) {

        NodeSpec(String name, Class<? extends GraphNode> type) {
            this(name, type, null, Map.of());
        }

        /** What the dispatcher is asked for: the bean name when there is one, else the class. */
        String target() {
            return beanName != null ? beanName : (type == null ? name : type.getName());
        }
    }

    /**
     * A conditional edge.
     *
     * <p>Both DSL forms, {@code switchOn} and {@code when}, end up here, so the engine and
     * the renderers have one thing to understand rather than two.
     */
    public static class ConditionalSpec {

        private final List<String> sources;
        private final Function<GraphState, String> router;
        private final Map<String, List<String>> branches = new LinkedHashMap<>();
        private List<String> defaultTargets = List.of();

        /**
         * The SpEL this was built from, or null when it was built from a lambda.
         *
         * <p>Kept so that a graph can be written out and read back. A lambda is code and has
         * no text to store; an expression is text already. See {@link GraphCatalog}.
         */
        private String expression;

        /** For the predicate form: each {@code when(...)}'s expression, in order. */
        private final List<String> predicateExpressions = new ArrayList<>();

        ConditionalSpec(List<String> sources, Function<GraphState, String> router) {
            this.sources = List.copyOf(sources);
            this.router = router;
        }

        void setExpression(String expression) {
            this.expression = expression;
        }

        /** The router's SpEL, or null when it came from a lambda and cannot be written out. */
        public String expression() {
            return expression;
        }

        void addPredicateExpression(String expression) {
            predicateExpressions.add(expression);
        }

        /**
         * Fills in the placeholder the lambda path has just added.
         *
         * <p>{@code when(String)} goes through {@code when(Predicate)}, which cannot know that
         * its predicate came from text. Without this, the first condition of every SpEL
         * {@code when} chain would be stored as null and the whole conditional would read back
         * as unloadable.
         */
        void replaceLastPredicateExpression(String expression) {
            predicateExpressions.set(predicateExpressions.size() - 1, expression);
        }

        /** The predicate form's expressions, in order. Empty for the switch form. */
        public List<String> predicateExpressions() {
            return predicateExpressions;
        }

        /** Whether this conditional can be written out and read back. */
        public boolean isLoadable() {
            if (!predicateExpressions.isEmpty()) {
                return predicateExpressions.stream().noneMatch(Objects::isNull)
                        && predicateExpressions.size() == branches.size();
            }
            return expression != null;
        }

        void addCase(String key, List<String> targets) {
            if (branches.putIfAbsent(key, List.copyOf(targets)) != null) {
                throw new DagException("conditional from " + sources + " declares the branch \""
                        + key + "\" twice");
            }
        }

        void setDefault(List<String> targets) {
            this.defaultTargets = List.copyOf(targets);
        }

        public List<String> sources() {
            return sources;
        }

        public Function<GraphState, String> router() {
            return router;
        }

        public Map<String, List<String>> branches() {
            return branches;
        }

        public List<String> defaultTargets() {
            return defaultTargets;
        }

        /** Every node this conditional can reach, chosen or not. */
        public Set<String> allTargets() {
            Set<String> all = new LinkedHashSet<>();
            branches.values().forEach(all::addAll);
            all.addAll(defaultTargets);
            return all;
        }
    }

    /**
     * The predicate form, expressed as an ordinary {@link ConditionalSpec}.
     *
     * <p>Predicates become branch keys {@code "0"}, {@code "1"} and so on, and the router
     * returns the index of the first true one. So there is exactly one conditional mechanism
     * in the engine, and the second DSL form costs nothing but this small adapter.
     */
    static class PredicateSpec {

        private final List<Predicate<GraphState>> predicates = new ArrayList<>();
        private final ConditionalSpec conditional;

        PredicateSpec(List<String> sources) {
            this.conditional = new ConditionalSpec(sources, this::route);
        }

        private String route(GraphState state) {
            for (int i = 0; i < predicates.size(); i++) {
                if (predicates.get(i).test(state)) {
                    return String.valueOf(i);
                }
            }
            // No match. An unknown key means "take the default, or skip every branch",
            // which is exactly the behaviour wanted here
            return "";
        }

        void add(Predicate<GraphState> predicate, List<String> targets) {
            conditional.addCase(String.valueOf(predicates.size()), targets);
            predicates.add(predicate);
        }

        ConditionalSpec conditional() {
            return conditional;
        }
    }

    static Collection<String> namesOf(Collection<NodeSpec> specs) {
        List<String> out = new ArrayList<>(specs.size());
        specs.forEach(s -> out.add(s.name()));
        return out;
    }
}
