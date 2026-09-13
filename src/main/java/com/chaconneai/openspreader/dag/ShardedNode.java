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

import com.chaconneai.openspreader.aggregation.MapReduceJob;
import com.chaconneai.openspreader.aggregation.MapReduceResult;
import com.chaconneai.openspreader.aggregation.ProcessingMapReduce;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A node whose work is split at run time and spread across the cluster.
 *
 * <p>The dynamic fan-out: a step that does not know until it runs how many pieces of work
 * there are. A thousand orders to re-price, a list of files to parse, a batch of records each
 * needing a pass through a rules engine.
 *
 * <pre>{@code
 * @Component
 * public class RepriceAll extends ShardedNode {
 *
 *     public RepriceAll(ProcessingMapReduce mapReduce) {
 *         super(mapReduce);
 *     }
 *
 *     protected String jobBeanName()   { return "repriceJob"; }   // a MapReduceJob bean
 *     protected String inputChannel()  { return "orderIds"; }     // read the collection here
 *     protected String outputChannel() { return "prices"; }       // write the result here
 * }
 * }</pre>
 *
 * <h2>It builds nothing: the sharding is openspreader's</h2>
 * Everything hard about this is already solved by {@link ProcessingMapReduce}, and reaching
 * for it rather than writing a second scatter-gather is the whole point:
 *
 * <table border="1">
 *   <caption>What the aggregation component supplies</caption>
 *   <tr><th>Needed</th><th>Supplied by</th></tr>
 *   <tr><td>Splitting the work into pieces</td>
 *       <td>{@link MapReduceJob#split}, with the node count as a hint</td></tr>
 *   <tr><td>A piece per node, in parallel</td><td>{@code map}, on every node at once</td></tr>
 *   <tr><td>Gathering the pieces back into one</td><td>{@code reduce}, then assembly</td></tr>
 *   <tr><td>A ceiling on the whole thing</td><td>the job timeout</td></tr>
 * </table>
 *
 * <p>Two of those are worth dwelling on, because writing this by hand would get them wrong.
 *
 * <p><b>Pieces are not items.</b> {@code split} decides how many shards, which is why ten
 * thousand records do not become ten thousand dispatches. Fanning out with one call per item
 * would have meant writing that batching again.
 *
 * <p><b>An empty input is not a failure.</b> Nothing to shard writes an empty result and the
 * graph carries on. Left to the job, an empty collection would make {@code split} return zero
 * shards, which the aggregation component refuses outright, so "no orders today" would fail
 * the whole run.
 *
 * <p><b>A full queue rejects rather than discards.</b> The aggregation component chose that
 * policy deliberately, because losing one intermediate result makes an aggregate <b>silently
 * wrong</b>. A gather wants exactly that: fail loudly rather than return a short answer.
 *
 * <h2>The graph stays static, and that is deliberate</h2>
 * However many shards there turn out to be, this remains <b>one node</b>: one box in the
 * picture, one status in the {@link RunResult}, one entry in the ancestor table. The
 * alternative, letting a node expand into N nodes at run time, would break the two
 * assumptions that make {@link CompiledGraph} cheap and checkable: that the node set is fixed
 * and that reachability can be computed once.
 *
 * <p>The cost is honest: the picture cannot show that this step actually ran forty-seven
 * ways. {@link MapReduceResult#stats()} can.
 *
 * <h2>The per-shard work is a job, not a node</h2>
 * What runs per shard is {@code map} on a {@link MapReduceJob} bean, not a {@link GraphNode}.
 * So a shard cannot have conditional edges of its own or its own fan-in.
 *
 * <p>It <b>can</b> be a whole graph, though, and that is the way round this: a
 * {@code CompiledGraph} is built in a bean and exists on every replica, so {@code map} is
 * free to call {@code invoke} on one. Per-shard work with internal dependencies is therefore
 * expressible, it simply lives in the job rather than in the outer graph.
 *
 * <h2>Switch the aggregation component on</h2>
 * {@code spring.spreader.multiprocessing.aggregation.enabled=true}, on every replica, as with
 * the engine itself. Without it there is no {@link ProcessingMapReduce} bean to inject and
 * the application will not start, which is the right moment to find out.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 13/09/2026
 */
public abstract class ShardedNode extends GraphNode {

    private final ProcessingMapReduce mapReduce;

    protected ShardedNode(ProcessingMapReduce mapReduce) {
        if (mapReduce == null) {
            throw new IllegalArgumentException("a ShardedNode needs a ProcessingMapReduce. "
                    + "Set spring.spreader.multiprocessing.aggregation.enabled=true");
        }
        this.mapReduce = mapReduce;
    }

    /** The {@link MapReduceJob} bean that does the work. Its name, as submitted. */
    protected abstract String jobBeanName();

    /** The channel holding the input to split. Its value is passed to {@code split} as it is. */
    protected abstract String inputChannel();

    /** The channel to write the gathered result to, as a {@code Map} of key to reduced value. */
    protected abstract String outputChannel();

    /**
     * Whether there is nothing to shard.
     *
     * <p>Only the shapes that have a meaningful notion of empty. Anything else is passed to
     * the job, which is the only thing that knows how to divide it.
     */
    private static boolean isEmpty(Object input) {
        if (input instanceof Collection<?> c) {
            return c.isEmpty();
        }
        if (input instanceof Map<?, ?> m) {
            return m.isEmpty();
        }
        if (input.getClass().isArray()) {
            return Array.getLength(input) == 0;
        }
        return false;
    }

    /**
     * How long the whole sharded step may take, in milliseconds.
     *
     * <p>0, the default, leaves it to the aggregation component's own configured job timeout.
     * Override it for a step whose natural duration differs from the rest.
     */
    protected long timeoutMs() {
        return 0L;
    }

    /**
     * Reads the input channel, runs the job, writes the result.
     *
     * <p>Final: what varies between sharded steps is the job and the two channel names, and
     * every one of them is declared above. A subclass with something else to do wants an
     * ordinary {@link GraphNode}.
     */
    @Override
    public final Map<String, Object> execute(GraphState state) throws Exception {
        Object input = state.get(inputChannel());
        if (input == null) {
            throw new DagException("sharded node " + defaultName() + " reads channel \""
                    + inputChannel() + "\", which nothing has written. Check that the node "
                    + "producing it runs before this one");
        }

        if (isEmpty(input)) {
            // Nothing to do is not a failure. Without this, every user would meet the same
            // trap: an empty collection makes split() return 0 shards, and the aggregation
            // component refuses 0 shards outright (correctly, since a reducer would then
            // never be told that everything had arrived). "No orders to reprice today" would
            // fail the whole graph
            return Map.of(outputChannel(), Map.of());
        }

        long timeout = timeoutMs();
        try {
            MapReduceResult<?, ?> result = timeout > 0
                    ? mapReduce.submit(jobBeanName(), input).get(timeout, TimeUnit.MILLISECONDS)
                    : mapReduce.submit(jobBeanName(), input).get();
            return Map.of(outputChannel(), result.data());
        } catch (ExecutionException e) {
            // The job's own failure, unwrapped. Wrapping it would put a framework type between
            // the caller and what actually went wrong, and RunResult already carries the node
            // name that this happened in
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            throw new DagException("sharded node " + defaultName() + " failed", cause);
        } catch (TimeoutException e) {
            throw new DagException("sharded node " + defaultName() + " did not finish within "
                    + timeout + "ms. The ceiling is the whole job, from the shards going out "
                    + "to the last result coming back", e);
        }
    }

}
