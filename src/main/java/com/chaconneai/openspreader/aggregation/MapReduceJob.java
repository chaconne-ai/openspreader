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
package com.chaconneai.openspreader.aggregation;

import com.chaconneai.spreader.loadbalance.LoadBalancer;

import java.util.List;

/**
 * Everything one distributed aggregation needs: how to divide, how to compute, how to
 * combine.
 *
 * <p>Implement it, register it as a Spring bean, and submit it with
 * {@link ProcessingMapReduce#submit}.
 *
 * <pre>{@code
 * @Component("wordCount")
 * public class WordCountJob implements MapReduceJob<String, String, Integer, Integer> {
 *
 *     public List<String> split(String text, int suggestedShards) {
 *         return splitByLines(text, suggestedShards);
 *     }
 *
 *     public void map(String shard, Emitter<String, Integer> emitter) {
 *         for (String word : shard.split("\\W+")) {
 *             if (!word.isBlank()) {
 *                 emitter.emit(word.toLowerCase(), 1);
 *             }
 *         }
 *     }
 *
 *     public Integer reduce(String word, List<Integer> counts) {
 *         return counts.stream().mapToInt(Integer::intValue).sum();
 *     }
 * }
 * }</pre>
 *
 * <h2>Where each of the three phases runs</h2>
 * <table border="1">
 *   <caption>Where execution happens</caption>
 *   <tr><th>Method</th><th>Which node runs it</th><th>How many times</th></tr>
 *   <tr><td>{@link #split}</td><td><b>The submitter only</b></td><td>Once</td></tr>
 *   <tr><td>{@link #map}</td><td>Every node, <b>the submitter among them</b></td>
 *       <td>Once per shard</td></tr>
 *   <tr><td>{@link #reduce}</td><td>Decided by the key's hash</td>
 *       <td>Once per distinct key</td></tr>
 * </table>
 *
 * <p>The last step, combining the nodes' partial results into one, <b>is not yours to
 * write</b> -- one key is reduced on exactly one node, so combining is joining a few maps
 * together, with no value left to compute.
 *
 * <h2>Every node in the cluster needs this bean, at the same version</h2>
 * The same constraint as {@code ProcessingPool}'s. Half the nodes on a new {@code map} and
 * half on the old one produce <b>a wrong answer rather than an error</b> -- the hardest kind
 * to track down. Do not submit jobs during a rolling deployment.
 *
 * <h2>{@code @MultiProcessingCall} is not needed</h2>
 * {@code map} and {@code reduce} really are called remotely by other nodes, but the allow-list
 * here <b>comes from the type itself</b>: only an object implementing this interface and
 * registered as a bean receives tasks, and <b>only the interface's two methods</b> can be
 * called -- no reflection, no lookup by method name, and no path anywhere in the cluster that
 * reaches another method through it.
 *
 * <p>Adding the annotation is in fact <b>harmful</b>: it would register these two methods in
 * {@code ProcessingPool}'s allow-list, opening an extra
 * {@code pool.submit(beanName, "map", ...)} call path -- and {@link Emitter} is not
 * serialisable at all, so that path could only blow up at runtime.
 *
 * @param <IN> the input type, which is also the shard type. Must be serialisable
 * @param <K>  the intermediate key, which is also the final result's key. Must be serialisable
 *             and <b>hash stably</b>
 * @param <V>  the intermediate value. Must be serialisable
 * @param <R>  the reduced result. Must be serialisable
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
public interface MapReduceJob<IN, K, V, R> {

    /**
     * Divides the input into shards, each of which is sent to a node for {@link #map}.
     *
     * <p><b>How many shards is yours to decide</b>; {@code suggestedShards} is only a hint, the
     * number of nodes currently available. More shards than nodes is fine -- a node handles
     * several in succession, and with {@link LoadBalancer#weighted()} that is exactly how a
     * stronger machine takes more of them.
     *
     * <p>Dividing into 0 shards fails the job outright: with no mapper there is no intermediate
     * output, and so nothing to tell a reducer how many records to expect.
     *
     * <h2>A job's data volume is bounded by the message size</h2>
     * Every shard is serialised and sent as a cluster message. <b>This is not Spark</b> -- do
     * not expect one job to swallow a 10GB file. Divide a large dataset yourself and submit it
     * in batches.
     *
     * @param suggestedShards the number of nodes currently available; a hint only
     */
    List<IN> split(IN input, int suggestedShards);

    /**
     * Handles one shard, emitting its intermediate output.
     *
     * <p>Each record produced goes to a node by the hash of its {@code key} to await reduction.
     * <b>Every record for one key lands on one node</b>, which {@link #shuffleBalancer()}
     * guarantees through consistent hashing, and on which the whole model rests.
     *
     * <p>Throwing fails the whole job -- the first version does not retry shards, because
     * retrying would require map to be idempotent, and <b>part of this shard's intermediate
     * output has already been sent</b>, so a rerun would double-count.
     */
    void map(IN shard, Emitter<K, V> emitter);

    /**
     * Reduces every intermediate value for one key into a single result.
     *
     * <p>It runs on <b>the node that holds the key</b>, and receives <b>every</b> intermediate
     * value for that key from across the cluster.
     *
     * <p>Precisely because it is every one of them, there is no need to think about "how to
     * combine with another node's partial result" -- with correct partitioning that question
     * does not arise. Finding that a second combining function is needed means the partitioning
     * has gone wrong.
     *
     * @param values every intermediate value for the key, in no particular order
     */
    R reduce(K key, List<V> values);

    /**
     * Whether intermediate output may be <b>reduced locally on the mapper</b> before it is
     * sent.
     *
     * <p>This is Hadoop's combiner. {@code emit(word, 1)} produces tens of thousands of
     * {@code 1}s for "the", each serialised, sent over the network, and added to a list on the
     * reducer. Switched on, one key leaves a shard as <b>a single record</b> -- the traffic
     * falls in inverse proportion to how repetitive the keys are, and word counting drops by an
     * order of magnitude or two.
     *
     * <h2>Two things must be confirmed before switching it on</h2>
     * <ol>
     *   <li><b>{@link #reduce} is associative and commutative.</b> Once on, reduce is called
     *       <b>twice</b>: over the local data on each mapper, and again over the mappers'
     *       results on the reducer. Summing, taking a maximum and counting are all fine;
     *       <b>taking a mean is not</b> -- the mean of means is not the mean -- and neither is
     *       "take the first" or "collect them all".</li>
     *   <li><b>{@code R} can serve as a {@code V}</b>, and usually the two are the same type,
     *       because the local reduction's result travels onward as an intermediate value.</li>
     * </ol>
     *
     * <p>Where the answer is not clear, <b>leave it off</b>. Off by default, the cost is only
     * extra network traffic; switched on wrongly, the result is wrong -- and, as with the other
     * traps here, <b>nothing reports it</b>.
     *
     * <p>{@code MatrixMultiplyJob} is the counter-example: its reduce requires "exactly one
     * value", and reducing locally first destroys that premise.
     */
    default boolean combinable() {
        return false;
    }

    /**
     * Which strategy the shuffle uses: one key must <b>land stably on one node</b>.
     *
     * <h2>The only place in a job that needs load balancing</h2>
     * The sharding phase has <b>no</b> corresponding method, and that is deliberate: sending
     * shard i to node i, {@code shardIndex % nodeCount} is enough -- deterministic, stateless,
     * and identical however many times it runs. Round-robin for the same job would only add a
     * piece of instance state and a knob that can be set wrongly.
     *
     * <p>To have a stronger machine do more, the right knob is {@link #split}: the size and
     * number of shards are already yours, which is far more direct than choosing nodes
     * afterwards.
     *
     * <h2>Under normal circumstances, do not override this</h2>
     * The whole model's correctness rests on "one key is reduced once". Replaced with
     * round-robin or random selection, one key scatters and each place reduces a half-finished
     * result -- and the framework <b>detects this and fails the job</b>; see the key collision
     * described in {@link ProcessingMapReduce#submit}.
     *
     * <p>There are two legitimate reasons to override it: raising the virtual node count
     * ({@code new ConsistentHashLoadBalancer(320)}, for a more even distribution with a great
     * many nodes), or having a <b>deterministic</b> strategy better suited to your own key
     * distribution than consistent hashing. Determinism is a hard requirement -- the same key
     * with the same candidate list must yield the same target every time, on every node.
     *
     * <h2>A guarantee from the framework: it is called once per job</h2>
     * The thousands of records emitted from one shard share a single instance, so creating one
     * here with {@code new} is safe. The guarantee matters most to consistent hashing, which
     * caches the virtual node ring internally and would otherwise recompute
     * {@value com.chaconneai.spreader.loadbalance.ConsistentHashLoadBalancer#DEFAULT_VIRTUAL_NODES}
     * times the node count MD5 hashes on every construction.
     *
     * <p><b>There is one instance per node</b>: mappers live in different JVMs and each builds
     * its own ring. That does not affect correctness -- consistent hashing's result depends
     * only on the key and the candidate list, and the candidate list is fixed by the submitter
     * and travels with the task, identical across the cluster.
     */
    default LoadBalancer shuffleBalancer() {
        return LoadBalancer.consistentHash();
    }
}
