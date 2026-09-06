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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Spreads one aggregation across the cluster: shards go out, results come back.
 *
 * <pre>{@code
 * Map<String, Integer> counts = mapReduce
 *         .<String, String, Integer, Integer>submit("wordCount", text)
 *         .get(60, TimeUnit.SECONDS);
 * }</pre>
 *
 * <p>What to compute is written in {@link MapReduceJob}; this interface only runs it.
 *
 * <h2>What one job does, start to finish</h2>
 * <pre>
 * 1. the submitter splits into N shards and sends shard i to node i, itself included
 * 2. each node maps, and sends the intermediate pairs straight to the node each key hashes to
 * 3. once a node has all N "that is all" messages, it reduces its own portion of the keys
 * 4. the partial results return to the submitter and are assembled into one whole
 * </pre>
 *
 * <p><b>Intermediate results never pass through the submitter</b> -- mappers send directly to
 * reducers. The submitter appears at the start and the end only, and the large volume in
 * between is point-to-point between nodes.
 *
 * <h2>Why the final step is merely assembly</h2>
 * Because one key is only ever reduced on one node, each node's results have <b>disjoint key
 * sets</b>, so merging is a {@code putAll} with not a single value left to compute.
 *
 * <p><b>Should the assembly actually produce a duplicate key, the partitioning is
 * broken</b> -- meaning one key was reduced half on each of two nodes, and both halves are
 * incomplete. That throws a {@link MapReduceException} rather than silently overwriting: an
 * exception is far better than a wrong answer.
 *
 * <h2>It is not Spark</h2>
 * Shards are serialised and sent as cluster messages, so <b>the whole job's data volume is
 * bounded by the message size limit</b>. What it is good at is moderate data that is slow to
 * compute -- tens of thousands of records each needing a run through a rules engine, minutes
 * on one machine and tens of seconds spread across five nodes.
 *
 * <p>Conversely, where the bottleneck is data volume rather than computation -- a 10GB file
 * -- use a real big-data stack, or shard it yourself and submit in batches.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
public interface ProcessingMapReduce {

    /**
     * Submits an aggregation with the default timeout.
     *
     * @param jobBeanName the bean name of the {@link MapReduceJob} implementation.
     *                    <b>Every node in the cluster must have that bean, under that name,
     *                    with the same implementation</b>
     * @param input       the input to process; {@link MapReduceJob#split} cuts it up
     * @return the complete aggregated result. A failure anywhere arrives through the future's
     *         exceptional path, wrapped in a {@link MapReduceException}
     */
    <IN, K, V, R> CompletableFuture<MapReduceResult<K, R>> submit(String jobBeanName, IN input);

    /**
     * Submits an aggregation with a given timeout.
     *
     * <p>The timeout covers <b>the whole job</b>: from the shards going out to the last
     * partial result coming back. Reaching it without a complete set completes exceptionally,
     * with how many were received and who is still missing written into the message -- "it
     * timed out" on its own helps nobody diagnose anything.
     *
     * <p>After a timeout, nodes are <b>not</b> told to stop. They finish the shard in hand,
     * send the result back to a job nobody is waiting on, and it is discarded. That is
     * deliberate: interrupting a distributed computation halfway is far harder than letting
     * it finish, and the benefit is a few seconds of CPU.
     */
    <IN, K, V, R> CompletableFuture<MapReduceResult<K, R>> submit(String jobBeanName, IN input,
                                                                  long timeout, TimeUnit unit);

    /** How many jobs are running with this node as the submitter. */
    int runningJobs();
}
