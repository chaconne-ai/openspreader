package com.chaconneai.openspreader.example.aggregation;

import com.chaconneai.openspreader.aggregation.Emitter;
import com.chaconneai.openspreader.aggregation.MapReduceException;
import com.chaconneai.openspreader.aggregation.MapReduceJob;
import com.chaconneai.openspreader.aggregation.ProcessingMapReduce;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * How to use distributed aggregation.
 *
 * <h2>Three steps to adopt it</h2>
 * <ol>
 *   <li>Turn the switch on:
 *       {@code spring.spreader.multiprocessing.aggregation.enabled = true}</li>
 *   <li>Write a {@link MapReduceJob} implementation and register it as a bean</li>
 *   <li>Inject {@link ProcessingMapReduce} and call {@code submit(beanName, input)}</li>
 * </ol>
 *
 * <pre>{@code
 * @Component("wordCount")
 * public class WordCountJob implements MapReduceJob<String, String, Integer, Integer> {
 *     public List<String> split(String text, int shards) { ... }
 *     public void map(String shard, Emitter<String, Integer> out) { ... }
 *     public Integer reduce(String word, List<Integer> counts) { ... }
 * }
 *
 * @Autowired ProcessingMapReduce mapReduce;
 *
 * Map<String, Integer> counts = mapReduce
 *         .<String, String, Integer, Integer>submit("wordCount", text)
 *         .get(60, TimeUnit.SECONDS);
 * }</pre>
 *
 * <h2>Three examples, three shapes</h2>
 * What differs between them is not the domain but <b>which phase carries the work</b>:
 *
 * <table border="1">
 *   <caption>Which to use as a template</caption>
 *   <tr><th>Example</th><th>Input size</th><th>Weight in</th><th>What it shows</th></tr>
 *   <tr><td>{@link WordCountJob}</td><td>Large</td><td>reduce</td>
 *       <td>The classic shape; and <b>why it cannot be used as acceptance</b></td></tr>
 *   <tr><td>{@link MonteCarloPiJob}</td><td><b>One number</b></td><td>map</td>
 *       <td>The shape this <b>really suits</b>: little data, unbounded appetite for
 *           computation</td></tr>
 *   <tr><td>{@link MatrixMultiplyJob}</td><td>Large</td><td>map</td>
 *       <td>reduce as mere collection; and where a GPU would connect</td></tr>
 * </table>
 *
 * <h2>What it suits, and what it does not</h2>
 * Shards are serialised and travel as cluster messages, so <b>data volume has a ceiling and
 * computation does not</b>.
 *
 * <ul>
 *   <li><b>Suits</b>: tens of thousands of records, each needing a rules engine, a model
 *       inference or a complex validation. Ten minutes on one machine, two on five nodes.</li>
 *   <li><b>Suits</b>: a tiny input with enormous computation -- Monte Carlo, parameter sweeps,
 *       brute-force search.</li>
 *   <li><b>Does not suit</b>: a 10GB file. That is a data-movement problem and calls for a real
 *       big-data stack.</li>
 *   <li><b>Does not suit</b>: online requests measured in milliseconds. One job costs at least
 *       two network round trips.</li>
 * </ul>
 *
 * <h2>Four things easy to get wrong</h2>
 *
 * <h3>1. Keys must hash stably</h3>
 * The key decides which node an intermediate record is reduced on, and that decision is made
 * <b>independently in different JVMs</b>. Use a {@code String}, a number, or a value object
 * with a correct {@code hashCode}.
 *
 * <p><b>Never use an array as a key</b>: an array's {@code hashCode} is its address, so two
 * arrays with the same contents hash differently on two nodes and one key lands on both. The
 * mistake <b>reports nothing</b> -- unless it happens to trip the framework's key-collision
 * check.
 *
 * <h3>2. Every node in the cluster needs the same job bean, by name and by implementation</h3>
 * Half the nodes on a new {@code map} and half on the old one produce <b>a wrong answer rather
 * than an error</b>. Do not submit jobs during a rolling deployment.
 *
 * <h3>3. Do not touch shared mutable state inside a job</h3>
 * {@code map} and {@code reduce} are <b>called concurrently by the inbound thread pool</b>,
 * and run in <b>several JVMs</b> across the cluster. Writing to a job's fields shows no
 * problem in a single-machine test and becomes "everyone counting their own" the moment it
 * reaches a cluster.
 *
 * <p>Parameters belong in the {@code input}, travelling with the shards.
 *
 * <h3>4. A failure fails the whole job</h3>
 * The first version <b>does not retry shards</b>: retrying would require map to be idempotent,
 * and part of the failed shard's intermediate output may already have been sent, so a rerun
 * would double-count. <b>A wrong answer is far worse than an exception.</b> To retry, submit
 * again from the caller.
 *
 * <h2>How to verify that partitioning is correct</h2>
 * This deserves saying on its own, because <b>the most natural verification does not
 * work</b>.
 *
 * <p>Word counting reduces by summing, which is associative -- even if one word were reduced
 * half on each of two nodes, adding at the end <b>still gives the right answer</b>. So "the
 * word count came out right" proves nothing at all.
 *
 * <p>Two things do work:
 * <ul>
 *   <li>Relying on the framework's <b>key-collision check</b> during the final merge: normally
 *       the nodes' key sets are disjoint, and a duplicate throws
 *       {@link MapReduceException}</li>
 *   <li>Trying a reduce that is <b>not associative</b>, such as "return the first value
 *       received" -- one mistake in the partitioning and the result is wrong at once</li>
 * </ul>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
public class MapReduceBestPractice {

    /**
     * The common usage: submit and wait for the result.
     *
     * <p>Always give a timeout. When a remote node dies, a {@code get()} without one hangs
     * indefinitely -- the service has timeouts of its own, but they are not guaranteed under a
     * network partition.
     */
    public static Map<String, Integer> countWords(ProcessingMapReduce mapReduce, String text)
            throws Exception {
        return mapReduce.<String, String, Integer, Integer>submit("wordCount", text)
                .get(60, TimeUnit.SECONDS);
    }

    /** The non-blocking usage. */
    public static void countWordsAsync(ProcessingMapReduce mapReduce, String text) {
        mapReduce.<String, String, Integer, Integer>submit("wordCount", text)
                .thenAccept(counts -> System.out.println(counts.size() + " distinct word(s)"))
                .exceptionally(e -> {
                    // Every cause is wrapped in a MapReduceException whose message says which
                    // node is missing
                    System.err.println("Aggregation failed: " + e.getMessage());
                    return null;
                });
    }

    /**
     * A <b>counter-example</b>: written this way it runs perfectly on one machine and is wrong
     * the moment it reaches a cluster.
     *
     * <p>{@code counter} is a field of the job instance. Each node has its own instance
     * counting its own total, with no idea what anyone else has counted -- and several threads
     * on the same node change it at once besides.
     *
     * <p>There is exactly one way to accumulate across nodes: <b>emit it, and let reduce
     * combine</b>.
     */
    @SuppressWarnings("unused")
    private static final class BrokenJob implements MapReduceJob<String, String, Integer, Integer> {

        /** Wrong: there is no shared state across JVMs, and nothing synchronises it across
         *  threads either */
        private int counter;

        @Override
        public List<String> split(String input, int suggestedShards) {
            return List.of(input);
        }

        @Override
        public void map(String shard, Emitter<String, Integer> emitter) {
            counter++;                      // this number means nothing in the end
            emitter.emit("count", counter); // and every node starts from its own 0
        }

        @Override
        public Integer reduce(String key, List<Integer> values) {
            return values.size();
        }
    }
}
