package com.chaconneai.openspreader.example.aggregation;

import com.chaconneai.openspreader.aggregation.Emitter;
import com.chaconneai.openspreader.aggregation.MapReduceJob;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Word counting -- MapReduce's "Hello World".
 *
 * <pre>{@code
 * Map<String, Integer> counts = mapReduce
 *         .<String, String, Integer, Integer>submit("wordCount", text)
 *         .get(60, TimeUnit.SECONDS);
 * }</pre>
 *
 * <h2>The data flow</h2>
 * <pre>
 * text --split--> ["part 1", "part 2", "part 3"]        cut into N shards
 *                      |
 *        map, per node |  emit("the",1) emit("cat",1) ...
 *                      |
 *   gathered by word   |  every 1 for "the" goes to one node
 *                      |
 *     reduce, per node |  summed
 *                      |
 *                      ↓
 *              {the=1053, cat=17, ...}
 * </pre>
 *
 * <h2>Careful using this to verify anything: it is far too forgiving</h2>
 * Word counting reduces by <b>summing</b>, which is associative and commutative. That means
 * <b>even with the partitioning broken</b> -- one word reduced half on each of two nodes --
 * adding the two halves at the end still gives <b>the right answer</b>.
 *
 * <p>So "word count came out correct" <b>proves nothing about the partitioning</b>. Real
 * verification means the key-collision check in {@code MapReduceService}, or trying a
 * reduce that is not associative -- "take the first value", say.
 *
 * <p>Put differently: this example is for <b>teaching</b>, not for <b>acceptance</b>.
 *
 * <h2>Why map emits 1 rather than a local subtotal</h2>
 * A bare {@code emit(word, 1)} makes a frequent word like "the" produce tens of thousands of
 * intermediate records, every one of them serialised and sent. Subtotalling first in a local
 * {@code Map<String,Integer>} and emitting one record per word cuts the network volume by an
 * order of magnitude or two -- in Hadoop this is called a combiner.
 *
 * <p>It is <b>deliberately not done here</b>: the example's point is to make the shape of
 * map and reduce clear. To handle large texts for real, add a local {@code HashMap} that
 * accumulates inside {@link #map}; no framework support is needed.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
public class WordCountJob implements MapReduceJob<String, String, Integer, Integer> {

    /** Anything not a letter or digit is a separator. Real use should pick a tokeniser to suit the language. */
    private static final Pattern SEPARATOR = Pattern.compile("[^\\p{L}\\p{N}]+");

    /**
     * Shards by line, as evenly as it can.
     *
     * <p><b>By line rather than by character count</b>: splitting mid-text would cut a word
     * in two and count each half as a word that does not exist. A bug like that reports
     * nothing and merely leaves a pile of incomprehensible short words in the result.
     */
    @Override
    public List<String> split(String text, int suggestedShards) {
        if (text == null || text.isBlank()) {
            return List.of();
        }
        String[] lines = text.split("\\R");
        int shards = Math.max(1, Math.min(suggestedShards, lines.length));
        List<StringBuilder> buckets = new ArrayList<>(shards);
        for (int i = 0; i < shards; i++) {
            buckets.add(new StringBuilder());
        }
        // Dealt round-robin into the buckets: consecutive lines tend to be of similar length,
        // and cutting them in sequence easily yields one large shard and one small
        for (int i = 0; i < lines.length; i++) {
            buckets.get(i % shards).append(lines[i]).append('\n');
        }
        List<String> out = new ArrayList<>(shards);
        for (StringBuilder b : buckets) {
            if (!b.isEmpty()) {
                out.add(b.toString());
            }
        }
        return out;
    }

    @Override
    public void map(String shard, Emitter<String, Integer> emitter) {
        for (String word : SEPARATOR.split(shard)) {
            if (!word.isBlank()) {
                emitter.emit(word.toLowerCase(), 1);
            }
        }
    }

    @Override
    public Integer reduce(String word, List<Integer> counts) {
        int sum = 0;
        for (Integer c : counts) {
            sum += c;
        }
        return sum;
    }

    /**
     * On. Summing is associative, so adding part and then the rest gives the same answer.
     *
     * <p>The benefit is particularly large for word counting: {@code map} emits a {@code 1}
     * per word, and a frequent word runs to tens of thousands of records within one shard.
     * With this on, a word leaves one record -- <b>and the network volume falls in inverse
     * proportion to how repetitive the keys are</b>.
     */
    @Override
    public boolean combinable() {
        return true;
    }
}
