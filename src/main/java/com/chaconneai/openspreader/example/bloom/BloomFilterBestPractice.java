package com.chaconneai.openspreader.example.bloom;

import com.chaconneai.openspreader.bloom.ProcessingBloomFilter;
import com.chaconneai.openspreader.cache.ProcessingCache;

import java.util.function.Function;

/**
 * A Bloom filter shared across the cluster: it <b>turns away the queries that certainly
 * cannot succeed</b>.
 *
 * <h2>How to use it</h2>
 * <pre>{@code
 * @Autowired
 * private ProcessingCache cache;
 *
 * // A million keys expected, tolerating a 1% false-positive rate
 * ProcessingBloomFilter seen = ProcessingBloomFilter.create(cache, "seen:users", 1_000_000, 0.01);
 *
 * if (!seen.mightContain(userId)) {
 *     return null;              // certainly absent; the database need not be touched
 * }
 * return loadFromDatabase(userId);   // possibly present, so query it -- 1% of these are wasted
 * }</pre>
 *
 * <h2>It has one purpose: saving queries that cannot succeed</h2>
 * A Bloom filter's answers are <b>asymmetric</b>:
 *
 * <ul>
 *   <li>"<b>Not present</b>" -- certainly absent, so return at once</li>
 *   <li>"<b>Possibly present</b>" -- uncertain, so the real source still has to be
 *       queried</li>
 * </ul>
 *
 * <p>So it <b>cannot</b> establish that something exists, only deny quickly. Treating it as a
 * dictionary of what is present is the commonest misuse.
 *
 * <h2>When it is worth using</h2>
 * There is a single test: <b>what proportion of queries come up empty</b>.
 *
 * <ul>
 *   <li>Guarding against cache penetration -- a great many requests for ids that do not exist
 *       (crawlers, attacks, dirty data), all of which reach the database. A filter turns away
 *       nearly all of them</li>
 *   <li>De-duplication -- "has this message been handled?", "has this URL been crawled?" at a
 *       volume too large for a Set in memory</li>
 * </ul>
 *
 * <p>Conversely, where queries nearly always hit, the filter says "possibly present" every
 * time and adds a test for nothing -- <b>a pure loss</b>.
 *
 * <h2>Why it can be built on the cluster cache</h2>
 * {@code setbit} is what makes it possible: the cache replicates <b>operations</b> rather than
 * data, so broadcasting "set bit N" is a few dozen bytes, <b>independent of how large the
 * bitmap is</b>.
 *
 * <p>With only "read the whole bitmap, change a few bits, write it back", a filter for a
 * hundred million entries would be 120MB and every put would broadcast 120MB -- the approach
 * simply would not stand.
 *
 * <h2>One hard constraint: nothing can be removed</h2>
 * A Bloom filter <b>only adds</b>. A bit may be shared by several keys, and clearing it would
 * make other keys "absent" too -- a <b>false negative</b>, which contradicts its entire value.
 *
 * <p>So the data grows stale: a deleted user id remains in the filter and the false-positive
 * rate climbs with it. The remedy is <b>rebuilding the whole thing periodically</b>; see
 * {@link #rebuild}.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
public class BloomFilterBestPractice {

    private final ProcessingBloomFilter filter;

    /**
     * @param cache              the cluster cache
     * @param key                the filter's key. <b>Include a version or a date</b>, such as
     *                           {@code seen:users:v2} -- rebuilding moves to a new key, the old
     *                           and the new coexist, and the switch is atomic
     * @param expectedInsertions how many entries are expected. <b>Estimate this high</b>; see
     *                           {@link #whySizingMatters}
     * @param falsePositiveRate  the tolerable false-positive rate; 0.01 is 1%
     */
    public BloomFilterBestPractice(ProcessingCache cache, String key,
                                   long expectedInsertions, double falsePositiveRate) {
        this.filter = ProcessingBloomFilter.create(cache, key,
                expectedInsertions, falsePositiveRate);
    }

    /**
     * Guarding against cache penetration -- the most typical usage.
     *
     * <pre>{@code
     * User user = practice.loadIfPossible(userId, this::queryDatabase);
     * }</pre>
     *
     * <h2>The flow</h2>
     * <ol>
     *   <li>The filter says "not present" -- <b>return null without touching the
     *       database</b></li>
     *   <li>The filter says "possibly present" -- query the real source</li>
     * </ol>
     *
     * <p>Step 2 is a wasted query with probability about {@code falsePositiveRate}. Trading 1%
     * wasted queries for 99% of the queries that could never have succeeded is an excellent
     * bargain where penetration is the problem.
     *
     * <h2>The premise: every key that exists must have been put in</h2>
     * This is a <b>correctness premise</b>, not an optimisation. Missing one key has the filter
     * say "not present" about data that really exists, and the application cannot find it --
     * and it is very hard to investigate, because the database plainly has it.
     *
     * <p>So wherever data is written, a put must follow; see {@link #onCreated}.
     */
    public <T> T loadIfPossible(String id, Function<String, T> loader) {
        if (!filter.mightContain(id)) {
            // "Not present" means certainly absent -- a Bloom filter's one strong guarantee
            return null;
        }
        return loader.apply(id);
    }

    /**
     * Adds new data to the filter <b>immediately</b> after it reaches the database.
     *
     * <h2>The order cannot be reversed</h2>
     * <b>Write the source first, then put into the filter</b>:
     *
     * <ul>
     *   <li>put before writing: a failed write leaves the filter holding a key that does not
     *       exist -- one wasted query, and harmless</li>
     *   <li><b>write before putting</b>: a failed put has the filter say "not present" about
     *       data that really exists -- <b>and the application cannot find it</b></li>
     * </ul>
     *
     * <p>The two failures cost wildly different amounts, so a wasted query is the lesser evil.
     * Which is to say that this method belongs <b>after</b> the transaction commits.
     *
     * @return true when this call really did change some bits from 0 to 1
     */
    public boolean onCreated(String id) {
        return filter.put(id);
    }

    /**
     * De-duplication: has this been handled?
     *
     * <pre>{@code
     * if (practice.markIfFirstTime(messageId)) {
     *     handle(message);          // first time seen; handle it
     * }                             // otherwise skip
     * }</pre>
     *
     * <h2>Think through which way a false positive goes here</h2>
     * A false positive treats a message that <b>was never handled</b> as handled and
     * <b>skips</b> it -- a <b>missed message</b>, not a repeated one.
     *
     * <p>So it suits only cases where missing one does not matter: de-duplicating a crawler's
     * work, sampling logs. Where missing one is an error -- orders, payments -- it <b>must
     * not</b> be used for de-duplication; that wants a unique index in a database.
     *
     * @return true when it is being seen for the first time and may be handled
     */
    public boolean markIfFirstTime(String id) {
        if (filter.mightContain(id)) {
            return false;       // possibly seen; conservatively taken as seen
        }
        filter.put(id);
        return true;
    }

    /**
     * Rebuilds periodically, clearing out data that has since been deleted.
     *
     * <h2>Why a rebuild is necessary</h2>
     * A Bloom filter <b>only adds</b> -- a bit may be shared by several keys, and clearing it
     * would turn other keys into false negatives. So deleted data still occupies bits, the
     * effective number of keys grows, and <b>the false-positive rate climbs with it</b>.
     *
     * <p>At twice the expected count, a designed rate of 1% becomes something above 5%.
     *
     * <h2>Rebuild under a new key rather than clearing in place</h2>
     * <pre>{@code
     * // Good: the old and the new coexist, and the switch is atomic
     * v2 = ProcessingBloomFilter.create(cache, "seen:users:v2", ...);
     * loadAllIdsFromDb().forEach(v2::put);
     * // switch over once everything is loaded
     *
     * // Bad: between clear() and the end of loading the filter is empty, and every query in
     * // that window is judged "not present" -- false negatives throughout
     * }</pre>
     *
     * <p>This method demonstrates <b>clearing in place</b>, which is only for a filter not yet
     * in use, or where a brief spell of universal false negatives is acceptable.
     */
    public void rebuild(Iterable<String> allExistingIds) {
        filter.clear();
        for (String id : allExistingIds) {
            filter.put(id);
        }
    }

    /**
     * Estimating capacity -- <b>estimate high rather than low</b>.
     *
     * <h2>What happens when the estimate is low</h2>
     * The bitmap size and hash count are fixed <b>at creation</b> from the expected count and
     * never change afterwards. Holding more than expected pushes the false-positive rate past
     * its designed value, and <b>sharply</b>:
     *
     * <ul>
     *   <li>At the expected count: 1%, as designed</li>
     *   <li>At twice: about 5%</li>
     *   <li>At five times: about 30%, which is essentially useless</li>
     * </ul>
     *
     * <h2>A high estimate costs far less</h2>
     * Only memory, and linearly at that: a million keys at 1% is about 1.2MB, and estimating
     * ten million is 12MB. That much memory to stop worrying about the rate running away is a
     * good bargain.
     *
     * <p>The bitmap ceiling is 64MB, roughly 500 million keys at 1%. Beyond that, consider
     * sharding.
     */
    public String whySizingMatters() {
        return String.format("a bitmap of %d bits, about %.1f MB, with %d hash functions",
                filter.bitSize(), filter.bitSize() / 8.0 / 1024 / 1024, filter.hashCount());
    }

    /**
     * <b>A network partition can produce false negatives</b> -- and this determines where it
     * can be used.
     *
     * <p>A Bloom filter's entire value rests on "when it says no, the answer is certainly no".
     * In this cluster, leader uniqueness is <b>an agreement about timing</b> -- whoever takes
     * the cluster port is the leader -- and <b>not consensus</b>.
     *
     * <p>Under a network partition each side may have a leader accepting setbits of its own,
     * and merging afterwards <b>loses bits</b>. A filter that has lost bits answers "not
     * present" for a key that really was put in.
     *
     * <table border="1">
     *   <caption>Whether it can be used</caption>
     *   <tr><th>Use</th><th>Is a false negative acceptable</th><th>Conclusion</th></tr>
     *   <tr><td>Guarding against cache penetration</td><td>Yes -- one extra database query</td>
     *       <td><b>Suitable</b></td></tr>
     *   <tr><td>De-duplicating a crawler</td><td>Yes -- one URL fetched again</td>
     *       <td><b>Suitable</b></td></tr>
     *   <tr><td>De-duplicating orders</td><td>No -- an order would be placed twice</td>
     *       <td><b>Do not use</b></td></tr>
     *   <tr><td>Authorisation decisions</td><td>No -- something would be let through that
     *       should not be</td><td><b>Do not use</b></td></tr>
     * </table>
     *
     * <p>The test is simple: <b>if a false negative merely makes you do again something you
     * could always have done, it is usable; if a false negative makes you do the wrong thing,
     * it is not.</b>
     */
    public void whenNotToUse() {
        // See the javadoc
    }

    /** The underlying filter, for finer control. */
    public ProcessingBloomFilter filter() {
        return filter;
    }
}
