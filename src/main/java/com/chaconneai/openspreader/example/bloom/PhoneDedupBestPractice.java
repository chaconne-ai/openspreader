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
package com.chaconneai.openspreader.example.bloom;

import com.chaconneai.openspreader.cache.ProcessingBloomFilter;
import com.chaconneai.openspreader.cache.ProcessingCache;

import java.nio.charset.StandardCharsets;

/**
 * Cluster-wide de-duplication of a hundred million phone numbers -- which also shows <b>what
 * to do once one bitmap's ceiling is exceeded</b>.
 *
 * <h2>How to use it</h2>
 * <pre>{@code
 * @Autowired
 * private ProcessingCache cache;
 *
 * // A hundred million numbers, tolerating a 1% false-positive rate
 * PhoneDedupBestPractice dedup = new PhoneDedupBestPractice(cache, "sms:sent", 100_000_000, 0.01);
 *
 * if (dedup.markIfFirstTime("13800138000")) {
 *     sendSms(phone);        // first time; send it
 * }                          // otherwise skip
 * }</pre>
 *
 * <h2>Why sharding is necessary</h2>
 * A hundred million entries at a 1% rate need a <b>114 MB</b> bitmap, while one bitmap's
 * ceiling is <b>64 MB</b>, a hard constraint from the cache. A direct {@code create} is
 * refused.
 *
 * <p>So it is divided into shards, each an independent filter, with a number's hash deciding
 * which shard it lands in:
 *
 * <pre>
 * 100M entries at 1%   -&gt;  114 MB   too large
 *   across 4 shards    -&gt;  25M entries each, about 29 MB   fits
 * </pre>
 *
 * <p><b>Sharding does not worsen the false-positive rate.</b> Each number lands in one fixed
 * shard, that shard holds 1/N of the total, and its parameters were computed for 1/N -- so
 * each shard's rate is still 1%, and so is the whole. This differs from "splitting the data
 * arbitrarily"; what matters is that <b>one key always lands in the same shard</b>.
 *
 * <h2>Work the memory out: every node needs those 114 MB</h2>
 * The cluster cache is <b>fully replicated</b>, so those 114 MB exist on every node. A
 * five-node cluster holds 570 MB of heap between them -- a number to know before going live,
 * not after it has eaten the heap.
 *
 * <p>There are only two ways to reduce it: <b>raise the false-positive rate</b> (3% needs only
 * 87 MB), or <b>lower the expected count</b>. The bitmap's size is fixed at creation and
 * cannot be changed afterwards.
 *
 * <h2>Raise the cache's byte ceiling first, or the bitmap is evicted</h2>
 * This is the easiest trap to fall into, and <b>its symptom is thoroughly misleading</b>: the
 * filter starts answering "never seen" for numbers it certainly recorded -- which looks like a
 * broken Bloom filter and is in fact the cache's LRU evicting the bitmap as an ordinary key.
 *
 * <p>{@code max-bytes} defaults to <b>a quarter of the maximum heap</b>. That is:
 *
 * <pre>
 * 512 MB heap  -&gt;  128 MB ceiling  -&gt;  just enough for a 114 MB bitmap
 * 256 MB heap  -&gt;   64 MB ceiling  -&gt;  the bitmap is evicted outright
 * </pre>
 *
 * <p>So at a hundred million entries, the configuration has to satisfy both of these:
 *
 * <pre>
 * spring.spreader.multiprocessing.cache.max-bytes: 268435456   # at least 256MB, with headroom
 * # and the JVM's -Xmx must be markedly larger, since the application needs heap too
 * </pre>
 *
 * <p>{@link #totalBytes()} exists for exactly this -- <b>print it and look before going
 * live</b>, compare it with {@code max-bytes}, and do not wait until production starts missing
 * duplicates.
 *
 * <h2>Why phone numbers suit this</h2>
 * A false positive treats a number that <b>was never sent to</b> as already sent and skips it
 * -- a <b>missed send</b>, not a duplicate one.
 *
 * <ul>
 *   <li>A marketing message missed: acceptable, and only 1%</li>
 *   <li>A verification code missed: <b>unacceptable</b>; the user assumes the system is
 *       broken</li>
 * </ul>
 *
 * <p>So this pattern suits marketing, notifications and de-duplicated statistics, and <b>does
 * not suit verification codes</b>. Those want a unique index in a database, or a cache key
 * with a TTL.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
public class PhoneDedupBestPractice {

    /**
     * How many shards.
     *
     * <p>A power of two, so a bitwise and replaces the modulo ({@code hash & (N-1)}) -- across
     * a hundred million lookups, the difference between a division and a bitwise and is
     * measurable.
     *
     * <p>More shards are not better: each is an independent cache key, and too many raise the
     * key count and the cost of managing them for no gain beyond "each shard is smaller".
     * Enough is enough.
     */
    private static final int SHARDS = 4;

    private final ProcessingBloomFilter[] shards;
    private final long capacity;
    private final double fpp;

    /**
     * @param cache     the cluster cache
     * @param keyPrefix the key prefix; each shard appends {@code :0}, {@code :1} and so on.
     *                  <b>Include a version</b>, such as {@code sms:sent:v1} -- rebuilding
     *                  moves to a new prefix, the old and the new coexist, and the switch is
     *                  atomic
     * @param capacity  how many numbers are expected. <b>Estimate high</b>: overfilling raises
     *                  the false-positive rate sharply, while a high estimate costs only a
     *                  little memory
     * @param fpp       the tolerable false-positive rate
     */
    public PhoneDedupBestPractice(ProcessingCache cache, String keyPrefix,
                                  long capacity, double fpp) {
        this.capacity = capacity;
        this.fpp = fpp;
        this.shards = new ProcessingBloomFilter[SHARDS];

        // Each shard holds 1/N of the total, so its parameters are computed for 1/N --
        // computing them for the total would make every shard full size, wasting N times the
        // memory for nothing
        long perShard = Math.max(1, capacity / SHARDS);
        for (int i = 0; i < SHARDS; i++) {
            shards[i] = ProcessingBloomFilter.create(cache, keyPrefix + ":" + i, perShard, fpp);
        }
    }

    /**
     * Whether this number is being seen for the first time, recording it if so.
     *
     * <pre>{@code
     * if (dedup.markIfFirstTime(phone)) {
     *     sendSms(phone);
     * }
     * }</pre>
     *
     * <h2>Which way a false positive goes</h2>
     * A false return is a false positive with probability about {@code fpp} -- the number was
     * never sent to, but is taken as sent and <b>skipped</b>. So this method suits only cases
     * where missing one does not matter.
     *
     * <p>A true return is <b>certain</b>: when a Bloom filter says "not present", it is not
     * present.
     *
     * @return true when it is seen for the first time and may be sent, false when it may
     *         already have been sent and should be skipped
     */
    public boolean markIfFirstTime(String phone) {
        ProcessingBloomFilter shard = shardOf(phone);
        if (shard.mightContain(phone)) {
            return false;
        }
        shard.put(phone);
        return true;
    }

    /** Queries without recording. */
    public boolean mightHaveSeen(String phone) {
        return shardOf(phone).mightContain(phone);
    }

    /** Records without querying -- for when it is known to be new, saving a lookup. */
    public void remember(String phone) {
        shardOf(phone).put(phone);
    }

    /**
     * Which shard a number lands in.
     *
     * <h2>One number must always land in the same shard</h2>
     * This is the <b>sole premise</b> on which the sharding scheme rests. It is decided by the
     * number's own hash, so the answer is the same on any node at any time.
     *
     * <p>Sharding by a random number, the time, or anything about the node would let one number
     * land in different shards -- recorded in shard A and queried in shard B, the
     * de-duplication simply stops working, and <b>nothing reports it</b>.
     */
    private ProcessingBloomFilter shardOf(String phone) {
        int hash = murmurLike(phone.getBytes(StandardCharsets.UTF_8));
        // SHARDS is a power of two, so a bitwise and is equivalent to a modulo and far quicker
        return shards[hash & (SHARDS - 1)];
    }

    /**
     * A hash that is good enough.
     *
     * <p>Not {@code String.hashCode()}: it scatters strings that differ only in their last
     * character poorly, and phone numbers have exactly that shape -- across a hundred million
     * of them, the great majority differ only in the last few digits. Sharding by it would fill
     * one shard far fuller than the rest, and that shard's false-positive rate would blow
     * up.
     */
    private static int murmurLike(byte[] data) {
        int h = 0x9747b28c;
        for (byte b : data) {
            h ^= b & 0xff;
            h *= 0x5bd1e995;
            h ^= h >>> 15;
        }
        return h & 0x7fffffff;
    }

    /** How many shards. */
    public int shardCount() {
        return SHARDS;
    }

    /** The total capacity. */
    public long capacity() {
        return capacity;
    }

    /**
     * How many bytes this set of filters occupies -- <b>on every node</b>.
     *
     * <p>Work it out before going live: the cluster cache is fully replicated, so five nodes
     * hold five copies.
     */
    public long totalBytes() {
        long bits = 0;
        for (ProcessingBloomFilter shard : shards) {
            bits += shard.bitSize();
        }
        return bits / 8;
    }

    /** A one-line summary, for logs and capacity planning. */
    public String summary() {
        return String.format("%,d numbers / %.1f%% false positives / %d shards / %.1f MB per node",
                capacity, fpp * 100, SHARDS, totalBytes() / 1024.0 / 1024.0);
    }

    /** Clears everything. For rebuilding -- and note that between clearing and refilling,
     *  everything answers "never seen". */
    public void clear() {
        for (ProcessingBloomFilter shard : shards) {
            shard.clear();
        }
    }
}
