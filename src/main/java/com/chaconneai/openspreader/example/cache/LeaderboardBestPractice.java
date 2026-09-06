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
package com.chaconneai.openspreader.example.cache;

import com.chaconneai.openspreader.cache.ProcessingCache;
import com.chaconneai.openspreader.cache.ScoredMember;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A live leaderboard: any instance records a score, and every instance sees the same board.
 *
 * <h2>How to use it</h2>
 * <pre>{@code
 * @Autowired
 * private ProcessingCache cache;
 *
 * LeaderboardBestPractice board = new LeaderboardBestPractice(cache, "game:2026");
 *
 * board.addScore("alice", 150);         // scored at the end of a round
 * board.top(10);                        // the front page's top ten
 * board.rankOf("alice");                // "you are 42nd"
 * board.around("alice", 5);             // "you, and five either side of you"
 * }</pre>
 *
 * <h2>Why a sorted set rather than a database</h2>
 * A leaderboard's access pattern is extreme: writes are frequent, since every round is scored,
 * and reads more so, since everyone looks at the board -- and <b>the rank is computed</b>. In
 * a database that means {@code ORDER BY score LIMIT n} every time, and a large board has to
 * fall back on a scheduled pre-computation, at which point it is no longer live.
 *
 * <p>A sorted set maintains the order by score, so the top N costs {@code O(log n + m)}.
 *
 * <h2>One performance trap worth knowing</h2>
 * {@link #rankOf} is <b>{@code O(n)}</b> -- the red-black tree underneath has no span
 * information as Redis's skip list does, so locating by index means counting through one by
 * one.
 *
 * <p>A board of a few thousand does not care; on <b>a board of a few hundred thousand,
 * looking up a rank on every request is slow</b>. The usual approach at that scale is to
 * bucket by score: count how many buckets hold scores above mine, then count exactly within
 * the bucket, reducing O(n) to O(buckets).
 *
 * <h2>What it is not for</h2>
 * Board data, once lost, is gone -- the cache does not persist, and a full cluster restart
 * clears it. Anything that must leave a trace, such as season settlement or handing out
 * prizes, has to go to a database; <b>the board here is only a live view</b>.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
public class LeaderboardBestPractice {

    private final ProcessingCache cache;
    private final String key;

    /**
     * @param key the board's key. <b>Include the season or the date</b>, such as
     *            {@code game:2026-08} -- a new season means a new key, so the old board lapses
     *            of its own accord and needs no cleanup
     */
    public LeaderboardBestPractice(ProcessingCache cache, String key) {
        this.cache = cache;
        this.key = key;
    }

    /**
     * Adds to a score. <b>It accumulates rather than overwrites.</b>
     *
     * <p>{@code zincrby} rather than read-then-write, because the latter loses updates across
     * instances: two instances both read 100, each add 50, and both write back 150 where the
     * answer should be 200. {@code zincrby} accumulates on the leader, so the problem does not
     * arise.
     *
     * @return the total after adding
     */
    public double addScore(String player, double points) {
        return cache.zincrby(key, player.getBytes(StandardCharsets.UTF_8), points);
    }

    /**
     * Sets the score outright, overwriting it.
     *
     * <p>For where the score is computed elsewhere -- a settlement service pushing a total
     * across, say. Everyday scoring should use {@link #addScore}.
     */
    public void setScore(String player, double score) {
        cache.zadd(key, player.getBytes(StandardCharsets.UTF_8), score);
    }

    /**
     * The top N, by score descending.
     *
     * <p>It uses {@code zrevrange}: a sorted set <b>always stores in ascending order of
     * score</b>, and the direction is chosen at query time. For the N lowest scores, use
     * {@code zrange}.
     */
    public List<ScoredMember> top(int n) {
        return cache.zrevrange(key, 0, n - 1);
    }

    /**
     * Someone's rank, <b>starting from 1</b> -- a rank for people to read, not an index.
     *
     * @return -1 when they are not on the board
     */
    public long rankOf(String player) {
        long rank = cache.zrevrank(key, player.getBytes(StandardCharsets.UTF_8));
        return rank < 0 ? -1 : rank + 1;
    }

    /**
     * Someone's score.
     *
     * @return 0 when they are not on the board
     */
    public double scoreOf(String player) {
        long rank = cache.zrevrank(key, player.getBytes(StandardCharsets.UTF_8));
        if (rank < 0) {
            return 0d;
        }
        List<ScoredMember> one = cache.zrevrange(key, (int) rank, (int) rank);
        return one.isEmpty() ? 0d : one.get(0).score();
    }

    /**
     * "You, and N either side of you" -- the commonest block on a leaderboard page.
     *
     * <p>Far more useful than showing the top ten alone: the top ten mean nothing to a player
     * ranked 5000th, who wants to know <b>how many more points would overtake the player
     * ahead</b>.
     *
     * @param radius how many to take on each side
     * @return an empty list when they are not on the board
     */
    public List<ScoredMember> around(String player, int radius) {
        long rank = cache.zrevrank(key, player.getBytes(StandardCharsets.UTF_8));
        if (rank < 0) {
            return List.of();
        }
        int start = (int) Math.max(0, rank - radius);
        return cache.zrevrange(key, start, (int) rank + radius);
    }

    /**
     * Takes the board a page at a time.
     *
     * <pre>{@code
     * page(0, 20);   // page 1, ranks 1 to 20
     * page(1, 20);   // page 2, ranks 21 to 40
     * }</pre>
     */
    public List<ScoredMember> page(int pageIndex, int pageSize) {
        int start = pageIndex * pageSize;
        return cache.zrevrange(key, start, start + pageSize - 1);
    }

    /** How many are on the board. */
    public int size() {
        return cache.zcard(key);
    }

    /**
     * How many have a score within a range.
     *
     * <p>For banded statistics: "how many are above 90?", "what share passed?" It is
     * {@code O(log n)}, far quicker than fetching them and counting.
     */
    public int countBetween(double minScore, double maxScore) {
        return cache.zcount(key, minScore, maxScore);
    }

    /** Removes someone from the board -- banned for cheating, say. */
    public boolean remove(String player) {
        return cache.zrem(key, player.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * A snapshot of the board, in a readable form.
     *
     * <p>The member inside a {@link ScoredMember} is a byte array -- the cache assumes nothing
     * about what is stored in it. Decoding before display is the caller's job, and naming the
     * charset there matters: without it, non-ASCII names come out differently in different
     * environments.
     */
    public List<String> topAsText(int n) {
        List<ScoredMember> members = top(n);
        List<String> out = new ArrayList<>(members.size());
        for (int i = 0; i < members.size(); i++) {
            ScoredMember m = members.get(i);
            out.add(String.format("%d. %s  %.0f pts",
                    i + 1, new String(m.member(), StandardCharsets.UTF_8), m.score()));
        }
        return out;
    }
}
