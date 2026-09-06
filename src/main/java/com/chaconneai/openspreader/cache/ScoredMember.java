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
package com.chaconneai.openspreader.cache;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

/**
 * One element of a sorted set: a member and a score.
 *
 * <p>Members are unique and the score decides the ordering. A repeated {@code zadd} on the
 * same member <b>updates its score</b> rather than adding a second entry, exactly as in
 * Redis.
 *
 * <p><b>equals and hashCode are not overridden.</b> This record is only ever ordered by
 * {@link #ORDER} inside the sorted structure, and looking a score up by member goes through
 * a different table. An array's default equals compares by reference, so relying on it for
 * equality would certainly go wrong -- and the path is simply not offered.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public record ScoredMember(byte[] member, double score) {

    /**
     * The ordering: <b>score first, then the member's unsigned byte lexicographic order</b>
     * when scores are equal -- exactly as Redis's ZSET does it.
     *
     * <h2>The second half is not optional</h2>
     * Without the member level, two different members with equal scores compare as 0, the
     * sorted structure treats them as one, and the second inserted displaces the first --
     * <b>with no error</b>.
     *
     * <h2>Why unsigned byte order</h2>
     * This cache stays consistent by every node replaying the same sequence of operations, so
     * the moment the ordering depends on the local environment, the nodes iterate in different
     * orders. Hence the deliberate avoidance of:
     * <ul>
     *   <li>{@code String.compareTo} and {@code Collator} -- they need a charset settled first
     *       and are influenced by locale</li>
     *   <li>signed byte comparison -- {@code (byte) 0x80} is negative and would sort before
     *       {@code 0x00}, the opposite of what byte order suggests</li>
     *   <li>hash order -- not guaranteed consistent between JVMs at all</li>
     * </ul>
     * {@link Arrays#compareUnsigned(byte[], byte[])} looks only at the byte content and gives
     * the same answer on any JVM.
     */
    public static final Comparator<ScoredMember> ORDER = Comparator
            .comparingDouble(ScoredMember::score)
            .thenComparing(ScoredMember::member, Arrays::compareUnsigned);

    public ScoredMember {
        if (member == null) {
            throw new ProcessingCacheException("a sorted set member must not be null");
        }
        if (Double.isNaN(score)) {
            // NaN compares false against everything, so putting one into a sorted structure
            // destroys the ordering entirely -- and reports no error, making it very hard to
            // track down. It is refused at the door, as Redis refuses nan
            throw new ProcessingCacheException("a sorted set score must not be NaN");
        }
    }

    /** The member decoded as a UTF-8 string, for logs and diagnostics. */
    public String memberAsString() {
        return new String(member, StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return memberAsString() + "=" + score;
    }
}
