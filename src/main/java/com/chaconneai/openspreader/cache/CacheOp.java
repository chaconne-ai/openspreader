package com.chaconneai.openspreader.cache;

/**
 * One write operation.
 *
 * <p>The cache synchronises by <b>replicating operations</b> rather than data: the leader
 * performs one, broadcasts it along with a version, and every other node replays it. Because
 * everyone starts from the same state and replays in the same order, everyone arrives at the
 * same result.
 *
 * <p>Every entry here must therefore be <b>deterministic</b> -- the same input replayed on
 * any node must produce the same result. An operation like "pop a random element" cannot be
 * added, unless it is changed so that the leader computes the outcome and broadcasts
 * that.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum CacheOp {

    /** Overwrites. {@code arg} is the lifetime in milliseconds; 0 means no expiry, and clears
     *  any existing TTL. */
    SET(1),

    /** Writes only when the key is absent. As SET, with one more test. */
    SET_IF_ABSENT(2),

    /** Deletes the whole key. */
    DEL(3),

    /**
     * Changes the lifetime.
     *
     * <p>{@code arg <= 0} <b>deletes the key immediately</b>, as Redis's EXPIRE does;
     * cancelling an expiry is {@link #PERSIST}'s job. The two mean opposite things -- do not
     * confuse them.
     */
    EXPIRE(4),

    /** Adds to a decimal integer; {@code arg} is the increment. Any existing TTL is kept. */
    INCR(5),

    LPUSH(6),

    RPUSH(7),

    /** Pops the head. Emptying the list deletes the key along with it. */
    LPOP(8),

    RPOP(9),

    HSET(10),

    HDEL(11),

    /** Clears every key. */
    CLEAR(12),

    /**
     * Adds a member to a sorted set, or changes its score. {@code value} is the member and
     * {@code arg} the score, encoded by {@link Double#doubleToRawLongBits}.
     */
    ZADD(13),

    /** Removes a member from a sorted set. {@code value} is the member. */
    ZREM(14),

    /**
     * Adds an increment to a sorted set member's score, treating an absent member as 0, as
     * Redis's ZINCRBY does. {@code arg} is the encoded increment.
     */
    ZINCRBY(15),

    /**
     * Pops the member with the lowest or highest score, matching Redis's ZPOPMIN and ZPOPMAX.
     *
     * <p><b>These two appear only in client-to-leader requests and are never broadcast.</b>
     * The outcome of popping depends on the local ordering, so having each node pop for itself
     * would let the slightest difference in order pop <b>different members</b> -- forking the
     * replicas on the spot, with no error. Instead the leader works out which member it is and
     * broadcasts a {@link #ZREM} naming it.
     */
    ZPOPMIN(16),

    ZPOPMAX(17),

    /** Removes a key's lifetime so it no longer expires. Matches Redis's PERSIST. */
    PERSIST(18),

    /**
     * Sets one bit to 0 or 1, matching Redis's SETBIT. {@code arg} is the bit offset, and a
     * non-zero first byte of {@code value} means set it to 1.
     *
     * <h2>Why this deserves an operation of its own</h2>
     * A bitmap lives in a {@link CacheValue.Kind#STRING}, so in principle "read it, change a
     * bit, write it back" would work. But that would broadcast <b>the entire bitmap</b> for
     * every bit changed -- a hundred-million-element Bloom filter is 120MB, and one
     * {@code put()} changes k bits.
     *
     * <p>As an operation, what gets broadcast is "set bit N", a few dozen bytes, entirely
     * independent of how large the bitmap is. This is precisely where replicating operations
     * rather than data pays off.
     *
     * <p>The byte array grows as needed, and bits never written are 0, as in Redis.
     */
    SETBIT(19),

    /**
     * Keeps one stretch of a list and trims both ends, matching Redis's LTRIM.
     * The high 32 bits of {@code arg} are start and the low 32 stop; both may be negative,
     * counting from the tail.
     *
     * <p>Trimming to empty deletes the key along with it, matching what LPOP does when it
     * empties a list.
     */
    LTRIM(20),

    /**
     * Adds to an integer held in a hash field, matching Redis's HINCRBY.
     * {@code field} is the field name and {@code arg} the increment; an absent field starts
     * from 0.
     */
    HINCRBY(21),

    /**
     * Statistical aggregation: records a sample against a running maximum. {@code arg} is the
     * sample, encoded by {@link Double#doubleToRawLongBits}.
     *
     * <p>It follows the same double-encoding convention as {@link #ZADD}. A long would not
     * do, because {@link #SUM} computes an average and integers would drop the fraction.
     */
    MAX(22),

    /** Statistical aggregation: records a sample against a running minimum. Encoded as
     *  {@link #MAX} is. */
    MIN(23),

    /**
     * Statistical aggregation: adds a sample to a running sum and <b>increments the count at
     * the same time</b>. Encoded as {@link #MAX} is.
     *
     * <p>The count is implicit; there is no command of its own for it. As two operations,
     * losing either one would skew the average permanently, and nothing in the result would
     * show it -- bound into one, that window does not exist.
     */
    SUM(24);

    private final byte code;

    CacheOp(int code) {
        this.code = (byte) code;
    }

    public byte code() {
        return code;
    }

    public static CacheOp fromCode(byte code) {
        for (CacheOp op : values()) {
            if (op.code == code) {
                return op;
            }
        }
        return null;
    }
}
