package com.chaconneai.openspreader.cache;

/**
 * The message types of the cache protocol.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum CacheMessageType {

    /** Follower to leader: please perform this write. */
    WRITE(1),

    /**
     * Leader to requester: the result of the write.
     *
     * <p>The response <b>carries the operation itself back, unchanged, with its version</b>,
     * and the requester applies it locally the moment it arrives. That way "write then read
     * immediately" reads back what was written, without waiting for the broadcast to come
     * round. The broadcast arrives later too, and the duplicate version is ignored.
     */
    RESPONSE(2),

    /** Leader to everyone: one performed write, carrying its version, replayed in order by
     *  whoever receives it. */
    UPDATE(3),

    /** Any node to leader: send me a full snapshot. */
    SYNC(4),

    /** Leader to requester: one chunk of the snapshot. */
    SNAPSHOT(5),

    /**
     * Leader to everyone: a batch of performed writes, packed into one frame.
     *
     * <p>Under a heavy write rate, one frame per write pins throughput to how many frames can
     * be sent per second -- acknowledged multicast measures at around 2600 frames per second,
     * so writes would cap at 2600 per second too. Packed, one frame holds hundreds, and
     * throughput comes free of the frame count entirely.
     *
     * <p>The packing <b>happens naturally</b> rather than through a batching timer: while the
     * broadcast thread is idle, each write goes out as it arrives, at the lowest possible
     * latency. Only when it cannot keep up and the queue builds does the next send take the
     * backlog with it. So latency is unchanged at low load and throughput rises by itself at
     * high load.
     */
    BATCH(6),

    /**
     * Leader to everyone: this is my epoch.
     *
     * <p>It closes a window that genuinely exists: a node has just taken over and has not yet
     * opened a new epoch when someone pulls a snapshot from it, and that snapshot carries the
     * old epoch. As long as nothing is written afterwards, whoever pulled it goes on believing
     * it is aligned.
     *
     * <p>Nothing goes wrong without it -- the first update to arrive has a mismatched epoch,
     * which triggers a fresh full pull -- but that means the first write after a change of
     * leader pays for an extra full synchronisation. Announcing it up front closes the
     * window.
     */
    EPOCH(7),

    /**
     * Follower to leader: these are the keys I read this round.
     *
     * <p>Why it is needed: reads complete <b>locally</b> on each node and the leader sees not
     * one of them, while eviction can only be done by the leader, being the sole write entry
     * point. Without the report the leader holds records of writes alone, and LRU would evict
     * a hot key that is only ever read as though it were cold -- precisely what a cache most
     * wants to keep.
     *
     * <p>The reporting is sampled, capped in count, sent periodically in batches, and
     * <b>makes no completeness guarantee</b>. Eviction is a heuristic, and evicting the wrong
     * key occasionally costs one trip to the source.
     */
    ACCESS(8);

    private final byte code;

    CacheMessageType(int code) {
        this.code = (byte) code;
    }

    public byte code() {
        return code;
    }

    public static CacheMessageType fromCode(byte code) {
        for (CacheMessageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        return null;
    }
}
