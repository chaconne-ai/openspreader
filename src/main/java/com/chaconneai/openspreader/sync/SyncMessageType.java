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
package com.chaconneai.openspreader.sync;

/**
 * The message types shared by latches and barriers.
 *
 * <p>Both use one message format but travel on <b>two different channels</b>
 * （{@code spreader.latch} / {@code spreader.barrier}），
 * so although the types share an enum they never cross. They are together because their
 * field requirements are all but identical, and two separate definitions would only drift
 * apart over time.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum SyncMessageType {

    // ------------------------------------------------------------------
    // Latch
    // ------------------------------------------------------------------

    /** Declares a latch with its initial count. An existing one returns its current state;
     *  a mismatched count is refused. */
    LATCH_DECLARE((byte) 1),

    /** Counts down by one. With a participantId it de-duplicates per participant, so
     *  sending it twice does not count down twice. */
    LATCH_COUNT_DOWN((byte) 2),

    /** Registers as a waiter and asks the current state along the way. It is how the leader
     *  knows whom to push to. */
    LATCH_AWAIT((byte) 3),

    /** Stops waiting, after a timeout or an interrupt, removing itself from the waiter list. */
    LATCH_CANCEL((byte) 4),

    // ------------------------------------------------------------------
    // Barrier
    // ------------------------------------------------------------------

    /** Arrives at the barrier and waits. Reaching parties releases it and moves to the next
     *  generation. */
    BARRIER_AWAIT((byte) 10),

    /** Leaves, after a timeout or an interrupt. By the JDK's semantics this <b>breaks</b>
     *  the current generation. */
    BARRIER_LEAVE((byte) 11),

    /** Asks the current state: how many have arrived this generation, and whether it is broken. */
    BARRIER_QUERY((byte) 12),

    /** Resets: discards this generation and moves to a new one; every waiter receives "broken". */
    BARRIER_RESET((byte) 13),

    // ------------------------------------------------------------------
    // Common
    // ------------------------------------------------------------------

    /**
     * A push from the leader: the condition you are waiting on has an outcome.
     *
     * <p>It exists purely for low latency. Waiters poll as a safety net at the same time --
     * something that hangs forever because one push was lost would be unusable.
     */
    NOTIFY((byte) 20),

    /** The response to a request, matched by requestId. */
    RESPONSE((byte) 21);

    private final byte code;

    SyncMessageType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    /** @return null for an unknown type, leaving the caller to discard it as malformed */
    public static SyncMessageType fromCode(byte code) {
        for (SyncMessageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        return null;
    }
}
