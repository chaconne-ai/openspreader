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
 * The state a latch, barrier or exchange point is currently in.
 *
 * <p>The exchanger has a message of its own ({@link ExchangeMessage}, because it carries an
 * item) but the same three states, so it reuses this enum rather than declaring a fourth
 * copy of the identical thing.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum SyncState {

    /** Not satisfied yet: the latch has not reached zero, the barrier is not full, or no
     *  partner has arrived at the exchange point. Keep waiting. */
    PENDING((byte) 0),

    /** Satisfied: the latch reached zero, this generation of the barrier was released, or the
     *  exchange paired. */
    SATISFIED((byte) 1),

    /**
     * Void; stop waiting.
     *
     * <p>Three ways to get here:
     * <ul>
     *   <li>the barrier was broken -- a participant timed out, was interrupted, or left</li>
     *   <li>the leader changed -- the register lived in the old leader's memory and is gone</li>
     *   <li>an explicit reset</li>
     *   <li>the exchange point no longer knows this party, because it was cancelled or the
     *       register turned over</li>
     * </ul>
     * A waiter receiving this state must leave immediately and report an error. It
     * <b>must not</b> be treated as "keep waiting": the condition will never be satisfied
     * now, and waiting on means hanging forever.
     */
    BROKEN((byte) 2);

    private final byte code;

    SyncState(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static SyncState fromCode(byte code) {
        for (SyncState s : values()) {
            if (s.code == code) {
                return s;
            }
        }
        return BROKEN;   // Anything unrecognised counts as void, which errs in the safe direction
    }
}
