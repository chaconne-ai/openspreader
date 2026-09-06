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
package com.chaconneai.openspreader.aggregation;

/**
 * The three messages that flow through one job.
 *
 * <pre>
 * 1. submitter --MAP_TASK------> mapper    a shard, plus this round's reducer roster
 * 2. mapper    --SHUFFLE-------> reducer   "every intermediate result my shard has for you"
 * 3. reducer   --REDUCE_RESULT-> submitter the locally reduced result
 * </pre>
 *
 * <p><b>Unicast throughout</b>; nothing is multicast.
 *
 * <h2>Why there is no separate "that is all" message</h2>
 * An early design followed {@link #SHUFFLE} with a {@code SHUFFLE_EOF}, telling the reducer
 * that this shard had finished sending to it. That design <b>was wrong</b>: two messages are
 * two independent unicasts, and <b>the network guarantees no ordering between them</b>. With
 * the EOF arriving first and the intermediate results after, a reducer would begin reducing
 * before its data was complete -- producing a <b>silently incomplete</b> result, with no
 * error and no timeout, merely a portion missing.
 *
 * <p>Now a shard sends a reducer <b>exactly one message</b>, and that message itself means
 * "that is all". The ordering problem cannot arise, and there are half as many messages.
 *
 * <h2>Why no "job starting" message is needed</h2>
 * A reducer needs to know three things: how many shards to expect, where to send the result,
 * and which job to reduce with. All three <b>travel with {@link #SHUFFLE}</b>, so nobody has
 * to be told in advance -- which removes the question of what happens when that notice is
 * lost.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
public enum MapReduceMessageType {

    /** Submitter to mapper: here is the shard for you to process. */
    MAP_TASK((byte) 1),

    /**
     * Mapper to reducer: every intermediate result this shard has for you, and nothing more
     * after it.
     *
     * <p><b>Not one reducer may be left out.</b> Even when no key from this shard lands on a
     * given reducer, an empty message goes to it -- otherwise that reducer never collects its
     * {@code shardCount} messages, never reduces, never returns a result, and the submitter
     * waits with it until the timeout.
     */
    SHUFFLE((byte) 2),

    /** Reducer to submitter: my part of the reduction is finished. */
    REDUCE_RESULT((byte) 3);

    private final byte code;

    MapReduceMessageType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    /** @return null for an unrecognised code, leaving the caller to decide what to do */
    public static MapReduceMessageType fromCode(byte code) {
        for (MapReduceMessageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        return null;
    }
}
