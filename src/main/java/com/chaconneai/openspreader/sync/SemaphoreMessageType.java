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
 * The message types of the semaphore protocol.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum SemaphoreMessageType {

    /** Acquire one permit. It fails outright when none is left; waiting and retrying are
     *  the requester's business. */
    ACQUIRE((byte) 1),

    /** Return one permit. */
    RELEASE((byte) 2),

    /** Renew. The holder sends it periodically to prove it is still alive. */
    RENEW((byte) 3),

    /** Ask how many permits remain. */
    QUERY((byte) 4),

    /** The response to any of the four above, matched to its request by requestId. */
    RESPONSE((byte) 5);

    private final byte code;

    SemaphoreMessageType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    /** @return null for an unknown type, leaving the caller to discard it as malformed */
    public static SemaphoreMessageType fromCode(byte code) {
        for (SemaphoreMessageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        return null;
    }
}
