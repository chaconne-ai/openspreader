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
 * The message types of the exchange protocol.
 *
 * <h2>Why the exchanger does not share the latch and barrier's message</h2>
 * {@link SyncMessage} carries names and counts alone. An exchange carries <b>the item
 * itself</b>, a serialised byte stream of unbounded size, and putting a payload field on
 * the shared message would make every latch count-down and every barrier arrival carry a
 * length word they never use.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public enum ExchangeMessageType {

    /** Arrives at the exchange point carrying an item. With a partner already waiting, the
     *  reply carries that partner's item and the exchange is done in one round trip. */
    ARRIVE((byte) 1),

    /** Asks whether a partner has arrived, and takes its item along the way. Sent after a
     *  waiter wakes. */
    COLLECT((byte) 2),

    /**
     * Stops waiting, after a timeout or an interrupt.
     *
     * <p>Unlike the latch's CANCEL, this one <b>can still come back with an item</b>: a
     * pairing may have happened on the leader in the instant before the cancel arrived, and
     * that item has nowhere else to go. See {@link ExchangeEntry#cancel}.
     */
    CANCEL((byte) 3),

    /** Asks the current state: whether anyone is waiting at this exchange point. For
     *  diagnostics only. */
    QUERY((byte) 4),

    /**
     * A push from the leader: a partner has arrived, so come and collect the item.
     *
     * <p>It is <b>unicast to the one node the waiter sits on</b>, not broadcast. See
     * {@code ExchangerService#notifyPartner}.
     */
    NOTIFY((byte) 20),

    /** The response to a request, matched by requestId. */
    RESPONSE((byte) 21);

    private final byte code;

    ExchangeMessageType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    /** @return null for an unknown type, leaving the caller to discard it as malformed */
    public static ExchangeMessageType fromCode(byte code) {
        for (ExchangeMessageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        return null;
    }
}
