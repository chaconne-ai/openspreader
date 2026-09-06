package com.chaconneai.openspreader.sync;

/**
 * The message types of the lock protocol.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum MutexMessageType {

    /** Acquire the lock. It fails outright when unavailable; waiting and retrying are the
     *  requester's business. */
    ACQUIRE((byte) 1),

    /** Release the lock. */
    RELEASE((byte) 2),

    /** Renew. The holder sends it periodically to prove it is still alive. */
    RENEW((byte) 3),

    /** Ask who holds it now. */
    QUERY((byte) 4),

    /** The response to any of the four above, matched to its request by requestId. */
    RESPONSE((byte) 5);

    private final byte code;

    MutexMessageType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    /** @return null for an unknown type, leaving the caller to discard it as malformed */
    public static MutexMessageType fromCode(byte code) {
        for (MutexMessageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        return null;
    }
}
