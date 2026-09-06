package com.chaconneai.openspreader.pooling;

/**
 * The message types of the task dispatch protocol.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum PoolMessageType {

    /** Invoke a method on a bean; the payload is the serialised argument array. */
    INVOKE((byte) 1),

    /** Run a recursive task; the payload is the serialised task object. */
    TASK((byte) 2),

    /** The response to either of the above; the payload is the serialised return value. */
    RESPONSE((byte) 3);

    private final byte code;

    PoolMessageType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    /** @return null for an unknown type, leaving the caller to discard it as malformed */
    public static PoolMessageType fromCode(byte code) {
        for (PoolMessageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        return null;
    }
}
