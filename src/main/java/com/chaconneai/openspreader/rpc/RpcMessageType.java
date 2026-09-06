package com.chaconneai.openspreader.rpc;

/**
 * The kinds of RPC message.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum RpcMessageType {

    /** A request: invoke a method on a bean. */
    REQUEST((byte) 1),

    /** A response, carrying either the return value or the reason it failed. */
    RESPONSE((byte) 2);

    private final byte code;

    RpcMessageType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    /** @return null for an unrecognised code, so the caller can treat it as not being one
     *          of our messages */
    public static RpcMessageType fromCode(byte code) {
        for (RpcMessageType t : values()) {
            if (t.code == code) {
                return t;
            }
        }
        return null;
    }
}
