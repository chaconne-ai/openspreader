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
