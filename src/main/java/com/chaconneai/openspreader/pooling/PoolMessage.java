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
package com.chaconneai.openspreader.pooling;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A message of the task dispatch protocol.
 *
 * <p>Same approach as the lock and the semaphore: spreader's business channel is one-way,
 * so the message carries its own requestId to match a response to its request. What
 * differs is that the payload here is a <b>serialised object</b> -- arguments, a return
 * value, a recursive task -- rather than a few strings.
 *
 * <p>Frame layout, big-endian:
 * <pre>
 * magic(4B) | version(1B) | type(1B) | requestId(8B) | depth(4B)
 *           | beanName(UTF) | className(UTF) | methodName(UTF)
 *           | payloadLen(4B) | payload(the serialised bytes)
 *           | success(1B) | message(UTF)
 * </pre>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public record PoolMessage(
        PoolMessageType type,
        long requestId,
        int depth,
        String beanName,
        String className,
        String methodName,
        byte[] payload,
        boolean success,
        String message) {

    /** Magic number "SPCP", for SPreader Commons Pooling. */
    public static final int MAGIC = 0x53504350;

    public static final byte VERSION = 1;

    private static final byte[] NO_PAYLOAD = new byte[0];

    public PoolMessage {
        payload = payload == null ? NO_PAYLOAD : payload;
    }

    public static PoolMessage invoke(long requestId, String beanName, String className,
                                     String methodName, byte[] payload) {
        return new PoolMessage(PoolMessageType.INVOKE, requestId, 0,
                beanName, className, methodName, payload, false, "");
    }

    public static PoolMessage task(long requestId, int depth, byte[] payload) {
        return new PoolMessage(PoolMessageType.TASK, requestId, depth,
                "", "", "", payload, false, "");
    }

    public static PoolMessage ok(long requestId, byte[] payload) {
        return new PoolMessage(PoolMessageType.RESPONSE, requestId, 0,
                "", "", "", payload, true, "");
    }

    public static PoolMessage fail(long requestId, String reason) {
        return new PoolMessage(PoolMessageType.RESPONSE, requestId, 0,
                "", "", "", NO_PAYLOAD, false, reason);
    }

    /** Whether these bytes are a task dispatch protocol message. */
    public static boolean matches(byte[] content) {
        if (content == null || content.length < 6) {
            return false;
        }
        int magic = ((content[0] & 0xFF) << 24) | ((content[1] & 0xFF) << 16)
                | ((content[2] & 0xFF) << 8) | (content[3] & 0xFF);
        return magic == MAGIC;
    }

    public byte[] encode() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(payload.length + 128);
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(MAGIC);
            out.writeByte(VERSION);
            out.writeByte(type.code());
            out.writeLong(requestId);
            out.writeInt(depth);
            out.writeUTF(beanName == null ? "" : beanName);
            out.writeUTF(className == null ? "" : className);
            out.writeUTF(methodName == null ? "" : methodName);
            out.writeInt(payload.length);
            out.write(payload);
            out.writeBoolean(success);
            out.writeUTF(message == null ? "" : message);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    /** @return the decoded message, or null when it is not this protocol, the version is
     *          unrecognised, or the format is wrong */
    public static PoolMessage decode(byte[] content) {
        if (!matches(content)) {
            return null;
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(content))) {
            in.readInt();
            if (in.readByte() != VERSION) {
                return null;
            }
            PoolMessageType type = PoolMessageType.fromCode(in.readByte());
            if (type == null) {
                return null;
            }
            long requestId = in.readLong();
            int depth = in.readInt();
            String beanName = in.readUTF();
            String className = in.readUTF();
            String methodName = in.readUTF();
            int len = in.readInt();
            if (len < 0 || len > content.length) {
                return null;
            }
            byte[] payload = new byte[len];
            in.readFully(payload);
            return new PoolMessage(type, requestId, depth, beanName, className, methodName,
                    payload, in.readBoolean(), in.readUTF());
        } catch (IOException e) {
            return null;
        }
    }
}
