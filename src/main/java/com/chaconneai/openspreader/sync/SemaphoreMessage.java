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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A message of the semaphore protocol.
 *
 * <p>Same approach as {@link MutexMessage}: spreader's business channel is one-way, so the
 * message carries its own {@link #requestId()} to match a response to its request. What
 * differs is two extra fields: {@link #permits()}, how many permits this semaphore has in
 * total, and {@link #available()}, how many remain.
 *
 * <p>Its magic number differs from the lock's and it travels on a different channel, so
 * the two cannot be confused.
 *
 * <p>Frame layout, big-endian:
 * <pre>
 * magic(4B) | version(1B) | type(1B) | requestId(8B) | epoch(8B)
 *           | name(UTF) | value(UTF) | permits(4B) | available(4B)
 *           | leaseMs(8B) | success(1B) | message(UTF)
 * </pre>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public record SemaphoreMessage(
        SemaphoreMessageType type,
        long requestId,
        long epoch,
        String name,
        String value,
        int permits,
        int available,
        long leaseMs,
        boolean success,
        String message) {

    /** Magic number "SPCS", for SPreader Commons Semaphore. */
    public static final int MAGIC = 0x53504353;

    public static final byte VERSION = 1;

    private static final int MAX_BYTES = 64 * 1024;

    public static SemaphoreMessage request(SemaphoreMessageType type, long requestId, long epoch,
                                           String name, String value, int permits, long leaseMs) {
        return new SemaphoreMessage(type, requestId, epoch, name, value, permits, 0, leaseMs, false, "");
    }

    public static SemaphoreMessage ok(long requestId, long epoch, String name, int available) {
        return new SemaphoreMessage(SemaphoreMessageType.RESPONSE, requestId, epoch, name,
                "", 0, available, 0L, true, "");
    }

    public static SemaphoreMessage fail(long requestId, long epoch, String name,
                                        int available, String reason) {
        return new SemaphoreMessage(SemaphoreMessageType.RESPONSE, requestId, epoch, name,
                "", 0, available, 0L, false, reason);
    }

    /** Whether these bytes are a semaphore protocol message. */
    public static boolean matches(byte[] content) {
        if (content == null || content.length < 6 || content.length > MAX_BYTES) {
            return false;
        }
        int magic = ((content[0] & 0xFF) << 24) | ((content[1] & 0xFF) << 16)
                | ((content[2] & 0xFF) << 8) | (content[3] & 0xFF);
        return magic == MAGIC;
    }

    public byte[] encode() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(128);
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(MAGIC);
            out.writeByte(VERSION);
            out.writeByte(type.code());
            out.writeLong(requestId);
            out.writeLong(epoch);
            out.writeUTF(name == null ? "" : name);
            out.writeUTF(value == null ? "" : value);
            out.writeInt(permits);
            out.writeInt(available);
            out.writeLong(leaseMs);
            out.writeBoolean(success);
            out.writeUTF(message == null ? "" : message);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    /** @return the decoded message, or null when it is not the semaphore protocol, the
     *          version is unrecognised, or the format is wrong */
    public static SemaphoreMessage decode(byte[] content) {
        if (!matches(content)) {
            return null;
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(content))) {
            in.readInt();
            byte version = in.readByte();
            if (version != VERSION) {
                return null;
            }
            SemaphoreMessageType type = SemaphoreMessageType.fromCode(in.readByte());
            if (type == null) {
                return null;
            }
            return new SemaphoreMessage(type, in.readLong(), in.readLong(), in.readUTF(),
                    in.readUTF(), in.readInt(), in.readInt(), in.readLong(),
                    in.readBoolean(), in.readUTF());
        } catch (IOException e) {
            return null;
        }
    }
}
