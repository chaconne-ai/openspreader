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
 * A protocol message for latches and barriers.
 *
 * <p>As with {@link MutexMessage}, spreader's business channel is one-way, so
 * request and response are matched by a {@link #requestId()} of the message's own.
 *
 * <p>Frame layout, big-endian:
 * <pre>
 * magic(4B) | version(1B) | type(1B) | requestId(8B) | epoch(8B)
 *           | name(UTF) | participantId(UTF)
 *           | count(8B) | generation(8B) | state(1B) | success(1B) | message(UTF)
 * </pre>
 *
 * <h2>Two fields mean different things per type</h2>
 * <table border="1">
 *   <caption>What count and generation carry</caption>
 *   <tr><th></th><th>count</th><th>generation</th></tr>
 *   <tr><td>LATCH_DECLARE request</td><td>the initial count</td><td>--</td></tr>
 *   <tr><td>LATCH_* response</td><td>the count remaining</td><td>--</td></tr>
 *   <tr><td>BARRIER_AWAIT request</td><td>parties</td>
 *       <td>the generation the caller sees</td></tr>
 *   <tr><td>BARRIER_* response</td><td>arrival index / arrivals so far</td>
 *       <td>the current generation</td></tr>
 * </table>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public record SyncMessage(
        SyncMessageType type,
        long requestId,
        long epoch,
        String name,
        String participantId,
        long count,
        long generation,
        SyncState state,
        boolean success,
        String message) {

    /** Magic number "SPCB", for SPreader Commons Barrier. Shared by latches and barriers. */
    public static final int MAGIC = 0x53504342;

    public static final byte VERSION = 1;

    /** A plausible ceiling for one message; anything larger is discarded as malformed. */
    private static final int MAX_BYTES = 64 * 1024;

    public static SyncMessage request(SyncMessageType type, long requestId, long epoch,
                                      String name, String participantId,
                                      long count, long generation) {
        return new SyncMessage(type, requestId, epoch, name, participantId,
                count, generation, SyncState.PENDING, false, "");
    }

    public static SyncMessage ok(long requestId, long epoch, String name,
                                 long count, long generation, SyncState state) {
        return new SyncMessage(SyncMessageType.RESPONSE, requestId, epoch, name, "",
                count, generation, state, true, "");
    }

    public static SyncMessage fail(long requestId, long epoch, String name, String reason) {
        return new SyncMessage(SyncMessageType.RESPONSE, requestId, epoch, name, "",
                0L, 0L, SyncState.BROKEN, false, reason);
    }

    /** A push from the leader: the condition under some name has an outcome. */
    public static SyncMessage notify(long epoch, String name, long generation, SyncState state) {
        return new SyncMessage(SyncMessageType.NOTIFY, 0L, epoch, name, "",
                0L, generation, state, true, "");
    }

    /** Whether these bytes are a message of this protocol. */
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
            out.writeUTF(participantId == null ? "" : participantId);
            out.writeLong(count);
            out.writeLong(generation);
            out.writeByte(state == null ? SyncState.PENDING.code() : state.code());
            out.writeBoolean(success);
            out.writeUTF(message == null ? "" : message);
        } catch (IOException e) {
            // A ByteArrayOutputStream never really throws an IOException
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    /** @return null when it is not this protocol, the version is unrecognised, or the
     *          format is wrong */
    public static SyncMessage decode(byte[] content) {
        if (!matches(content)) {
            return null;
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(content))) {
            in.readInt();
            if (in.readByte() != VERSION) {
                return null;
            }
            SyncMessageType type = SyncMessageType.fromCode(in.readByte());
            if (type == null) {
                return null;
            }
            return new SyncMessage(type, in.readLong(), in.readLong(), in.readUTF(),
                    in.readUTF(), in.readLong(), in.readLong(),
                    SyncState.fromCode(in.readByte()), in.readBoolean(), in.readUTF());
        } catch (IOException e) {
            return null;
        }
    }
}
