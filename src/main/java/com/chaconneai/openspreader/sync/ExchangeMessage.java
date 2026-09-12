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
 * A protocol message for the cross-process exchanger.
 *
 * <p>As with {@link SyncMessage}, spreader's business channel is one-way, so request and
 * response are matched by a {@link #requestId()} of the message's own. What differs is that
 * the payload here is <b>the item being exchanged</b>, serialised by the configured
 * {@code ObjectCodec}, rather than a few names and counts.
 *
 * <p>Frame layout, big-endian:
 * <pre>
 * magic(4B) | version(1B) | type(1B) | requestId(8B) | epoch(8B)
 *           | name(UTF) | participantId(UTF)
 *           | payloadLen(4B) | payload(the serialised item)
 *           | state(1B) | success(1B) | message(UTF)
 * </pre>
 *
 * <h2>What the payload means per type</h2>
 * <table border="1">
 *   <caption>Whose item is in the payload</caption>
 *   <tr><th></th><th>payload</th></tr>
 *   <tr><td>ARRIVE request</td><td>the item the caller is handing over</td></tr>
 *   <tr><td>COLLECT, CANCEL, QUERY request</td><td>empty</td></tr>
 *   <tr><td>RESPONSE with state SATISFIED</td><td><b>the partner's item</b></td></tr>
 *   <tr><td>RESPONSE otherwise</td><td>empty</td></tr>
 * </table>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public record ExchangeMessage(
        ExchangeMessageType type,
        long requestId,
        long epoch,
        String name,
        String participantId,
        byte[] payload,
        SyncState state,
        boolean success,
        String message) {

    /** Magic number "SPCX", for SPreader Commons eXchanger. */
    public static final int MAGIC = 0x53504358;

    public static final byte VERSION = 1;

    private static final byte[] NO_PAYLOAD = new byte[0];

    public ExchangeMessage {
        payload = payload == null ? NO_PAYLOAD : payload;
    }

    public static ExchangeMessage request(ExchangeMessageType type, long requestId, long epoch,
                                          String name, String participantId, byte[] payload) {
        return new ExchangeMessage(type, requestId, epoch, name, participantId, payload,
                SyncState.PENDING, false, "");
    }

    /** Nobody is waiting yet, or nobody has arrived yet: keep waiting. */
    public static ExchangeMessage pending(long requestId, long epoch, String name) {
        return new ExchangeMessage(ExchangeMessageType.RESPONSE, requestId, epoch, name, "",
                NO_PAYLOAD, SyncState.PENDING, true, "");
    }

    /** Paired: the payload is the partner's item. */
    public static ExchangeMessage exchanged(long requestId, long epoch, String name,
                                            byte[] partnerPayload) {
        return new ExchangeMessage(ExchangeMessageType.RESPONSE, requestId, epoch, name, "",
                partnerPayload, SyncState.SATISFIED, true, "");
    }

    /** Void: the register turned over, or this party was already cancelled. */
    public static ExchangeMessage broken(long requestId, long epoch, String name, String reason) {
        return new ExchangeMessage(ExchangeMessageType.RESPONSE, requestId, epoch, name, "",
                NO_PAYLOAD, SyncState.BROKEN, true, reason == null ? "" : reason);
    }

    public static ExchangeMessage fail(long requestId, long epoch, String name, String reason) {
        return new ExchangeMessage(ExchangeMessageType.RESPONSE, requestId, epoch, name, "",
                NO_PAYLOAD, SyncState.BROKEN, false, reason);
    }

    /** A push from the leader: a partner has arrived at this exchange point. */
    public static ExchangeMessage notify(long epoch, String name, String participantId) {
        return new ExchangeMessage(ExchangeMessageType.NOTIFY, 0L, epoch, name, participantId,
                NO_PAYLOAD, SyncState.SATISFIED, true, "");
    }

    /** Whether these bytes are a message of this protocol. */
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
            out.writeLong(epoch);
            out.writeUTF(name == null ? "" : name);
            out.writeUTF(participantId == null ? "" : participantId);
            out.writeInt(payload.length);
            out.write(payload);
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
    public static ExchangeMessage decode(byte[] content) {
        if (!matches(content)) {
            return null;
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(content))) {
            in.readInt();
            if (in.readByte() != VERSION) {
                return null;
            }
            ExchangeMessageType type = ExchangeMessageType.fromCode(in.readByte());
            if (type == null) {
                return null;
            }
            long requestId = in.readLong();
            long epoch = in.readLong();
            String name = in.readUTF();
            String participantId = in.readUTF();
            int len = in.readInt();
            if (len < 0 || len > content.length) {
                // A length beyond the frame itself means corrupted bytes. Checking it before
                // allocating matters: an unchecked length word is an invitation to allocate
                // two gigabytes on a single malformed packet
                return null;
            }
            byte[] payload = new byte[len];
            in.readFully(payload);
            return new ExchangeMessage(type, requestId, epoch, name, participantId, payload,
                    SyncState.fromCode(in.readByte()), in.readBoolean(), in.readUTF());
        } catch (IOException e) {
            return null;
        }
    }
}
