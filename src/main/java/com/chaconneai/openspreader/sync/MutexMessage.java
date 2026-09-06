package com.chaconneai.openspreader.sync;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A message of the lock protocol.
 *
 * <p>spreader's business channel is one-way -- send it, and the peer receives it through
 * {@code onPayload} -- with no request/response semantics. A lock acquisition must get an
 * answer of "granted" or "refused", so the message carries a {@link #requestId()} of its
 * own: the requester notes it on sending, the response comes back carrying the same id,
 * and the requester matches the two.
 *
 * <h2>Why a magic number</h2>
 * {@code onPayload} receives all of the application's business messages, with lock traffic
 * mixed in among them. The leading four bytes {@code SPCM} identify a message as belonging
 * to the lock protocol; anything else passes straight through to the application, so other
 * people's messages are never misparsed.
 *
 * <p>Frame layout, big-endian:
 * <pre>
 * magic(4B) | version(1B) | type(1B) | requestId(8B) | epoch(8B)
 *           | lockName(UTF) | value(UTF) | leaseMs(8B) | success(1B) | message(UTF)
 * </pre>
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public record MutexMessage(
        MutexMessageType type,
        long requestId,
        long epoch,
        String lockName,
        String value,
        long leaseMs,
        boolean success,
        String message) {

    /** Magic number "SPCM", for SPreader CoMmons. */
    public static final int MAGIC = 0x5350434D;

    public static final byte VERSION = 1;

    /** A plausible ceiling for one lock message; anything larger is discarded as malformed. */
    private static final int MAX_BYTES = 64 * 1024;

    public static MutexMessage request(MutexMessageType type, long requestId, long epoch,
                                       String lockName, String value, long leaseMs) {
        return new MutexMessage(type, requestId, epoch, lockName, value, leaseMs, false, "");
    }

    public static MutexMessage ok(long requestId, long epoch, String lockName, String value) {
        return new MutexMessage(MutexMessageType.RESPONSE, requestId, epoch, lockName,
                value == null ? "" : value, 0L, true, "");
    }

    public static MutexMessage fail(long requestId, long epoch, String lockName, String reason) {
        return new MutexMessage(MutexMessageType.RESPONSE, requestId, epoch, lockName,
                "", 0L, false, reason);
    }

    /** Whether these bytes are a lock protocol message. */
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
            out.writeUTF(lockName == null ? "" : lockName);
            out.writeUTF(value == null ? "" : value);
            out.writeLong(leaseMs);
            out.writeBoolean(success);
            out.writeUTF(message == null ? "" : message);
        } catch (IOException e) {
            // A ByteArrayOutputStream never really throws an IOException
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    /**
     * Decodes a message.
     *
     * @return the decoded message, or null when it is not the lock protocol, the version is
     *         unrecognised, or the format is wrong
     */
    public static MutexMessage decode(byte[] content) {
        if (!matches(content)) {
            return null;
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(content))) {
            in.readInt();
            byte version = in.readByte();
            if (version != VERSION) {
                return null;
            }
            MutexMessageType type = MutexMessageType.fromCode(in.readByte());
            if (type == null) {
                return null;
            }
            return new MutexMessage(type, in.readLong(), in.readLong(), in.readUTF(),
                    in.readUTF(), in.readLong(), in.readBoolean(), in.readUTF());
        } catch (IOException e) {
            return null;
        }
    }
}
