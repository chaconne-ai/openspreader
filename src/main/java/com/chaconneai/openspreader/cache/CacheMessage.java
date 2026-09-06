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
package com.chaconneai.openspreader.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A message of the cache protocol, travelling on the dedicated
 * {@link CacheService#CHANNEL}.
 *
 * <p>Frame layout, big-endian:
 * <pre>
 * magic(4B) | version(1B) | type(1B) | requestId(8B) | epoch(8B) | seq(8B)
 *           | op(1B) | key(UTF) | field(UTF) | arg(8B) | extra(8B)
 *           | value(4B length + content; a length of -1 means null)
 *           | success(1B) | message(UTF)
 * </pre>
 *
 * <h2>Several fields mean different things per type</h2>
 * <table border="1">
 *   <caption>What each field carries</caption>
 *   <tr><th></th><th>seq</th><th>arg</th><th>extra</th><th>value</th></tr>
 *   <tr><td>WRITE</td><td>--</td><td>TTL or increment</td><td>--</td>
 *       <td>the value to write</td></tr>
 *   <tr><td>RESPONSE</td><td>this operation's version</td><td>numeric result</td>
 *       <td>boolean result, 0 or 1</td><td>byte result</td></tr>
 *   <tr><td>UPDATE</td><td>version</td><td>TTL or increment</td><td>--</td>
 *       <td>the value to write</td></tr>
 *   <tr><td>SYNC</td><td>--</td><td>--</td><td>--</td><td>--</td></tr>
 *   <tr><td>SNAPSHOT</td><td>the version the snapshot is of</td><td>chunk index</td>
 *       <td>chunk count</td><td>this chunk's data</td></tr>
 * </table>
 *
 * <p>Reusing two longs for different meanings is admittedly inelegant, but far less work
 * than a message class per type -- and the table is right here.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public record CacheMessage(
        CacheMessageType type,
        long requestId,
        long epoch,
        long seq,
        CacheOp op,
        String key,
        String field,
        long arg,
        long extra,
        byte[] value,
        boolean success,
        String message) {

    /** Magic number "SPCC", for SPreader Commons Cache. */
    public static final int MAGIC = 0x53504343;

    public static final byte VERSION = 1;

    private static final byte NO_OP = 0;

    public static CacheMessage write(long requestId, CacheOp op, String key, String field,
                                     byte[] value, long arg) {
        return new CacheMessage(CacheMessageType.WRITE, requestId, 0L, 0L,
                op, key, field, arg, 0L, value, false, "");
    }

    public static CacheMessage update(long epoch, long seq, CacheOp op, String key, String field,
                                      byte[] value, long arg) {
        return new CacheMessage(CacheMessageType.UPDATE, 0L, epoch, seq,
                op, key, field, arg, 0L, value, true, "");
    }

    /**
     * The response carries the operation back, so the requester can apply the same one
     * locally; see {@link CacheMessageType#RESPONSE}.
     *
     * <p>Note that {@code arg} holds the numeric <b>result</b> -- the value after incr, the
     * length after push, the number of keys clear removed -- and not the arg from the
     * request. Echoing the request's arg would make {@code incr} on a follower return the
     * increment rather than the new value, and since the leader takes a direct path that
     * never comes through here, a mistake like that is very hard to notice.
     */
    public static CacheMessage ok(CacheMessage request, long epoch, long seq,
                                  CacheStore.Result result) {
        return new CacheMessage(CacheMessageType.RESPONSE, request.requestId(), epoch, seq,
                request.op(), request.key(), request.field(), result.number(),
                result.flag() ? 1L : 0L, result.bytes(), true, "");
    }

    public static CacheMessage fail(long requestId, long epoch, String reason) {
        return new CacheMessage(CacheMessageType.RESPONSE, requestId, epoch, 0L,
                null, "", "", 0L, 0L, null, false, reason);
    }

    /** Reports read accesses; see {@link CacheMessageType#ACCESS}. */
    public static CacheMessage access(Collection<String> keys) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(keys.size() * 24);
        try (DataOutputStream out = new DataOutputStream(bos)) {
            for (String k : keys) {
                out.writeUTF(k);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new CacheMessage(CacheMessageType.ACCESS, 0L, 0L, 0L,
                null, "", "", keys.size(), 0L, bos.toByteArray(), true, "");
    }

    /** @return the reported keys, or an empty list when they will not decode. Losing them is
     *          harmless -- eviction does not become incorrect because of it */
    public List<String> decodeAccessKeys() {
        int count = (int) arg;
        if (value == null || count <= 0) {
            return List.of();
        }
        List<String> out = new ArrayList<>(count);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(value))) {
            for (int i = 0; i < count; i++) {
                out.add(in.readUTF());
            }
        } catch (IOException e) {
            return out;
        }
        return out;
    }

    /** The leader announcing its epoch; see {@link CacheMessageType#EPOCH}. */
    public static CacheMessage epochAnnounce(long epoch, long seq) {
        return new CacheMessage(CacheMessageType.EPOCH, 0L, epoch, seq,
                null, "", "", 0L, 0L, null, true, "");
    }

    public static CacheMessage sync(long requestId) {
        return new CacheMessage(CacheMessageType.SYNC, requestId, 0L, 0L,
                null, "", "", 0L, 0L, null, false, "");
    }

    public static CacheMessage snapshot(long requestId, long epoch, long seq,
                                        int index, int total, byte[] chunk) {
        return new CacheMessage(CacheMessageType.SNAPSHOT, requestId, epoch, seq,
                null, "", "", index, total, chunk, true, "");
    }

    /**
     * Packs a batch of updates into one frame.
     *
     * <p>{@code arg} is the count, {@code seq} is the first entry's version -- present only
     * to make the log readable, since what actually takes effect is the version each entry
     * carries -- and {@code value} is the packed content.
     */
    public static CacheMessage batch(long epoch, List<Entry> entries) {
        return new CacheMessage(CacheMessageType.BATCH, 0L, epoch, entries.get(0).seq(),
                null, "", "", entries.size(), 0L, encodeEntries(entries), true, "");
    }

    /**
     * One entry within a batch.
     *
     * <p>The version is carried <b>by each entry</b> rather than derived from "first version
     * plus index". What those few extra bytes buy is that a missing entry, or an entry out of
     * order, is noticed by the receiver immediately -- instead of every operation after it
     * being applied silently at the wrong offset.
     */
    public record Entry(long epoch, long seq, CacheOp op, String key, String field,
                        byte[] value, long arg) {
    }

    public static byte[] encodeEntries(List<Entry> entries) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(64 * entries.size());
        try (DataOutputStream out = new DataOutputStream(bos)) {
            for (Entry e : entries) {
                out.writeLong(e.seq());
                out.writeByte(e.op().code());
                out.writeUTF(e.key() == null ? "" : e.key());
                out.writeUTF(e.field() == null ? "" : e.field());
                out.writeLong(e.arg());
                if (e.value() == null) {
                    out.writeInt(-1);
                } else {
                    out.writeInt(e.value().length);
                    out.write(e.value());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    /** @return null when it will not decode. Applying half a batch would fork the replica, so
     *          the whole batch is dropped and a full snapshot fetched instead */
    public List<Entry> decodeEntries() {
        int count = (int) arg;
        if (value == null || count <= 0) {
            return List.of();
        }
        List<Entry> out = new ArrayList<>(count);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(value))) {
            for (int i = 0; i < count; i++) {
                long seq = in.readLong();
                CacheOp op = CacheOp.fromCode(in.readByte());
                if (op == null) {
                    return null;
                }
                String key = in.readUTF();
                String field = in.readUTF();
                long entryArg = in.readLong();
                int len = in.readInt();
                byte[] v = null;
                if (len >= 0) {
                    v = new byte[len];
                    in.readFully(v);
                }
                out.add(new Entry(epoch, seq, op, key, field, v, entryArg));
            }
        } catch (IOException | NegativeArraySizeException e) {
            return null;
        }
        return out;
    }

    /** The numeric result; see the field table in the class documentation. */
    public long numberResult() {
        return arg;
    }

    public boolean booleanResult() {
        return extra != 0L;
    }

    public byte[] encode() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(
                128 + (value == null ? 0 : value.length));
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(MAGIC);
            out.writeByte(VERSION);
            out.writeByte(type.code());
            out.writeLong(requestId);
            out.writeLong(epoch);
            out.writeLong(seq);
            out.writeByte(op == null ? NO_OP : op.code());
            out.writeUTF(key == null ? "" : key);
            out.writeUTF(field == null ? "" : field);
            out.writeLong(arg);
            out.writeLong(extra);
            if (value == null) {
                out.writeInt(-1);
            } else {
                out.writeInt(value.length);
                out.write(value);
            }
            out.writeBoolean(success);
            out.writeUTF(message == null ? "" : message);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    /** @return null when it is not a cache message, the version does not match, or the format
     *          is corrupt */
    public static CacheMessage decode(byte[] content) {
        if (content == null || content.length < 6) {
            return null;
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(content))) {
            if (in.readInt() != MAGIC || in.readByte() != VERSION) {
                return null;
            }
            CacheMessageType type = CacheMessageType.fromCode(in.readByte());
            if (type == null) {
                return null;
            }
            long requestId = in.readLong();
            long epoch = in.readLong();
            long seq = in.readLong();
            byte opCode = in.readByte();
            CacheOp op = opCode == NO_OP ? null : CacheOp.fromCode(opCode);
            if (opCode != NO_OP && op == null) {
                // The peer used an opcode this side does not recognise, almost always a version
                // mismatch. Dropping it beats executing something arbitrary
                return null;
            }
            String key = in.readUTF();
            String field = in.readUTF();
            long arg = in.readLong();
            long extra = in.readLong();
            int len = in.readInt();
            byte[] value = null;
            if (len >= 0) {
                value = new byte[len];
                in.readFully(value);
            }
            return new CacheMessage(type, requestId, epoch, seq, op, key, field,
                    arg, extra, value, in.readBoolean(), in.readUTF());
        } catch (IOException | NegativeArraySizeException | OutOfMemoryError e) {
            return null;
        }
    }
}
