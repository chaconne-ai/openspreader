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
package com.chaconneai.openspreader.aggregation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A message of the distributed aggregation protocol.
 *
 * <p>All three message types share one structure and each fills only the fields it needs,
 * so there is a single codec rather than three sets of read/write orders to maintain.
 * Unused fields are the empty string, an empty array or 0 -- a few dozen bytes, in exchange
 * for "adding a field means changing one place".
 *
 * @param jobId         this job's number, unique globally
 * @param jobBeanName   the job's bean name. It is how a receiving node finds the same job
 *                      implementation
 * @param shardIndex    which shard this is. <b>It doubles as the mapper's identity</b>: a
 *                      reducer de-duplicates the shards it receives by it, so a
 *                      retransmission at the message layer cannot count one shard twice
 * @param shardCount    how many shards there are in total. A reducer is complete once it has
 *                      that many
 * @param reducerIds    this round's reducer roster, <b>fixed by the submitter and sent with
 *                      the task</b>. Every mapper must hash against the same roster, or one
 *                      key lands on different nodes and the result is wrong
 * @param coordinatorId the submitter's node id, where reducers send their results
 * @param payload       the serialised data: shard content, a batch of intermediate pairs, or
 *                      a partial reduction
 * @param success       used only by {@link MapReduceMessageType#REDUCE_RESULT}
 * @param message       why it failed
 * @param emitted       SHUFFLE only ({@link MapReduceMessageType#SHUFFLE}): how many records
 *                      this shard's {@code map} emitted in total
 * @param shuffled      SHUFFLE only: how many remain <b>after</b> this shard's local
 *                      combining. Its ratio to {@code emitted} is the combiner's effect
 * @param combineCalls  SHUFFLE only: how many times this shard called combine
 * @param phaseNanos    SHUFFLE only: how long this shard's {@code map}, combining included,
 *                      took
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
public record MapReduceMessage(
        MapReduceMessageType type,
        long jobId,
        String jobBeanName,
        int shardIndex,
        int shardCount,
        List<String> reducerIds,
        String coordinatorId,
        byte[] payload,
        boolean success,
        String message,
        long emitted,
        long shuffled,
        long combineCalls,
        long phaseNanos) {

    /** Magic number "SPMR", for SPreader MapReduce. */
    public static final int MAGIC = 0x53504D52;

    public static final byte VERSION = 1;

    private static final byte[] NO_PAYLOAD = new byte[0];

    public MapReduceMessage {
        payload = payload == null ? NO_PAYLOAD : payload;
        reducerIds = reducerIds == null ? List.of() : List.copyOf(reducerIds);
        jobBeanName = jobBeanName == null ? "" : jobBeanName;
        coordinatorId = coordinatorId == null ? "" : coordinatorId;
        message = message == null ? "" : message;
    }

    public static MapReduceMessage mapTask(long jobId, String jobBeanName, int shardIndex,
                                           int shardCount, List<String> reducerIds,
                                           String coordinatorId, byte[] payload) {
        return new MapReduceMessage(MapReduceMessageType.MAP_TASK, jobId, jobBeanName,
                shardIndex, shardCount, reducerIds, coordinatorId, payload, false, "",
                0L, 0L, 0L, 0L);
    }

    /**
     * <b>Every</b> intermediate result one shard has for one reducer, with nothing following
     * it.
     *
     * <p>The shard is accumulated in full and then sent, rather than sent as it is computed.
     * One message per record would make the message count {@code shards x keys}, and would
     * reintroduce the problem of deciding whether the last one has arrived. The price is that
     * a shard's intermediate results are held in memory, and are subject to the message size
     * limit.
     *
     * <p>The four descriptive fields ride on every message, because this <b>may be the only
     * message a given reducer receives</b> -- no other shard having any key that lands on it
     * -- and it is the only place it can learn how many shards to expect, which job to reduce
     * with, and where to send the result.
     *
     * @param coordinatorId not merely a return address but <b>part of the identity</b>: two
     *                      nodes may submit jobs at the same moment and each be assigned
     *                      {@code jobId=1}, and only {@code (coordinatorId, jobId)} tells a
     *                      reducer them apart
     */
    public static MapReduceMessage shuffle(long jobId, String jobBeanName, int shardIndex,
                                           int shardCount, String coordinatorId, byte[] payload,
                                           long emitted, long shuffled, long combineCalls,
                                           long phaseNanos) {
        return new MapReduceMessage(MapReduceMessageType.SHUFFLE, jobId, jobBeanName,
                shardIndex, shardCount, List.of(), coordinatorId, payload, false, "",
                emitted, shuffled, combineCalls, phaseNanos);
    }

    public static MapReduceMessage reduceResult(long jobId, byte[] payload) {
        return new MapReduceMessage(MapReduceMessageType.REDUCE_RESULT, jobId, "",
                0, 0, List.of(), "", payload, true, "", 0L, 0L, 0L, 0L);
    }

    public static MapReduceMessage reduceFailed(long jobId, String reason) {
        return new MapReduceMessage(MapReduceMessageType.REDUCE_RESULT, jobId, "",
                0, 0, List.of(), "", NO_PAYLOAD, false, reason, 0L, 0L, 0L, 0L);
    }

    /** Whether these bytes are an aggregation protocol message. */
    public static boolean matches(byte[] content) {
        if (content == null || content.length < 6) {
            return false;
        }
        int magic = ((content[0] & 0xFF) << 24) | ((content[1] & 0xFF) << 16)
                | ((content[2] & 0xFF) << 8) | (content[3] & 0xFF);
        return magic == MAGIC;
    }

    public byte[] encode() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(payload.length + 256);
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(MAGIC);
            out.writeByte(VERSION);
            out.writeByte(type.code());
            out.writeLong(jobId);
            out.writeUTF(jobBeanName);
            out.writeInt(shardIndex);
            out.writeInt(shardCount);
            out.writeInt(reducerIds.size());
            for (String id : reducerIds) {
                out.writeUTF(id);
            }
            out.writeUTF(coordinatorId);
            out.writeInt(payload.length);
            out.write(payload);
            out.writeBoolean(success);
            out.writeUTF(message);
            out.writeLong(emitted);
            out.writeLong(shuffled);
            out.writeLong(combineCalls);
            out.writeLong(phaseNanos);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    /** @return the decoded message, or null when it is not this protocol, the version is
     *          unrecognised, or the format is wrong */
    public static MapReduceMessage decode(byte[] content) {
        if (!matches(content)) {
            return null;
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(content))) {
            in.readInt();
            if (in.readByte() != VERSION) {
                return null;
            }
            MapReduceMessageType type = MapReduceMessageType.fromCode(in.readByte());
            if (type == null) {
                return null;
            }
            long jobId = in.readLong();
            String jobBeanName = in.readUTF();
            int shardIndex = in.readInt();
            int shardCount = in.readInt();
            int idCount = in.readInt();
            // Validate the length against the message itself first: one corrupt int would
            // have the line below allocate an enormous array
            if (idCount < 0 || idCount > content.length) {
                return null;
            }
            List<String> reducerIds = new ArrayList<>(idCount);
            for (int i = 0; i < idCount; i++) {
                reducerIds.add(in.readUTF());
            }
            String coordinatorId = in.readUTF();
            int len = in.readInt();
            if (len < 0 || len > content.length) {
                return null;
            }
            byte[] payload = new byte[len];
            in.readFully(payload);
            boolean success = in.readBoolean();
            String message = in.readUTF();
            return new MapReduceMessage(type, jobId, jobBeanName, shardIndex, shardCount,
                    reducerIds, coordinatorId, payload, success, message,
                    in.readLong(), in.readLong(), in.readLong(), in.readLong());
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "MapReduceMessage{" + type + ", job=" + jobId + ", shard=" + shardIndex
                + "/" + shardCount + ", payload=" + payload.length + "B}";
    }
}
