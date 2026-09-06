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

import org.slf4j.Logger;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Writes the whole cache to disk, and reads it back after a restart.
 *
 * <h2>It is <b>not</b> a dual write</h2>
 * The disk is touched exactly twice: read once at startup, written once at shutdown. Not one
 * byte is written while running, so write-path latency is entirely unaffected -- quite unlike
 * "every write synchronises to external storage", which turns a 0.0005ms local write into a
 * network round trip.
 *
 * <p>The cost, stated plainly: <b>kill -9 the process or cut the machine's power, and this
 * run's data is gone</b>. That is the right trade-off for a cache -- what is wanted is "no
 * need to send every request to the database after a restart", not "nothing may ever be
 * lost". Anything that genuinely needs the latter should not live in a cache alone.
 *
 * <h2>The format is the full-synchronisation format, reused</h2>
 * {@link CacheStore#dump(int)} and {@link CacheStore#restore(List)} already serialise the four
 * data structures plus the statistics into bytes, for synchronising between nodes. A disk
 * format of its own would only bring a second copy of the encoding, and the two would drift
 * apart sooner or later -- a divergence whose symptom is "it stores and reads back, but what
 * comes back is wrong", which is extremely hard to track down.
 *
 * <h2>TTLs, though, are stored differently</h2>
 * The synchronisation format stores a TTL as <b>the milliseconds remaining</b>, which is a
 * design for crossing machines: two machines' clocks need not agree, and an absolute instant
 * would have a peer whose clock is a few seconds slow treat unexpired keys as expired.
 *
 * <p>Going to disk need not accommodate clock skew, so the <b>absolute expiry instant</b> is
 * stored -- the second parameter of {@link CacheStore#dump(int, boolean)}. However long the
 * file sits on disk makes no difference: loading compares it with the instant of loading, and
 * {@link CacheStore#restore(List, boolean)} discards whatever has expired.
 *
 * <p>Storing the milliseconds remaining is what would be awkward: "five minutes left",
 * restored as it was, becomes "five minutes from now", <b>reviving expired keys wholesale</b>,
 * and the real elapsed time would have to be worked back from the dump instant in the header.
 * Storing the absolute instant removes that step entirely.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 21/08/2026
 */
public class CachePersistence {

    /** The magic number identifying the file, which also catches being pointed at an
     *  unrelated one. */
    private static final int MAGIC = 0x53505243;   // 'S','P','R','C'

    /** The format version. Increment it on a format change; a loader meeting a version it does
     *  not know gives up rather than guessing at it. */
    private static final int VERSION = 1;

    /** The chunk size. A disk has no MTU, so a large one keeps the chunk count down. */
    private static final int CHUNK_BYTES = 4 * 1024 * 1024;

    private final Path file;
    private final Logger log;

    public CachePersistence(Path file, Logger log) {
        this.file = file;
        this.log = log;
    }

    public Path file() {
        return file;
    }

    /**
     * Writes the whole cache to disk.
     *
     * <h2>A temporary file first, then an atomic rename</h2>
     * Writing to the target file directly leaves, when the process is killed halfway through, a
     * file on disk that <b>appears to exist and is in fact truncated</b>. The next startup
     * loads it as valid data, and the truncation shows up as "an exception partway through
     * parsing" -- at best a failed startup, and at worst half the keys loaded, after which the
     * cluster synchronises that truncated content outward as the baseline.
     *
     * <p>A rename within one filesystem is atomic: either the old file is seen or the new one
     * is, and there is no intermediate state where the new file is half written.
     *
     * @param chunks   what {@link CacheStore#dump(int)} produced
     * @param keyCount the key count, written into the header purely so loading can log it
     * @return how many bytes were written
     */
    public long dump(List<byte[]> chunks, int keyCount) throws IOException {
        Path dir = file.toAbsolutePath().getParent();
        if (dir != null) {
            Files.createDirectories(dir);
        }
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");

        long bytes;
        try (OutputStream fos = Files.newOutputStream(tmp);
             // gzip: a cache holds a great deal of text and repeated structure, so the ratio
             // is usually well worth it, and the CPU goes on the shutdown path, affecting
             // nothing online
             GZIPOutputStream gz = new GZIPOutputStream(fos, 64 * 1024);
             DataOutputStream out = new DataOutputStream(gz)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            // The dump instant. TTLs are already absolute, so nothing is converted from it;
            // it is kept only so the log can say how old the file is
            out.writeLong(System.currentTimeMillis());
            out.writeInt(keyCount);
            out.writeInt(chunks.size());
            for (byte[] chunk : chunks) {
                out.writeInt(chunk.length);
                out.write(chunk);
            }
        }

        try {
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            // An atomic rename is unavailable across filesystems -- a temporary directory and
            // a data directory on different volumes, say. Falling back to an ordinary move is
            // still safer than overwriting in place; only the atomicity guarantee is lost
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
        }
        bytes = Files.size(file);
        return bytes;
    }

    /**
     * Reads it back from disk.
     *
     * @return the snapshot chunks; {@code null} when the file is absent, corrupt, or of a
     *         version this code does not know
     */
    public Loaded load() {
        if (!Files.isReadable(file)) {
            return null;
        }
        try (InputStream fis = Files.newInputStream(file);
             GZIPInputStream gz = new GZIPInputStream(fis, 64 * 1024);
             DataInputStream in = new DataInputStream(gz)) {

            if (in.readInt() != MAGIC) {
                log.warn("Cache file {} has the wrong magic number and is ignored", file);
                return null;
            }
            int version = in.readInt();
            if (version != VERSION) {
                // An unknown version is better treated as absent than forced through the
                // current format, which would read out a pile of plausible-looking rubbish
                log.warn("Cache file {} is format version {}, and only {} is understood; "
                        + "ignored", file, version, VERSION);
                return null;
            }
            long dumpedAt = in.readLong();
            int keyCount = in.readInt();
            int chunkCount = in.readInt();

            List<byte[]> chunks = new ArrayList<>(chunkCount);
            for (int i = 0; i < chunkCount; i++) {
                byte[] chunk = new byte[in.readInt()];
                in.readFully(chunk);
                chunks.add(chunk);
            }
            return new Loaded(chunks, dumpedAt, keyCount);
        } catch (IOException | RuntimeException e) {
            // Corrupt is treated as absent. A lost cache sends the application back to its
            // source, whereas loading half the data has the cluster synchronise truncated
            // content outward as the baseline, which is far worse than an empty cache
            log.warn("Cache file {} could not be read; starting with an empty cache: {}",
                    file, e.toString());
            return null;
        }
    }

    /** Deletes the file. Called after a successful load; see the note in
     *  {@link CacheService}. */
    public void delete() {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            log.debug("Failed to delete the cache file; this does not affect operation: {}",
                    e.toString());
        }
    }

    /**
     * The result of one load.
     *
     * @param chunks   the snapshot chunks, ready to hand to {@link CacheStore#restore(List)}
     * @param dumpedAt the dump instant, for logging only -- TTLs are absolute and need no
     *                 conversion
     * @param keyCount the key count at dump time, for logging only
     */
    public record Loaded(List<byte[]> chunks, long dumpedAt, int keyCount) {

        /** How long it is since the dump. For logging only. */
        public long elapsedMillis() {
            return Math.max(0L, System.currentTimeMillis() - dumpedAt);
        }
    }
}
