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
package com.chaconneai.openspreader.bloom;

import com.chaconneai.openspreader.cache.ProcessingCache;

import java.nio.charset.StandardCharsets;

/**
 * {@link ProcessingBloomFilter} implemented on {@link ProcessingCache}'s bit operations.
 *
 * <p>Whether it suits your case, how to choose the parameters, and the false negatives
 * possible under a partition are all documented on {@link ProcessingBloomFilter}, which is
 * what a user should read. This class only computes the bit positions and sends the
 * setbit/getbit calls.
 *
 * <p>Construct it through {@link ProcessingBloomFilter#create} rather than {@code new}:
 * argument validation and the derivation of bit count and hash count live there.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MultiProcessingBloomFilter implements ProcessingBloomFilter {

    private final ProcessingCache cache;
    private final String key;
    private final long bitSize;
    private final int hashCount;

    protected MultiProcessingBloomFilter(ProcessingCache cache, String key, long bitSize, int hashCount) {
        this.cache = cache;
        this.key = key;
        this.bitSize = bitSize;
        this.hashCount = hashCount;
    }

    @Override
    public boolean put(String value) {
        long[] offsets = offsets(value);
        boolean changed = false;
        for (long offset : offsets) {
            // setbit returns the bit's previous value, so a previous 0 means this call
            // really did change it
            if (!cache.setbit(key, offset, true)) {
                changed = true;
            }
        }
        return changed;
    }

    @Override
    public boolean mightContain(String value) {
        for (long offset : offsets(value)) {
            if (!cache.getbit(key, offset)) {
                // A single 0 bit is proof it was never put
                return false;
            }
        }
        return true;
    }

    @Override
    public void clear() {
        cache.delete(key);
    }

    @Override
    public long bitSize() {
        return bitSize;
    }

    @Override
    public int hashCount() {
        return hashCount;
    }

    @Override
    public String key() {
        return key;
    }

    /**
     * Derives k positions by double hashing.
     *
     * <p>One 128-bit hash is computed, split into two 64-bit halves, and the positions
     * derived as {@code h1 + i * h2}. This is not a shortcut: Kirsch and Mitzenmacher
     * proved that positions derived by double hashing are <b>equivalent in false-positive
     * rate to k genuinely independent hashes</b>, at the cost of one hash.
     */
    protected long[] offsets(String value) {
        if (value == null) {
            throw new IllegalArgumentException("a value put into a Bloom filter must not be null");
        }
        long[] hash = MurmurHash3.hash128(value.getBytes(StandardCharsets.UTF_8), 0);
        long h1 = hash[0];
        long h2 = hash[1];
        long[] offsets = new long[hashCount];
        long combined = h1;
        for (int i = 0; i < hashCount; i++) {
            // Math.floorMod rather than %: the latter returns a negative for a negative
            // input, which would become a negative bit offset and make setbit throw
            offsets[i] = Math.floorMod(combined, bitSize);
            combined += h2;
        }
        return offsets;
    }

    @Override
    public String toString() {
        return "MultiProcessingBloomFilter{key=" + key + ", bits=" + bitSize
                + ", hashes=" + hashCount + '}';
    }
}
