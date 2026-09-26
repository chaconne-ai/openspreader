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

/**
 * The 128-bit x64 variant of MurmurHash3.
 *
 * <h2>Why it is written here</h2>
 * The reference implementation is Guava's {@code Hashing.murmur3_128()}. Pulling the whole
 * of Guava in for a few dozen lines of hash function is a poor trade -- every other optional
 * dependency of this library (Jackson, Kryo, spring-retry) is added only to use a
 * particular feature, and the Bloom filter should not be the one that forces a dependency.
 *
 * <h2>Why this hash</h2>
 * What a Bloom filter needs from a hash is <b>even distribution</b>, not collision
 * resistance -- it serves no security purpose, so a cryptographic hash like SHA is pure
 * waste. MurmurHash3 is the standard choice here: good avalanche behaviour, fast, and
 * implemented in almost every language, so <b>recomputing it elsewhere gives the same
 * answer</b>.
 *
 * <p>It yields 128 bits, exactly what {@link ProcessingBloomFilter} needs to derive k
 * positions by double hashing rather than actually hashing k times.
 *
 * <p><b>Do not change the constants or the shifts here.</b> They were not chosen freely:
 * change one and it is no longer MurmurHash3, and every bit in existing data stops lining
 * up -- the Bloom filter would start returning false negatives.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public final class MurmurHash3 {

    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;

    private MurmurHash3() {
    }

    /**
     * Computes the 128-bit hash.
     *
     * @return an array of two, {@code [h1, h2]}
     */
    public static long[] hash128(byte[] data, int seed) {
        final int length = data.length;
        final int nblocks = length >> 4;

        long h1 = seed & 0xFFFFFFFFL;
        long h2 = seed & 0xFFFFFFFFL;

        // The body: 16 bytes at a time
        for (int i = 0; i < nblocks; i++) {
            final int base = i << 4;
            long k1 = readLong(data, base);
            long k2 = readLong(data, base + 8);

            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            h1 ^= k1;

            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            h2 ^= k2;

            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        // The tail: whatever is left of the final 16 bytes
        long k1 = 0;
        long k2 = 0;
        final int tail = nblocks << 4;
        switch (length & 15) {
            case 15: k2 ^= ((long) data[tail + 14] & 0xff) << 48;
            case 14: k2 ^= ((long) data[tail + 13] & 0xff) << 40;
            case 13: k2 ^= ((long) data[tail + 12] & 0xff) << 32;
            case 12: k2 ^= ((long) data[tail + 11] & 0xff) << 24;
            case 11: k2 ^= ((long) data[tail + 10] & 0xff) << 16;
            case 10: k2 ^= ((long) data[tail + 9] & 0xff) << 8;
            case 9:
                k2 ^= (long) data[tail + 8] & 0xff;
                k2 *= C2;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= C1;
                h2 ^= k2;
                // fall through
            case 8: k1 ^= ((long) data[tail + 7] & 0xff) << 56;
            case 7: k1 ^= ((long) data[tail + 6] & 0xff) << 48;
            case 6: k1 ^= ((long) data[tail + 5] & 0xff) << 40;
            case 5: k1 ^= ((long) data[tail + 4] & 0xff) << 32;
            case 4: k1 ^= ((long) data[tail + 3] & 0xff) << 24;
            case 3: k1 ^= ((long) data[tail + 2] & 0xff) << 16;
            case 2: k1 ^= ((long) data[tail + 1] & 0xff) << 8;
            case 1:
                k1 ^= (long) data[tail] & 0xff;
                k1 *= C1;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= C2;
                h1 ^= k1;
                break;
            default:
                break;
        }

        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        h2 += h1;

        return new long[] { h1, h2 };
    }

    /** Reads 8 bytes little-endian. The byte order must be fixed, or the same data would
     *  hash to different bits on different architectures. */
    private static long readLong(byte[] data, int offset) {
        return ((long) data[offset] & 0xff)
                | (((long) data[offset + 1] & 0xff) << 8)
                | (((long) data[offset + 2] & 0xff) << 16)
                | (((long) data[offset + 3] & 0xff) << 24)
                | (((long) data[offset + 4] & 0xff) << 32)
                | (((long) data[offset + 5] & 0xff) << 40)
                | (((long) data[offset + 6] & 0xff) << 48)
                | (((long) data[offset + 7] & 0xff) << 56);
    }

    /** The final mix, spreading entropy from the high bits down to the low ones. Without it
     *  the low bits are distributed poorly. */
    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }
}
