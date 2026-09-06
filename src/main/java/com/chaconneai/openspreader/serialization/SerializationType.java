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
package com.chaconneai.openspreader.serialization;

/**
 * How objects are serialised. <b>Both produce byte streams.</b>
 *
 * <p>Configured by {@code spring.spreader.multiprocessing.serialization}. Task dispatch
 * ({@code @MultiProcessingCall}), RPC and the cluster cache <b>share one setting and one
 * implementation</b> -- they are all solving the same problem: turning an object into
 * bytes to send to another process.
 *
 * <p><b>The whole cluster must be configured the same way.</b> The caller encodes by this
 * setting and the callee decodes by its own, so a mismatch is a pile of undecodable bytes.
 * For a rolling upgrade, either change the configuration everywhere before the code, or
 * take the cluster down to switch.
 *
 * <p>{@link ObjectCodec} has the comparison table for choosing between the two, and also
 * explains <b>why there is no JSON</b> and what to do when you genuinely need it.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public enum SerializationType {

    /**
     * Java's native serialisation. The default, with no dependency.
     *
     * <p>Requires arguments and return values to implement {@code Serializable}.
     */
    JDK,

    /**
     * Kryo, requiring {@code com.esotericsoftware:kryo}.
     *
     * <p>The smallest and the fastest. Requires classes to have a no-argument constructor.
     */
    KRYO
}
