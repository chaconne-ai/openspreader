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
 * Turns objects into bytes and back. Task dispatch, RPC and the cluster cache all share it.
 *
 * <h2>How the two decodes differ</h2>
 * <ul>
 *   <li>{@link #decode(byte[], Class)} -- the target type <b>is known</b>. RPC always takes
 *       this path: argument types are on the method signature, which the server has, and the
 *       return type is on it too, which the client has</li>
 *   <li>{@link #decode(byte[])} -- the type is <b>not known</b> and the byte stream has to
 *       say for itself. Recursive tasks and anything else that ships a whole object graph
 *       need this one</li>
 * </ul>
 *
 * <p>Use the typed form wherever possible: it also removes Java's native serialisation's
 * greatest security problem -- nothing is instantiated merely because the byte stream asked
 * for it.
 *
 * <h2>Choosing between the two implementations</h2>
 * <b>Both are byte streams</b> and both carry type information, so {@link #decode(byte[])}
 * works under either.
 *
 * <table border="1">
 *   <caption>The trade-offs</caption>
 *   <tr><th></th><th>{@link SerializationType#JDK JDK}</th>
 *       <th>{@link SerializationType#KRYO KRYO}</th></tr>
 *   <tr><td>Extra dependency</td><td>none</td><td>Kryo</td></tr>
 *   <tr><td>Requirement</td><td>everything implements Serializable</td>
 *       <td>none, via Objenesis</td></tr>
 *   <tr><td>Size</td><td>large</td><td>smallest</td></tr>
 *   <tr><td>Speed</td><td>slow</td><td>fastest</td></tr>
 *   <tr><td>Class structures out of step</td><td>fails outright</td><td>fails outright</td></tr>
 * </table>
 *
 * <p><b>JDK by default</b>: no dependency, and it runs with nothing configured. Move to Kryo
 * once throughput matters.
 *
 * <h2>Why there is no JSON</h2>
 * There was one, removed when everything was <b>unified onto byte streams</b>. The problem
 * was never JSON itself but how it fitted this toolkit: task dispatch takes an
 * {@code Object[]} of arguments and returns an {@code Object}, so the callee has only the
 * byte stream to tell it what each element is -- and a JSON byte stream carries no type
 * information. That left it usable for RPC alone, while requiring the whole configuration,
 * validation and fallback apparatus to make way for that one exception -- producing an option
 * that would most likely trip up whoever configured it.
 *
 * <p>The cost is recorded here too, so nobody later assumes it was removed for nothing: JSON
 * was the only cross-language option and the only one tolerant of the two sides' class
 * structures being out of step. Neither applies to this project -- everything in the cluster
 * is Java, and every node is already required to run the same version with the same
 * configuration.
 *
 * <p>Where it is genuinely needed, implement one as below rather than adding it back to the
 * enum: adding it back would mean reintroducing the entire "does this serialisation support
 * untyped decoding" validation and fallback apparatus, which is precisely the cleanliness its
 * removal bought.
 *
 * <p>To implement one of your own -- Protobuf, Avro, JSON -- implement this interface and
 * register it as a bean; the auto-configuration steps aside.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface ObjectCodec {

    /** Serialises a value. null encodes to an empty array. */
    byte[] encode(Object value);

    /**
     * Deserialises to a known type.
     *
     * @param type the expected type, from a method signature or from the cache's reader
     * @return an empty array decodes to null
     */
    Object decode(byte[] data, Class<?> type);

    /**
     * No type given; the byte stream says for itself.
     *
     * <p>Both built-in implementations support it -- which is exactly what unifying onto byte
     * streams bought: a caller never has to ask whether this serialisation can manage it. A
     * codec of your own that cannot should throw {@link SerializationException} here, and
     * <b>refuse at construction time</b> rather than leaving it to runtime.
     */
    Object decode(byte[] data);

    /** Which one this is, for logs and diagnostics. */
    SerializationType type();
}
