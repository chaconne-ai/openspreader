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

/**
 * The output of the {@code map} phase: emits one intermediate key-value pair.
 *
 * <p>Why a callback rather than having {@code map} return a collection: the intermediate
 * results can far outnumber the input. One line of text emits a dozen words, a shard holds
 * hundreds of thousands of lines, and gathering them into a List before returning would put
 * millions of objects on the heap at once. A callback lets them be <b>sent as they are
 * produced</b>.
 *
 * <p>The implementation is <b>thread-safe</b>, though nothing requires you to call it
 * concurrently -- one shard's {@code map} runs on a single thread.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
@FunctionalInterface
public interface Emitter<K, V> {

    /**
     * Emits one intermediate result.
     *
     * <h2>A key must satisfy two conditions, and they concern <b>different methods</b></h2>
     * <table border="1">
     *   <caption>What a key must provide</caption>
     *   <tr><th>Used for</th><th>Requirement</th><th>Why</th></tr>
     *   <tr><td>routing to a reducer</td>
     *       <td>{@code toString()} <b>stable across JVMs</b></td>
     *       <td>consistent hashing computes {@code hash(String.valueOf(key))},
     *           <b>not hashCode</b></td></tr>
     *   <tr><td>merging into the final result</td>
     *       <td>correct {@code equals} and {@code hashCode}</td>
     *       <td>the result is a {@code Map}</td></tr>
     * </table>
     *
     * <p>{@code String}, the numeric types and {@code Enum} all qualify -- and <b>an enum is
     * the counter-intuitive case</b>: its {@code hashCode()} is an identity hash and differs
     * between JVMs, but routing looks at {@code toString()}, that is {@code name()}, so using
     * one as a key is safe.
     *
     * <h2>Do not use arrays -- and this mistake <b>cannot be detected</b></h2>
     * An array's {@code toString()} is something like {@code [I@1b6d3586}, carrying an object
     * address, so identical contents route to different places on two nodes.
     *
     * <p>What makes it worse is that it <b>does not trip the framework's key-collision
     * check</b>: after deserialisation the arrays on each node are different objects and are
     * not {@code equals}, so in the final result they appear as <b>several distinct keys</b>
     * rather than as a collision. The outcome is a Map that looks perfectly normal and from
     * which {@code result.get(yourArray)} returns nothing.
     *
     * <p>The same applies to your own value objects: <b>both</b> {@code toString()} and
     * {@code equals} must be implemented correctly. A record satisfies this naturally, since
     * both are generated from its fields.
     *
     * @param key   the intermediate key; must not be null
     * @param value the intermediate value; must be serialisable
     */
    void emit(K key, V value);
}
