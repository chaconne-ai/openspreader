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
package com.chaconneai.openspreader.dag;

/**
 * How one channel's value absorbs an incoming update.
 *
 * <p>This is the whole reason state is a map of channels rather than one object: with a
 * reducer per channel, two branches running in parallel and touching different channels
 * cannot conflict at all, and two branches touching the <b>same</b> channel have to say
 * what happens, in writing, before the graph is allowed to run.
 *
 * <pre>{@code
 * .channel("items", (current, incoming) -> {
 *     List<String> merged = new ArrayList<>(current == null ? List.of() : current);
 *     merged.addAll(incoming);
 *     return merged;
 * })
 * }</pre>
 *
 * <h2>It runs on the coordinator, and only there</h2>
 * So it may be a lambda, a method reference, anything at all: it is never serialised and
 * never leaves the process that started the run. Merging is by definition the job of
 * whoever holds the authoritative state.
 *
 * <h2>It must not care about arrival order</h2>
 * Updates from parallel branches are merged <b>in node-name order</b>, not in the order
 * they came back, so that one graph over one input gives one answer. A reducer that is
 * associative and commutative needs nothing further; one that is not gets a deterministic
 * order anyway, which is the next best thing.
 *
 * @param <T> the channel's value type
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
@FunctionalInterface
public interface Reducer<T> {

    /**
     * @param current  what the channel holds now, or {@code null} when nothing has been
     *                 written to it yet. <b>Every reducer has to handle null here</b>, which
     *                 is why {@link Reducers} exists rather than leaving everyone to write
     *                 the same null check
     * @param incoming what a node just returned for this channel
     * @return the channel's new value
     */
    T reduce(T current, T incoming);

    /**
     * What this reducer is called, or null when it has no name.
     *
     * <p>It exists for one reason: a graph that is <b>written out and read back</b> cannot
     * carry a lambda. A named reducer is stored as its name and looked up again through a
     * {@link GraphCatalog}; an anonymous one is stored as null, and loading a graph that
     * needs it fails with a message saying which channel is the problem.
     *
     * <p>Every factory on {@link Reducers} names itself, so the built-in ones round-trip. A
     * reducer of your own does too, if it says what it is called:
     *
     * <pre>{@code
     * Reducer<Money> summed = new Reducer<>() {
     *     public Money reduce(Money a, Money b) { return a == null ? b : a.plus(b); }
     *     public String name() { return "sumMoney"; }
     * };
     * }</pre>
     */
    default String name() {
        return null;
    }

    /** The same reducer under a name, so that it survives being written out and read back. */
    static <T> Reducer<T> named(String name, Reducer<T> delegate) {
        return new Reducer<T>() {
            @Override
            public T reduce(T current, T incoming) {
                return delegate.reduce(current, incoming);
            }

            @Override
            public String name() {
                return name;
            }
        };
    }
}
