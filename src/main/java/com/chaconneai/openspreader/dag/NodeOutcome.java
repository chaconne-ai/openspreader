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

import java.io.Serializable;
import java.util.Map;

/**
 * What one node reported back.
 *
 * <p>It travels from wherever the node ran to whoever started the run, so it and everything
 * in it is {@link Serializable}.
 *
 * @param node       the node's name in the graph
 * @param updates    the channels it changed. Never null; an empty map for a node with only
 *                   side effects
 * @param ok         whether it finished without throwing
 * @param failure    what it threw, when that is itself serialisable. See the note below
 * @param failureText the exception's type and message as text, always present on a failure.
 *                   This is the belt to {@code failure}'s braces
 * @param executedOn the label of the node that ran it. Purely for diagnostics, and the
 *                   quickest way to confirm that work really is being spread
 * @param millis     how long the node's own code took, excluding dispatch
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public record NodeOutcome(String node,
                          Map<String, Object> updates,
                          boolean ok,
                          Throwable failure,
                          String failureText,
                          String executedOn,
                          long millis) implements Serializable {

    private static final long serialVersionUID = 1L;

    public NodeOutcome {
        updates = updates == null ? Map.of() : updates;
    }

    public static NodeOutcome success(String node, Map<String, Object> updates,
                                      String executedOn, long millis) {
        return new NodeOutcome(node, updates, true, null, null, executedOn, millis);
    }

    /**
     * A failure.
     *
     * <p>The throwable is carried when it can be, because an application catching its own
     * exception type is worth more than any wrapper. But an exception is not guaranteed
     * serialisable: it may hold a connection, a stream or a Spring bean in a field, and then
     * the whole reply would fail to come back and the real error would be replaced by a
     * serialisation error. So the text is filled in as well, and the engine falls back to it
     * when the object could not travel.
     */
    public static NodeOutcome failure(String node, Throwable error, String executedOn,
                                      long millis) {
        String text = error.getClass().getName()
                + (error.getMessage() == null ? "" : ": " + error.getMessage());
        Throwable carried = isSerialisable(error) ? error : null;
        return new NodeOutcome(node, Map.of(), false, carried, text, executedOn, millis);
    }

    /**
     * A cheap check, not a guarantee: a class implementing Serializable may still fail on a
     * non-serialisable field. It filters out the obvious cases, and {@link #failureText}
     * covers what it misses.
     */
    private static boolean isSerialisable(Throwable error) {
        return error instanceof Serializable;
    }
}
