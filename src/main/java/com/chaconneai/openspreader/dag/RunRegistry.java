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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The runs going on right now, so that one of them can be asked to stop.
 *
 * <p>Cancelling needs a handle, and {@code invoke} does not hand one back: it blocks until
 * the run is over, which is what nearly every caller wants. So the handle lives here, under
 * the run's own identity, and {@code ProcessingDag.cancel(runId)} finds it.
 *
 * <h2>Local to the instance that started the run, and that is not a limitation</h2>
 * A run <b>exists</b> only on its coordinator: the authoritative state is in that process's
 * memory. There is nothing on another instance to cancel. An application that wants to stop a
 * run from elsewhere sends itself a message the way it would for anything else, and calls
 * this on the instance that answers.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/09/2026
 */
public class RunRegistry {

    private final Map<String, GraphRunner> running = new ConcurrentHashMap<>();

    void add(String runId, GraphRunner runner) {
        running.put(runId, runner);
    }

    void remove(String runId) {
        running.remove(runId);
    }

    /**
     * Asks a run to stop.
     *
     * @return whether there was one to ask. False for a run that has already finished, which
     *         is the ordinary race rather than an error
     */
    public boolean cancel(String runId) {
        GraphRunner runner = running.get(runId);
        if (runner == null) {
            return false;
        }
        runner.cancel();
        return true;
    }

    /** Whether this run is still going. */
    public boolean isRunning(String runId) {
        return running.containsKey(runId);
    }

    /** The identities of the runs in flight on this instance. */
    public Set<String> runIds() {
        return Collections.unmodifiableSet(running.keySet());
    }

    /** How many runs are in flight on this instance. */
    public int size() {
        return running.size();
    }
}
