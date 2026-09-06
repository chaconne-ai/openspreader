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
package com.chaconneai.openspreader.scheduling;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Execution statistics for cluster-scheduled tasks.
 *
 * <p>It exists mainly to answer one question: which instance actually ran this round? In a
 * multi-instance deployment, one instance's log alone makes it look as though the task
 * never ran, when in fact another instance won it.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MultiProcessingTaskStats {

    private final Map<String, Counters> tasks = new ConcurrentHashMap<>();

    void recordExecuted(String lockName, long elapsedMs) {
        Counters c = tasks.computeIfAbsent(lockName, k -> new Counters());
        c.executed.incrementAndGet();
        c.totalElapsedMs.addAndGet(elapsedMs);
        c.lastExecutedAtMs.set(System.currentTimeMillis());
    }

    void recordSkipped(String lockName) {
        tasks.computeIfAbsent(lockName, k -> new Counters()).skipped.incrementAndGet();
    }

    void recordFailed(String lockName) {
        tasks.computeIfAbsent(lockName, k -> new Counters()).failed.incrementAndGet();
    }

    /** A statistics snapshot per task. */
    public Map<String, Map<String, Object>> snapshot() {
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        tasks.forEach((name, c) -> {
            long executed = c.executed.get();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("executed", executed);
            m.put("skipped", c.skipped.get());
            m.put("failed", c.failed.get());
            m.put("avgElapsedMs", executed == 0 ? 0 : c.totalElapsedMs.get() / executed);
            m.put("lastExecutedAtMs", c.lastExecutedAtMs.get());
            result.put(name, m);
        });
        return result;
    }

    public void reset() {
        tasks.clear();
    }

    private static final class Counters {
        final AtomicLong executed = new AtomicLong();
        final AtomicLong skipped = new AtomicLong();
        final AtomicLong failed = new AtomicLong();
        final AtomicLong totalElapsedMs = new AtomicLong();
        final AtomicLong lastExecutedAtMs = new AtomicLong();
    }
}
