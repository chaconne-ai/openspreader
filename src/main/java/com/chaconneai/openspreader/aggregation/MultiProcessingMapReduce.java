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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * {@link ProcessingMapReduce} implemented on top of {@link MapReduceService}.
 *
 * <p>This layer is a generic facade and nothing more; all state and protocol live in the
 * service. They are separate so the interface injected into application code has
 * <b>three methods</b>. The service also carries message handling, the state machines for
 * three roles, and statistics -- framework business that has no place in an application's
 * autocomplete.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 23/08/2026
 */
public class MultiProcessingMapReduce implements ProcessingMapReduce {

    private final MapReduceService service;

    public MultiProcessingMapReduce(MapReduceService service) {
        this.service = service;
    }

    @Override
    public <IN, K, V, R> CompletableFuture<MapReduceResult<K, R>> submit(String jobBeanName, IN input) {
        return service.submit(jobBeanName, input, 0L);
    }

    @Override
    public <IN, K, V, R> CompletableFuture<MapReduceResult<K, R>> submit(String jobBeanName,
            IN input, long timeout, TimeUnit unit) {
        return service.submit(jobBeanName, input, unit.toMillis(timeout));
    }

    @Override
    public int runningJobs() {
        return service.runningJobs();
    }

    @Override
    public String toString() {
        return "MultiProcessingMapReduce{channel=" + MapReduceService.CHANNEL + '}';
    }
}
