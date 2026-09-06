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
