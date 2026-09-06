package com.chaconneai.openspreader.pooling;

import java.util.concurrent.CompletableFuture;

/**
 * The default {@link ProcessingPool}, forwarding calls to {@link PoolService}.
 *
 * <p>This layer is thin: target selection, serialisation, transport and the local path for
 * a single process all live in the service. They are separate so that users see two
 * methods, {@code submit} and {@code execute}, and never have to face the communication
 * details.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MultiProcessingPool implements ProcessingPool {

    private final PoolService service;

    public MultiProcessingPool(PoolService service) {
        this.service = service;
    }

    @Override
    public <T> CompletableFuture<T> submit(String className, String beanName,
                                           String methodName, Object[] args) {
        return service.submitCall(className, beanName, methodName, args);
    }

    @Override
    public void execute(String className, String beanName, String methodName, Object[] args) {
        service.fireAndForget(className, beanName, methodName, args);
    }

    @Override
    public int peerCount() {
        return service.peerCount();
    }

    @Override
    public PoolStats poolStats() {
        return service.poolStats();
    }

    @Override
    public String toString() {
        return "MultiProcessingPool{peers=" + peerCount() + '}';
    }
}
