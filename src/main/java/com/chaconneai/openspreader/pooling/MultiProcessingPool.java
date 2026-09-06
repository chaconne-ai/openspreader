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
