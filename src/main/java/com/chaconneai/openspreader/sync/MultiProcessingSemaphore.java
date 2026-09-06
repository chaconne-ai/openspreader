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
package com.chaconneai.openspreader.sync;

import java.util.concurrent.TimeUnit;

/**
 * A {@link ProcessingSemaphore}.
 *
 * <p>A thin wrapper: the local gate and the held count are both managed in
 * {@link SemaphoreService}.
 *
 * <p>Unlike {@code MultiProcessingMutex}, the local lock does not live on the instance,
 * because a change of leader requires <b>returning the permits already taken locally</b> --
 * and that has to be done by whoever receives cluster events, which is the service. Keeping
 * it on the instance would force every instance to subscribe to events itself, adding a
 * layer for nothing.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MultiProcessingSemaphore implements ProcessingSemaphore {

    private final SemaphoreService service;
    private final String name;
    private final int permits;
    private final String value;

    public MultiProcessingSemaphore(SemaphoreService service, String name, int permits, String value) {
        if (permits <= 0) {
            throw new IllegalArgumentException("a semaphore needs more than 0 permits, got " + permits);
        }
        this.service = service;
        this.name = name;
        this.permits = permits;
        this.value = value;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int permits() {
        return permits;
    }

    @Override
    public boolean tryAcquire() {
        return service.tryAcquire(name, permits, value);
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return service.acquire(name, permits, value, unit.toMillis(timeout));
    }

    @Override
    public boolean acquire(long timeout, TimeUnit unit) throws InterruptedException {
        return tryAcquire(timeout, unit);
    }

    @Override
    public boolean acquire() {
        try {
            // -1 means untimed: while leadership is vacant it keeps retrying, waiting for a
            // new leader to take over
            return service.acquire(name, permits, value, -1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void release() {
        service.release(name);
    }

    @Override
    public int availablePermits() {
        return service.availablePermits(name, permits);
    }

    @Override
    public int heldPermits() {
        return service.heldPermits(name);
    }

    @Override
    public String toString() {
        return "ProcessingSemaphore{" + name + ", permits=" + permits
                + ", held=" + heldPermits() + '}';
    }
}
