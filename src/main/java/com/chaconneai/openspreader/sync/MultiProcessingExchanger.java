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
import java.util.concurrent.TimeoutException;

/**
 * A thin {@link ProcessingExchanger}.
 *
 * <p>The real work lives in {@link ExchangerService} -- pairing, the item left on the leader
 * for the sleeping party, resending under one requestId, and the push-plus-re-check safety
 * net. This forwards the name and the arguments.
 *
 * <p><b>It holds no local state</b>: every {@code exchange} goes by the leader's register. So
 * one instance may be called from several threads at once, each counting as one party, and
 * two of those threads may perfectly well pair with each other.
 *
 * <h2>The cast is where the type parameter stops being checked</h2>
 * The item crosses the network as bytes, so {@code V} exists only in this process. Two call
 * sites using one name with different types compile cleanly and fail at the cast, in the
 * thread that received the wrong item. That is the same bargain every serialising API makes;
 * the defence is to keep one name to one type.
 *
 * @param <V> the type of item exchanged
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 12/09/2026
 */
public class MultiProcessingExchanger<V> implements ProcessingExchanger<V> {

    private final ExchangerService service;
    private final String name;

    public MultiProcessingExchanger(ExchangerService service, String name) {
        this.service = service;
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public V exchange(V item) throws InterruptedException {
        try {
            return cast(service.exchange(name, item, -1L));
        } catch (TimeoutException e) {
            // An untimed call cannot time out; reaching here would mean the service layer's
            // semantics had changed
            throw new ProcessingExchangerException("an untimed exchange should never time out", e);
        }
    }

    @Override
    public V exchange(V item, long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException {
        return cast(service.exchange(name, item, unit.toMillis(timeout)));
    }

    @Override
    public boolean hasWaiter() {
        ExchangeMessage r = service.query(name);
        return r != null && r.success() && r.state() == SyncState.SATISFIED;
    }

    @SuppressWarnings("unchecked")
    private V cast(Object value) {
        return (V) value;
    }

    @Override
    public String toString() {
        return "ProcessingExchanger[" + name + "]";
    }
}
