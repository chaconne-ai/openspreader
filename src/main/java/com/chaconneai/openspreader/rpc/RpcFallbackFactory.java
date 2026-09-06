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
package com.chaconneai.openspreader.rpc;

/**
 * Builds a fallback implementation from the reason it failed.
 *
 * <pre>{@code
 * @Component
 * public class OrderFallbackFactory implements RpcFallbackFactory<OrderApi> {
 *
 *     @Override
 *     public OrderApi create(Throwable cause) {
 *         if (cause instanceof RpcException) {
 *             log.warn("Order service unavailable; returning an empty order: {}", cause.getMessage());
 *         } else {
 *             log.error("Order service threw", cause);
 *         }
 *         return id -> Order.EMPTY;
 *     }
 * }
 * }</pre>
 *
 * <p>What it adds over {@link RpcClient#fallback()} is the {@code cause}: "the peer is not
 * up" and "the peer's business logic threw" usually deserve different treatment, and the
 * return value alone cannot tell them apart.
 *
 * @param <T> the interface being fallen back on
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@FunctionalInterface
public interface RpcFallbackFactory<T> {

    /**
     * Builds a fallback implementation.
     *
     * @param cause why the last attempt failed; never null
     * @return the fallback, or null to decline falling back and let {@code cause} reach the
     *         caller
     */
    T create(Throwable cause);
}
