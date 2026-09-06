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
