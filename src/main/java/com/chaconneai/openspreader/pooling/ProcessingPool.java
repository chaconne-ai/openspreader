package com.chaconneai.openspreader.pooling;

import java.util.concurrent.CompletableFuture;

/**
 * A cross-process task pool: hands one method call to some process in the cluster to run.
 *
 * <pre>{@code
 * // Wanting the result: a future, possibly computed in another process
 * String report = pool.<String>submit("reportService", "render", "2026-08")
 *                     .get(30, TimeUnit.SECONDS);
 *
 * // Not wanting the result: sent and forgotten
 * pool.execute("cacheWarmer", "warm", "hot-key");
 * }</pre>
 *
 * <h2>The target method must be exposed</h2>
 * The method being called has to be annotated {@link MultiProcessingCall}, or the request is
 * refused. That is enforced, not advised: without an allow-list, any node in the cluster
 * could invoke any method in this process by reflection.
 *
 * <h2>Dispatched only among replicas of the same application</h2>
 * Never across applications -- executing anything requires the peer to have the same bean and
 * method, which only another replica of the same application does.
 *
 * <h2>With a single replica it runs locally</h2>
 * No network, no serialisation: an ordinary reflective call. And the decision is <b>made
 * afresh on every call</b>: a new replica joining means the next call begins dispatching
 * outward, and every other replica leaving returns it to local execution automatically.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public interface ProcessingPool {

    /**
     * Submits a method call and <b>returns its result</b>.
     *
     * <p>It does not block the calling thread. To wait synchronously, {@code join()} or
     * {@code get(timeout)} on the returned future -- but never {@code get()} without a
     * timeout: it hangs when the remote node dies. The service has timeouts of its own, but
     * they are not guaranteed under a network partition.
     *
     * <pre>{@code
     * // Asynchronously
     * pool.submit("orderService", "settle", orderId)
     *     .thenAccept(result -> log.info("settlement finished: {}", result));
     *
     * // Synchronously, and always with a timeout
     * Object r = pool.submit("orderService", "settle", orderId).get(10, TimeUnit.SECONDS);
     * }</pre>
     *
     * <p>Failures -- the method not being exposed, serialisation failing, an error at the
     * remote end, a timeout -- all arrive through the future's exceptional path, wrapped in a
     * {@link ProcessingPoolException}.
     *
     * @param className  the target class name, simple or fully qualified. May be null when a
     *                   beanName is given
     * @param beanName   the target bean name; it takes precedence in the lookup
     * @param methodName the method name, or the name published in
     *                   {@link MultiProcessingCall#value()}
     * @param args       the arguments, all of which must be serialisable. Pass null or an
     *                   empty array for none
     */
    <T> CompletableFuture<T> submit(String className, String beanName,
                                    String methodName, Object[] args);

    /** Shorthand for submitting by bean name. */
    default <T> CompletableFuture<T> submit(String beanName, String methodName, Object... args) {
        return submit(null, beanName, methodName, args);
    }

    /**
     * Issues a method call and <b>wants no result</b>.
     *
     * <p>Sent and forgotten: it does not block and does not wait. A failure leaves a warning
     * in the log and <b>throws nothing</b> -- a caller that chose to want no result should not
     * then be interrupted by an exception.
     *
     * <p>It suits cases where handing the work off is all that matters: refreshing a cache,
     * sending a notification, cleaning up temporary data. Where the result genuinely matters,
     * use {@link #submit}.
     *
     * <p>The semantics match {@code Executor.execute} against
     * {@code ExecutorService.submit}.
     */
    void execute(String className, String beanName, String methodName, Object[] args);

    /** Shorthand for issuing by bean name. */
    default void execute(String beanName, String methodName, Object... args) {
        execute(null, beanName, methodName, args);
    }

    /** How many replicas of this application there are besides this process. 0 means every
     *  subsequent call runs locally. */
    int peerCount();

    /**
     * How the pool is doing: active, completed and queued counts, plus fork/join's own steal
     * count.
     *
     * <p>Shaped to match the familiar {@code ThreadPoolExecutor} readings, though there are in
     * fact <b>two</b> pools underneath -- fork/join for local recursion, an ordinary pool for
     * inbound tasks -- and each one's numbers can be read separately. They are only meaningful
     * read that way: a local queue means this node cannot keep up computing, an inbound queue
     * means others are pushing too hard at it.
     *
     * <p>Reading it takes an instantaneous snapshot and blocks no execution path.
     */
    PoolStats poolStats();
}
