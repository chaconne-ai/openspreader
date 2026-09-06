package com.chaconneai.openspreader.rpc;

import com.chaconneai.openspreader.pooling.MultiProcessingCall;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Placed on an <b>interface</b>, this makes it the entry point for remote calls to another
 * application.
 *
 * <pre>{@code
 * // Over in the order service there is a bean whose method carries @MultiProcessingCall
 * @RpcClient(serviceId = "order-service", beanName = "orderService")
 * public interface OrderApi {
 *     Order findById(Long id);
 * }
 *
 * // In this application it is injected and used as an ordinary bean
 * @Service
 * public class CheckoutService {
 *
 *     private final OrderApi orders;          // constructor injection; the proxy is built for you
 *
 *     public void checkout(Long id) {
 *         Order order = orders.findById(id);  // this line goes over the network
 *     }
 * }
 * }</pre>
 *
 * <h2>Its division of labour with {@code @MultiProcessingCall}</h2>
 * <table border="1">
 *   <caption>Which to use when</caption>
 *   <tr><th></th><th>{@code @MultiProcessingCall}</th><th>{@code @RpcClient}</th></tr>
 *   <tr><td>Who is called</td><td>Another replica of <b>the same application</b></td>
 *       <td>An instance of <b>another application</b></td></tr>
 *   <tr><td>Why</td><td>To spread the computation; anyone can do it</td>
 *       <td>Only the other side has that capability</td></tr>
 *   <tr><td>Alone in the cluster</td><td>Runs locally, without the network</td>
 *       <td>Fails -- the other side is simply not in the cluster</td></tr>
 *   <tr><td>How it is written</td><td>An annotation on the method; the caller notices
 *       nothing</td><td>Define an interface and inject it</td></tr>
 * </table>
 *
 * <p>The two share <b>one allow-list</b>: the serving method must carry
 * {@link MultiProcessingCall}, or the request is refused. That is not for convenience but
 * because they guard against the same thing -- without an allow-list, any node in the cluster
 * could reflectively call any method of any bean in this process.
 *
 * <h2>It is synchronous</h2>
 * A call blocks until the result arrives or it times out, exactly as a local call does. For
 * asynchrony, wrap it in {@code CompletableFuture.supplyAsync} -- no asynchronous form is
 * offered here, because "it looks like a local call" is the whole point, and a second form is
 * a second thing to hold in mind.
 *
 * <h2>The target is chosen afresh on every call</h2>
 * Each call reads the member list again and picks an instance by the cluster's configured
 * load-balancing strategy ({@code spring.spreader.load-balancer}, round-robin by default). So
 * the next call follows the other side's scaling <b>immediately</b>, and nowhere caches "how
 * many instances they have".
 *
 * <h2>Retries and idempotence</h2>
 * A failure is retried {@link #maxRetries()} times, <b>on a different instance each time</b>.
 *
 * <p><b>The called method should therefore be idempotent.</b> Retrying after a timeout brings
 * an unavoidable problem: the request may already have completed, with only the reply lost on
 * the way back. A non-idempotent operation -- a debit, an order -- must either carry an
 * idempotence key of its own or set {@code maxRetries} to 0.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RpcClient {

    /**
     * "Not configured on the annotation; take the global value."
     *
     * <p>It is the default for {@link #timeout()}, {@link #maxRetries()} and
     * {@link #retryInterval()}. A sentinel of its own is needed because an annotation's default
     * and a value the user wrote out explicitly <b>look identical</b> at runtime, and without
     * one there is no telling "they did not configure it" from "they configured a value that
     * happens to equal the default" -- in which case a global setting such as
     * {@code spring.spreader.multiprocessing.rpc.timeout} would never take effect, with nothing
     * to show for it.
     */
    long USE_GLOBAL = Long.MIN_VALUE;

    /**
     * The "take the global value" sentinel for the {@code int} settings.
     *
     * <p><b>{@code (int) USE_GLOBAL} will not do</b>: casting {@code Long.MIN_VALUE} to an int
     * gives <b>0</b>, and 0 is a perfectly legitimate {@code maxRetries} meaning "do not
     * retry". An explicit {@code maxRetries = 0} would then be taken for "not configured" and
     * the global value used -- an interface that said plainly "do not retry" would retry, and
     * that is a mistake very hard to work back to from the symptom.
     */
    int USE_GLOBAL_INT = Integer.MIN_VALUE;

    /**
     * The target application name, matching the other side's
     * {@code spring.application.name}.
     *
     * <p>Blank means <b>any application</b> -- any node in the cluster with that bean. It
     * should generally be filled in: only then does "this calls the order service" have a
     * definite meaning, and only then can a failure say something comprehensible like
     * "order-service has no available instance".
     */
    String serviceId() default "";

    /**
     * The target bean's class name, fully qualified or simple.
     *
     * <p><b>At least one of this and {@link #beanName()}</b> should be set; with both blank,
     * this interface's fully qualified name is matched -- the other side's bean matches as long
     * as it implements an interface of the same name, which is also the least trouble.
     */
    String className() default "";

    /**
     * The target bean's name.
     *
     * <p>More precise than {@link #className()}, and a little quicker -- the other side looks a
     * bean name up by hash and scans for a class name. Where one class is registered as one
     * bean on that side, the two are equivalent.
     */
    String beanName() default "";

    /**
     * The timeout for <b>each attempt</b>, not the total: three retries at 60 seconds means
     * the caller waits four minutes in the worst case.
     *
     * <p>Unset, it takes {@code spring.spreader.multiprocessing.rpc.timeout}, 60 seconds by
     * default.
     *
     * <p>0 or negative waits indefinitely. Think before configuring that: a stuck peer becomes
     * a stuck process here, and with a long call chain that is where a cascading failure
     * starts.
     */
    long timeout() default USE_GLOBAL;

    /**
     * How many further attempts follow a failure. 0 means no retry.
     *
     * <p>Unset, it takes {@code spring.spreader.multiprocessing.rpc.max-retries}, 3 by default.
     *
     * <p>Each attempt <b>picks a different instance</b>, so retrying works against "one
     * instance has died"; against "every instance reports the same error" it merely repeats the
     * error a few times -- narrow that with {@link #retryableExceptions()}.
     */
    int maxRetries() default USE_GLOBAL_INT;

    /**
     * How long to wait between attempts, in {@link #timeUnit()}. 0 retries at once.
     *
     * <p>Unset, it takes {@code spring.spreader.multiprocessing.rpc.retry-interval}, one second
     * by default.
     */
    int retryInterval() default USE_GLOBAL_INT;

    /**
     * How many requests this process may have in flight on this interface at once. 0 means no
     * limit.
     *
     * <p>Unset, it takes {@code spring.spreader.multiprocessing.rpc.max-concurrent}.
     *
     * <h2>What it holds back</h2>
     * When something downstream slows, a caller sending at the same rate piles requests up --
     * in the downstream queue, and on this process's threads. By the time anyone notices, both
     * are full and <b>still getting worse</b>.
     *
     * <p>Capping the in-flight count puts a gate on the path: as downstream slows, this
     * process's calls slow with it, and pressure does not accumulate without bound. That is
     * back-pressure. Long tasks -- seconds to minutes -- need it most, since they occupy an
     * in-flight slot for longest and pile up most readily.
     *
     * <p>Failing to take a permit throws {@link RpcOverloadException}, <b>not</b>
     * {@link RpcException}: overload and failure are different things, and retrying the former
     * only makes it worse.
     *
     * <h2>It is per process, not cluster-wide</h2>
     * Each calling instance limits itself. A cluster-wide total would need
     * {@link com.chaconneai.openspreader.sync.ProcessingSemaphore}, but that means <b>asking
     * the leader for a permit before every call</b> -- another cross-node round trip on every
     * RPC, doubling the latency for the sake of limiting it. Not a good bargain.
     */
    int maxConcurrent() default USE_GLOBAL_INT;

    /**
     * Once the in-flight limit is reached, how long to wait for a slot, in
     * {@link #timeUnit()}.
     *
     * <p>Unset, it takes {@code spring.spreader.multiprocessing.rpc.acquire-timeout}.
     *
     * <p>0 does not wait and throws {@link RpcOverloadException} at once -- failing fast, which
     * suits a caller with a fallback path. A positive value queues briefly, which suits a short
     * traffic spike: it recovers once the spike passes rather than refusing everything.
     *
     * <p><b>Do not set it too large</b>: queueing is piling up too, only inside this
     * process.
     */
    long acquireTimeout() default USE_GLOBAL;

    /**
     * Which exceptions trigger a retry; every {@link Exception} by default.
     *
     * <p>The default errs towards trying again, but in production it is <b>worth
     * narrowing</b>: narrowed to {@link RpcException}, only communication failures retry --
     * cannot connect, timed out, no available instance -- and a business exception the peer
     * threw explicitly does not. A failed argument validation fails just as surely on the
     * eight-hundredth attempt, wasting time and the peer's resources.
     */
    Class<? extends Throwable>[] retryableExceptions() default Exception.class;

    /** The unit of {@link #timeout()} and {@link #retryInterval()}; seconds by default. */
    TimeUnit timeUnit() default TimeUnit.SECONDS;

    /**
     * The fallback implementation: called once retries are exhausted, instead of throwing to
     * the caller.
     *
     * <p>It must implement this interface, and must be either a Spring bean, looked up by type
     * first, or have a no-argument constructor.
     *
     * <p><b>A fallback turns a failure into a return value that looks normal</b>, so use it
     * only where "an empty or cached answer will do" -- recommendation slots, non-critical
     * display data. Do not configure one for calls involving money or state changes: there, the
     * information that it failed matters more than any substitute value.
     */
    Class<?> fallback() default void.class;

    /**
     * A fallback factory: like {@link #fallback()}, but also given <i>why</i> it failed.
     *
     * <p>It must implement {@link RpcFallbackFactory}. It suits cases that treat exception
     * types differently, or that log or record the reason for the failure.
     *
     * <p>Configured alongside {@code fallback}, the factory wins.
     */
    Class<?> fallbackFactory() default void.class;
}
