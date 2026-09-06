package com.chaconneai.openspreader.pooling;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.scheduling.annotation.Async;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Hands calls to methods annotated {@link MultiProcessingCall} over to the process pool.
 *
 * <h2>With a single replica the pool is not entered at all</h2>
 * A synchronous call where this process is the only replica of the application passes straight
 * through -- no thread pool, no future, no serialisation, at exactly the cost of an ordinary
 * method call. Single-instance deployments -- local development, small projects, the start of
 * a rollout -- take this path throughout.
 *
 * <h2>The recursion guard is the most important thing here</h2>
 * Dispatch ends with {@link PoolService} reflectively invoking the same method of the same
 * bean, and the bean it holds is <b>the proxy</b> -- it has to be, or aspects such as
 * {@code @Transactional} would be bypassed. That reflective call therefore arrives here again,
 * and unstopped it is an infinite recursion: dispatch, execute, dispatch again, and on it
 * goes.
 *
 * <p>A {@link ThreadLocal} marks "this thread is executing locally on behalf of a dispatch",
 * and a call in that state passes straight through. The mark must be cleared in a
 * {@code finally}, or one exception stops that thread from ever dispatching again --
 * <b>permanently</b>, and in complete silence.
 *
 * <h2>Its relationship with {@code @Async} does not rest on AOP ordering</h2>
 * With both annotations stacked, which aspect ends up outermost is undetermined, since both
 * advisors default to the lowest precedence. Rather than gamble on the order, the behaviour is
 * made independent of it: <b>{@code @Async} always means treat the call as asynchronous</b>.
 * <ul>
 *   <li>Outermost: a future is returned here at once and the caller does not block -- what
 *       {@code @Async} wanted is already achieved</li>
 *   <li>Innermost: {@code @Async} has already moved the call to another thread, a future is
 *       returned on that thread, and it flattens the future -- exactly what Spring does with
 *       an asynchronous method returning a future</li>
 * </ul>
 * Both orderings give the same result: the caller does not block, and the computation may land
 * in another process.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MultiProcessingInterceptor implements MethodInterceptor {

    private static final Logger log = LoggerFactory.getLogger(MultiProcessingInterceptor.class);

    /** Whether this thread is executing locally on behalf of a dispatch. See the class
     *  javadoc. */
    private static final ThreadLocal<Boolean> EXECUTING_LOCALLY =
            ThreadLocal.withInitial(() -> Boolean.FALSE);

    private final PoolService service;
    private final MethodRegistry registry;

    public MultiProcessingInterceptor(PoolService service, MethodRegistry registry) {
        this.service = service;
        this.registry = registry;
    }

    /**
     * Called by {@link PoolService} either side of actually executing the local method.
     *
     * <p>It lives here rather than in PoolService because the mark belongs entirely to the
     * aspect: what it guards against is re-entry the aspect itself causes.
     */
    public static void enterLocalExecution() {
        EXECUTING_LOCALLY.set(Boolean.TRUE);
    }

    public static void exitLocalExecution() {
        // remove rather than set(false): pool threads live a long time, and keeping a
        // ThreadLocal entry around serves no purpose
        EXECUTING_LOCALLY.remove();
    }

    /** Whether the current thread is executing locally on behalf of a dispatch. */
    public static boolean isExecutingLocally() {
        return EXECUTING_LOCALLY.get();
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        if (isExecutingLocally()) {
            // This call is where dispatch ends: execute it, and do not dispatch again
            return invocation.proceed();
        }

        Method method = invocation.getMethod();
        Object target = invocation.getThis();
        MultiProcessingCall annotation = findAnnotation(method, target);
        if (annotation == null) {
            return invocation.proceed();
        }

        String beanName = registry.beanNameOf(target);
        String className = target == null ? null : AopUtils.getTargetClass(target).getName();
        String exposed = annotation.value().isBlank() ? method.getName() : annotation.value().trim();
        Object[] args = invocation.getArguments();

        boolean async = !annotation.sync()
                || AnnotatedElementUtils.hasAnnotation(method, Async.class);

        if (!async && service.peerCount() == 0) {
            // This process is the application's only replica, so dispatching would send the
            // call to itself. Execute it directly: no thread pool, no future, no
            // serialisation, at exactly the cost of an ordinary method call.
            //
            // The shortcut is well worth it: single-instance deployments -- local development,
            // small projects, the start of a rollout -- take it throughout, and a round trip
            // through a thread pool and a future costs a good deal under frequent calls.
            //
            // Only the synchronous branch does this. The asynchronous branch promises not to
            // occupy the calling thread, and executing directly would break that promise
            return invocation.proceed();
        }

        if (!async) {
            // timeout<=0 is future.get() with no deadline; otherwise future.get(timeout, unit)
            long timeoutMs = annotation.timeout() <= 0
                    ? 0L
                    : annotation.timeUnit().toMillis(annotation.timeout());
            return awaitResult(service.submitCall(className, beanName, exposed, args),
                    timeoutMs, beanName, exposed);
        }

        Class<?> returnType = method.getReturnType();
        if (returnType == void.class || returnType == Void.class) {
            // No result wanted: fire and forget
            service.fireAndForget(className, beanName, exposed, args);
            return null;
        }
        if (CompletableFuture.class.isAssignableFrom(returnType)
                || Future.class.isAssignableFrom(returnType)) {
            return flatten(service.submitCall(className, beanName, exposed, args));
        }
        throw new ProcessingPoolException(method.getDeclaringClass().getSimpleName() + "#"
                + method.getName() + " is dispatched asynchronously (sync=false, or it carries "
                + "@Async), so its return type must be CompletableFuture, Future or void, but "
                + "it is " + returnType.getSimpleName());
    }

    /**
     * Flattens nested futures.
     *
     * <p>An asynchronously dispatched method <b>returns a future of its own</b> -- it must, the
     * return type says so -- and dispatch wraps another around it. Unflattened, the caller
     * receives a {@code CompletableFuture<CompletableFuture<String>>}, where one {@code get()}
     * yields another future and only the second yields the result.
     *
     * <p>Nobody can use a signature that disagrees with its return value correctly, and the
     * compiler cannot catch it: after erasure {@code get()} returns Object, and assigning it to
     * a String blows up at runtime. Spring's {@code @Async} treats future-returning methods the
     * same way.
     */
    @SuppressWarnings("unchecked")
    private static CompletableFuture<Object> flatten(CompletableFuture<Object> future) {
        return future.thenCompose(value -> {
            if (value instanceof CompletionStage<?> stage) {
                return (CompletionStage<Object>) stage;
            }
            return CompletableFuture.completedFuture(value);
        });
    }

    /**
     * Waits synchronously for the result.
     *
     * <p>A {@code timeoutMs} of 0 waits indefinitely.
     *
     * <p>Exceptions are rethrown <b>as they are</b>: what the caller should see is the
     * exception the business method threw, not an ExecutionException two layers deep -- which
     * is awkward even to catch.
     */
    private Object awaitResult(CompletableFuture<Object> future, long timeoutMs,
                               String beanName, String methodName) throws Throwable {
        try {
            return timeoutMs <= 0
                    ? future.get()
                    : future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new ProcessingPoolException("interrupted while waiting for the result of "
                    + beanName + "#" + methodName, e);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new ProcessingPoolException(beanName + "#" + methodName + " timed out after "
                    + timeoutMs + "ms. Where the method is slow by nature, widen it there with "
                    + "@MultiProcessingCall(timeout=...)");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw cause == null ? e : cause;
        }
    }

    /** Annotations are read from the method on the <b>real class</b>; a proxy class's method
     *  carries none. */
    private static MultiProcessingCall findAnnotation(Method method, Object target) {
        MultiProcessingCall direct = AnnotatedElementUtils.findMergedAnnotation(method,
                MultiProcessingCall.class);
        if (direct != null || target == null) {
            return direct;
        }
        try {
            Method actual = AopUtils.getTargetClass(target)
                    .getMethod(method.getName(), method.getParameterTypes());
            return AnnotatedElementUtils.findMergedAnnotation(actual, MultiProcessingCall.class);
        } catch (NoSuchMethodException e) {
            log.debug("Method {} was not found on the real class; treated as unannotated", method);
            return null;
        }
    }
}
