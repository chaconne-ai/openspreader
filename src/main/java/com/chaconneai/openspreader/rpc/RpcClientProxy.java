package com.chaconneai.openspreader.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.retry.support.RetryTemplateBuilder;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * The dynamic proxy behind a {@link RpcClient} interface: it turns a method call into a
 * remote one.
 *
 * <h2>Retrying uses spring-retry rather than something written here</h2>
 * "How many attempts, how long between them, which exceptions retry" is a question with a
 * settled answer, and spring-retry has already done the backoff strategies, the exception
 * classification and the context. A for loop of one's own looks simpler, but backoff, jitter
 * and classification by exception follow soon enough -- ending in a reinvention that is worse.
 *
 * <h2>Every retry moves to another instance</h2>
 * This is the crucial point, and what makes retrying worth anything here: the target is chosen
 * <b>inside</b> {@link RpcService#invoke}, freshly on each entry. So "one instance has died"
 * is routed around by a single retry.
 *
 * <p>Conversely, where every instance reports the same error -- an invalid argument, say --
 * retrying merely repeats the same error. Narrow that case with
 * {@link RpcClient#retryableExceptions()}: {@link RpcException} is the recommended value,
 * retrying only on a communication failure.
 *
 * <h2>The fallback comes after every retry is exhausted</h2>
 * Not on each failure -- that would hand the caller a false empty result on one network
 * wobble, where a retry would have recovered.
 *
 * <h2>The limit is on concurrency, not on calls per second</h2>
 * What is limited is the number of requests <b>in flight at once</b>, through a semaphore
 * inside the process. This differs from the usual "N per second", and concurrency is exactly
 * what is wanted here: calls are synchronous, so one in-flight request occupies one calling
 * thread, and the in-flight count rises of its own accord as downstream slows -- capping it
 * makes this process's call rate follow downstream automatically.
 *
 * <p>A per-second limit cannot do that: when downstream goes from 10ms to 10s, holding the
 * rate steady piles in-flight requests up until something bursts.
 *
 * <h2>serviceId is resolved on every call</h2>
 * It supports {@code ${...}} placeholders and {@code #{...}} expressions, both of which may
 * change while running -- a configuration server pushes a new value, or the state a bean the
 * expression depends on changes. So whenever the value contains an expression it is
 * <b>re-evaluated on every call</b> and never cached: "point at another application" ought to
 * take effect at once.
 *
 * <p>A plain string with no expression is settled at construction and costs nothing extra,
 * which is the case for the great majority of interfaces.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class RpcClientProxy implements InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(RpcClientProxy.class);

    private final Class<?> apiType;
    private final RpcClient config;
    private final RpcService service;
    private final BeanFactory beanFactory;
    private final RetryTemplate retryTemplate;

    /** The target bean name and class name, worked out at construction so no call has to
     *  decide again. */
    private final String beanName;
    private final String className;
    private final long timeoutMs;

    /** Whether serviceId contains an expression. Without one, {@link #staticServiceId} is
     *  used directly, at no cost at all. */
    private final boolean dynamicServiceId;
    private final String staticServiceId;

    /** For evaluating {@code ${...}} and {@code #{...}}; null without a Spring container. */
    private final ConfigurableBeanFactory expressionFactory;
    private final BeanExpressionContext expressionContext;

    /**
     * Permits for in-flight requests. null means no limit.
     *
     * <p>{@code fair=false}: a fair semaphore maintains the waiting queue's order at a
     * noticeably higher cost, and what is waited for here is "downstream has room", where the
     * order does not matter.
     */
    private final Semaphore inflightPermits;
    private final long acquireTimeoutMs;
    private final int maxConcurrent;

    /** The fallback implementation, loaded lazily -- most interfaces configure none, and
     *  startup should do nothing on their behalf. */
    private volatile Object fallbackInstance;

    public RpcClientProxy(Class<?> apiType, RpcClient config, RpcService service,
                          BeanFactory beanFactory) {
        this(apiType, config, service, beanFactory, RpcDefaults.FALLBACK);
    }

    public RpcClientProxy(Class<?> apiType, RpcClient config, RpcService service,
                          BeanFactory beanFactory, RpcDefaults defaults) {
        this.apiType = apiType;
        this.config = config;
        this.service = service;
        this.beanFactory = beanFactory;
        this.beanName = config.beanName();
        this.className = config.className();
        // Global configuration with local overrides: anything absent from the annotation takes
        // the global value
        this.timeoutMs = defaults.resolveTimeoutMs(config);
        this.retryTemplate = buildRetryTemplate(config, defaults);
        this.maxConcurrent = defaults.resolveMaxConcurrent(config);
        this.acquireTimeoutMs = defaults.resolveAcquireTimeoutMs(config);
        this.inflightPermits = maxConcurrent > 0 ? new Semaphore(maxConcurrent, false) : null;

        this.expressionFactory = beanFactory instanceof ConfigurableBeanFactory cbf ? cbf : null;
        this.expressionContext = expressionFactory == null
                ? null
                : new BeanExpressionContext(expressionFactory, null);
        String rawServiceId = config.serviceId();
        this.dynamicServiceId = expressionFactory != null && hasExpression(rawServiceId);
        this.staticServiceId = dynamicServiceId ? null : rawServiceId;
    }

    /** {@code ${...}} is a property placeholder and {@code #{...}} is SpEL; both count as
     *  expressions. */
    private static boolean hasExpression(String value) {
        return value != null && (value.contains("${") || value.contains("#{"));
    }

    /**
     * Works out which application this call goes to.
     *
     * <p>The resolution order matches {@code @Value}'s: {@code ${...}} placeholders are
     * expanded first, then {@code #{...}} expressions are evaluated. The other way round, an
     * expression configured inside a placeholder would never be evaluated.
     */
    private String resolveServiceId() {
        if (!dynamicServiceId) {
            return staticServiceId;
        }
        String resolved = expressionFactory.resolveEmbeddedValue(config.serviceId());
        BeanExpressionResolver resolver = expressionFactory.getBeanExpressionResolver();
        if (resolver == null || resolved == null) {
            return resolved;
        }
        Object value = resolver.evaluate(resolved, expressionContext);
        return value == null ? null : value.toString();
    }

    /**
     * Which class name this call looks the peer's bean up by.
     *
     * <p>With neither {@code className} nor {@code beanName} configured, it uses <b>the
     * interface that declares the method</b> rather than the annotated interface. The two are
     * often different: the contract interface is the shared one ({@code OrderApi}), while the
     * annotated one may be a sub-interface -- creating one sub-interface per timeout or retry
     * policy is a common usage.
     *
     * <p>What the peer implements is <b>the contract interface</b>, and it has no idea how many
     * sub-interfaces exist on the calling side. So matching by the method's declarer is what
     * lines up.
     */
    private String resolveClassName(Method method) {
        if (!className.isBlank() || !beanName.isBlank()) {
            return className;
        }
        return method.getDeclaringClass().getName();
    }

    /** Builds a proxy implementing {@code apiType}, with the fallback defaults. */
    public static Object create(Class<?> apiType, RpcClient config, RpcService service,
                                BeanFactory beanFactory) {
        return create(apiType, config, service, beanFactory, RpcDefaults.FALLBACK);
    }

    /** Builds a proxy implementing {@code apiType}, with {@code defaults} from the global
     *  configuration. */
    public static Object create(Class<?> apiType, RpcClient config, RpcService service,
                                BeanFactory beanFactory, RpcDefaults defaults) {
        return Proxy.newProxyInstance(
                apiType.getClassLoader(),
                new Class<?>[]{apiType},
                new RpcClientProxy(apiType, config, service, beanFactory, defaults));
    }

    private static RetryTemplate buildRetryTemplate(RpcClient config, RpcDefaults defaults) {
        RetryTemplateBuilder builder = RetryTemplate.builder()
                // maxAttempts is "attempts in total" while maxRetries is "further attempts
                // after a failure"
                .maxAttempts(Math.max(1, defaults.resolveMaxRetries(config) + 1));

        long intervalMs = defaults.resolveRetryIntervalMs(config);
        if (intervalMs > 0) {
            builder.fixedBackoff(intervalMs);
        } else {
            builder.noBackoff();
        }

        List<Class<? extends Throwable>> retryable = Arrays.asList(config.retryableExceptions());
        if (!retryable.isEmpty()) {
            builder.retryOn(retryable);
        }
        // Matches even through a wrapper: a remote exception restored here sometimes carries
        // a shell around it
        builder.traversingCauses();
        return builder.build();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // Object's own methods must not go over the network -- a remote call for one
        // toString() would be absurd
        if (method.getDeclaringClass() == Object.class) {
            return handleObjectMethod(proxy, method, args);
        }
        // An interface's default method runs locally: it is the interface's own logic and does
        // not belong to the peer
        if (method.isDefault()) {
            return InvocationHandler.invokeDefault(proxy, method, args);
        }

        // The limit sits outside the retry: one logical call occupies one permit, however many
        // attempts it makes inside. Inside, retries would take extra permits -- adding pressure
        // at exactly the moment the system is under most strain
        if (!acquirePermit(method)) {
            return fallbackOrThrow(method, args, new RpcOverloadException(
                    apiType.getSimpleName() + "#" + method.getName()
                            + " in-flight requests have reached the limit of " + maxConcurrent
                            + ", and no permit came free after " + acquireTimeoutMs + "ms. "
                            + "Either downstream has slowed, or max-concurrent is set too low"));
        }
        try {
            return retryTemplate.execute(context -> {
                if (context.getRetryCount() > 0) {
                    log.debug("{}#{} retry {}", apiType.getSimpleName(), method.getName(),
                            context.getRetryCount());
                }
                try {
                    // Recomputed each time: a new value from a configuration server, or a
                    // change in state the expression depends on, should take effect at once
                    return service.invoke(resolveServiceId(), beanName, resolveClassName(method),
                            method, args, timeoutMs);
                } catch (RuntimeException | Error e) {
                    throw e;
                } catch (Throwable t) {
                    // A checked exception the business method declares. Wrapped so it can pass
                    // through RetryTemplate's signature, and unwrapped before it leaves
                    throw new InvocationTargetException(t);
                }
            });
        } catch (InvocationTargetException e) {
            Throwable cause = e.getTargetException();
            return fallbackOrThrow(method, args, cause == null ? e : cause);
        } catch (Throwable t) {
            return fallbackOrThrow(method, args, t);
        } finally {
            releasePermit();
        }
    }

    /**
     * Takes an in-flight permit.
     *
     * @return true when one was taken, and true as well when there is no limit
     */
    private boolean acquirePermit(Method method) {
        if (inflightPermits == null) {
            return true;
        }
        if (acquireTimeoutMs <= 0) {
            // No waiting: failing to take one gives way at once, so the caller takes its
            // fallback path
            return inflightPermits.tryAcquire();
        }
        try {
            return inflightPermits.tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** Permits must be returned in a finally: missing one costs this interface a permit for
     *  good, and in complete silence. */
    private void releasePermit() {
        if (inflightPermits != null) {
            inflightPermits.release();
        }
    }

    /** How many requests are in flight. For monitoring and tests. */
    public int inflightCount() {
        return inflightPermits == null ? 0 : maxConcurrent - inflightPermits.availablePermits();
    }

    /**
     * Every retry is exhausted; see whether there is a fallback.
     *
     * @return whatever the fallback implementation returns
     * @throws Throwable the last failure's cause, when no fallback is configured or the
     *                   fallback factory declines
     */
    private Object fallbackOrThrow(Method method, Object[] args, Throwable cause) throws Throwable {
        Object fallback = resolveFallback(cause);
        if (fallback == null) {
            throw cause;
        }
        log.warn("{}#{} failed; taking the fallback: {}", apiType.getSimpleName(),
                method.getName(), cause.toString());
        if (!apiType.isInstance(fallback)) {
            throw new RpcException("the fallback " + fallback.getClass().getName()
                    + " does not implement " + apiType.getName(), cause);
        }
        try {
            // Invoked through the interface's Method rather than looking one up on the
            // fallback's own class: a fallback is often an anonymous or non-public class, whose
            // methods a reflective call refuses with IllegalAccessException, while an interface
            // method is always public
            return method.invoke(fallback, args);
        } catch (InvocationTargetException e) {
            // An exception the fallback threw itself, rethrown as it is
            throw e.getTargetException();
        } catch (IllegalAccessException e) {
            throw new RpcException("the fallback " + fallback.getClass().getName()
                    + " has an inaccessible method", e);
        }
    }

    /** Obtains the fallback, the factory first -- it also knows why the call failed. */
    private Object resolveFallback(Throwable cause) {
        Class<?> factoryType = config.fallbackFactory();
        if (factoryType != void.class) {
            Object factory = instantiate(factoryType);
            if (factory instanceof RpcFallbackFactory<?> f) {
                Object created = f.create(cause);
                if (created != null && !apiType.isInstance(created)) {
                    throw new RpcException("the fallback factory " + factoryType.getName()
                            + " produced a " + created.getClass().getName()
                            + ", which does not implement " + apiType.getName());
                }
                return created;
            }
            throw new RpcException("fallbackFactory " + factoryType.getName()
                    + " must implement RpcFallbackFactory");
        }

        Class<?> fallbackType = config.fallback();
        if (fallbackType == void.class) {
            return null;
        }
        Object cached = fallbackInstance;
        if (cached == null) {
            synchronized (this) {
                cached = fallbackInstance;
                if (cached == null) {
                    cached = instantiate(fallbackType);
                    if (!apiType.isInstance(cached)) {
                        throw new RpcException("the fallback " + fallbackType.getName()
                                + " does not implement " + apiType.getName());
                    }
                    fallbackInstance = cached;
                }
            }
        }
        return cached;
    }

    /** Looked up as a Spring bean first -- a fallback often needs things injected: a cache,
     *  a logger, instrumentation. */
    private Object instantiate(Class<?> type) {
        if (beanFactory != null) {
            try {
                return beanFactory.getBean(type);
            } catch (RuntimeException ignore) {
                // Not a bean; fall back to constructing one reflectively
            }
        }
        try {
            return type.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RpcException(type.getName() + " is neither a Spring bean nor has a "
                    + "no-argument constructor", e);
        }
    }

    private Object handleObjectMethod(Object proxy, Method method, Object[] args) {
        return switch (method.getName()) {
            case "toString" -> "RpcClient(" + apiType.getName() + " -> "
                    + (config.serviceId().isBlank() ? "the cluster" : config.serviceId()) + ")";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == (args == null ? null : args[0]);
            default -> null;
        };
    }
}
