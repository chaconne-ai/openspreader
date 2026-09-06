package com.chaconneai.openspreader.pooling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.scheduling.annotation.Async;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The allow-list of remotely callable methods.
 *
 * <p>At startup every bean is scanned and each method annotated with
 * {@link MultiProcessingCall} is registered. <b>Any method not registered is refused</b> --
 * otherwise any node in the cluster could reflectively call any method in this process.
 *
 * <p>The key is {@code beanName#methodName#argCount}. The argument count distinguishes
 * overloads; where several overloads share a name and an argument count, the first is taken
 * and a warning is logged.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MethodRegistry implements BeanPostProcessor {

    private static final Logger log = LoggerFactory.getLogger(MethodRegistry.class);

    private final Map<String, Target> methods = new ConcurrentHashMap<>();

    /**
     * Bean instance to bean name.
     *
     * <p>When the aspect intercepts a call it holds the object but not its name, and dispatch
     * needs the bean name to find the same bean on the far side. Scanning {@link #methods} by
     * class name would also find it, but that is an O(n) linear sweep sitting on the critical
     * path of every cross-process call.
     *
     * <p>Keys compare by <b>identity</b>: a bean may have overridden equals -- a record or
     * Lombok's @Data especially -- and two different beans would then be taken for one.
     */
    private final Map<IdentityKey, String> beanNames = new ConcurrentHashMap<>();

    /**
     * One remotely callable method.
     *
     * @param bean     the proxy. Executing through it is what keeps aspects such as
     *                 transactions and caching from being bypassed
     * @param rawBean  the real object behind the proxy. There is exactly one case for it, see
     *                 {@link #targetFor(Method)}
     * @param method   the method on the real class, from which annotations are read
     * @param beanName the bean's name in Spring
     */
    public record Target(Object bean, Object rawBean, Method method, String beanName) {

        /**
         * Whether this local execution should go through the proxy or the real object.
         *
         * <p>Normally through the <b>proxy</b>. The one exception is a method annotated
         * {@code @Async}: that annotation moves execution onto another thread pool, while
         * {@link MultiProcessingInterceptor}'s recursion guard is a {@link ThreadLocal} and
         * <b>does not cross a thread boundary</b> -- the new thread cannot see the "executing
         * locally" mark, so it dispatches again, and round it goes until threads run out.
         *
         * <p>Skipping it is right semantically too: {@code @Async} asks not to block the
         * caller, and dispatch has already achieved that -- the caller holds a future. The
         * executing side's job is to finish the work and carry the result back, and going
         * asynchronous a second time means nothing.
         *
         * <p><b>The cost</b>: in this case the executing side's other aspects
         * ({@code @Transactional}, {@code @Cacheable}) are bypassed along with it. Where a
         * transaction is wanted, do not stack {@code @Async} on the same method -- express
         * the asynchrony with {@code @MultiProcessingCall(sync = false)}, which does the same
         * thing without this problem.
         */
        public Object targetFor(Method method) {
            return AnnotatedElementUtils.hasAnnotation(method, Async.class)
                    ? rawBean
                    : bean;
        }
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        // The real class rather than the proxy class: a proxy class carries no method annotations
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        ReflectionUtils.doWithMethods(targetClass, method -> {
            MultiProcessingCall annotation =
                    AnnotatedElementUtils.findMergedAnnotation(method, MultiProcessingCall.class);
            if (annotation == null) {
                return;
            }
            String exposed = annotation.value().isBlank() ? method.getName() : annotation.value().trim();
            String key = key(beanName, exposed, method.getParameterCount());

            // The call goes through the proxy (bean), so that aspects such as transactions and
            // caching are not bypassed; but annotations are read off the real class's method
            Target previous = methods.putIfAbsent(key,
                    new Target(bean, rawTargetOf(bean), method, beanName));
            if (previous != null) {
                log.warn("Duplicate remotely callable method: {}; keeping {} and ignoring {}. "
                        + "Rename overloads that share a name and an argument count",
                        key, previous.method(), method);
                return;
            }
            beanNames.put(new IdentityKey(bean), beanName);
            log.info("Registered remotely callable method: {} -> {}#{}", key,
                    targetClass.getSimpleName(), method.getName());
        }, ReflectionUtils.USER_DECLARED_METHODS);
        return bean;
    }

    /**
     * Looks up a callable method.
     *
     * @param beanName   the bean name; when blank, className is the fallback
     * @param className  the class name, simple or fully qualified, used when beanName is blank
     * @param methodName the method name
     * @param argCount   the argument count
     * @return null when nothing is found -- on which the caller should report "the method is
     *         not exposed" rather than "the method does not exist"; the two are the same to
     *         the caller but not to whoever is troubleshooting
     */
    public Target find(String beanName, String className, String methodName, int argCount) {
        if (beanName != null && !beanName.isBlank()) {
            Target byBean = methods.get(key(beanName, methodName, argCount));
            if (byBean != null) {
                return byBean;
            }
        }
        if (className == null || className.isBlank()) {
            return null;
        }
        // Falling back to the class name: a caller may know the class but not the bean name
        for (Target t : methods.values()) {
            if (classMatches(AopUtils.getTargetClass(t.bean()), className)
                    && t.method().getName().equals(methodName)
                    && t.method().getParameterCount() == argCount) {
                return t;
            }
        }
        return null;
    }

    /**
     * Whether the class name matches.
     *
     * <p>Besides the class's own fully qualified and simple names, <b>the interfaces it
     * implements count too</b> -- an RPC caller holds only the interface, since
     * {@code @RpcClient} sits on the interface, and has no idea what the implementation class
     * on the far side is called. Matching interface names means the two sides need share only
     * an API interface, rather than also fixing the implementation's name by agreement.
     */
    private static boolean classMatches(Class<?> targetClass, String className) {
        if (targetClass.getName().equals(className)
                || targetClass.getSimpleName().equals(className)) {
            return true;
        }
        for (Class<?> api : targetClass.getInterfaces()) {
            if (api.getName().equals(className) || api.getSimpleName().equals(className)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Looks the bean name back up.
     *
     * @return null when the bean has no method annotated {@link MultiProcessingCall} at all
     */
    public String beanNameOf(Object bean) {
        return bean == null ? null : beanNames.get(new IdentityKey(bean));
    }

    /** Strips the proxy to get the real object; something that is not a proxy is itself. */
    private static Object rawTargetOf(Object bean) {
        Object raw = AopProxyUtils.getSingletonTarget(bean);
        return raw == null ? bean : raw;
    }

    private static String key(String beanName, String methodName, int argCount) {
        return beanName + "#" + methodName + "#" + argCount;
    }

    /** A map key that compares by identity rather than equals. */
    private record IdentityKey(Object bean) {

        @Override
        public boolean equals(Object o) {
            return o instanceof IdentityKey other && other.bean == this.bean;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(bean);
        }
    }

    /** The registered methods, for troubleshooting. */
    public Map<String, String> listMethods() {
        Map<String, String> result = new LinkedHashMap<>();
        methods.forEach((k, v) -> result.put(k,
                AopUtils.getTargetClass(v.bean()).getSimpleName() + "#" + v.method().getName()));
        return result;
    }

    public int size() {
        return methods.size();
    }
}
