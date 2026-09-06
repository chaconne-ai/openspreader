package com.chaconneai.openspreader.scheduling;

import com.chaconneai.openspreader.Scope;
import com.chaconneai.openspreader.sync.ProcessingMutex;
import com.chaconneai.openspreader.sync.ProcessingSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.ScheduledMethodRunnable;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link TaskScheduler} decorator that wraps scheduled tasks in cluster-wide mutual
 * exclusion.
 *
 * <h2>Why decorate the scheduler rather than use AOP</h2>
 * Intercepting {@code @Scheduled} methods with AOP brings two troubles: it needs AspectJ, and
 * {@code ScheduledAnnotationBeanPostProcessor} readily falls out with auto-proxying over
 * ordering -- the task gets registered before the proxy is built, and the aspect never takes
 * effect at all.
 *
 * <p>Decorating the scheduler sidesteps all of it: every {@code @Scheduled} method reaches the
 * scheduler as a {@link ScheduledMethodRunnable}, possibly inside several layers of Spring's
 * own wrapping, so one more layer holding a lock is all that is needed here -- and the method
 * object is right there.
 *
 * <h2>Failing to take the lock skips the round rather than queueing</h2>
 * Queueing a scheduled task on a lock is harmful: by the time it arrives the next round may be
 * due, and tasks pile up. The default is to skip this round outright and race again on the
 * next. Where waiting really is wanted, use {@link MultiProcessingScheduled#waitMs()}.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public class MultiProcessingTaskScheduler implements TaskScheduler {

    private static final Logger log = LoggerFactory.getLogger(MultiProcessingTaskScheduler.class);

    private final TaskScheduler delegate;
    private final ProcessingSyncService syncs;
    private final MultiProcessingTaskStats stats;

    /** Whether methods without the annotation are made exclusive too. */
    private final boolean applyToAll;

    /** The default granularity used when applyToAll is in effect. */
    private final Scope defaultScope;

    /** How many layers to peel before giving up. Normally there are only one or two. */
    private static final int MAX_UNWRAP_DEPTH = 5;

    /** A failed unwrap warns once only, so that every task does not log a line. */
    private final AtomicBoolean unwrapWarned = new AtomicBoolean();

    /** Method to resolved task settings, so annotations are not re-read on every firing. */
    private final Map<Method, TaskSetting> settings = new ConcurrentHashMap<>();

    public MultiProcessingTaskScheduler(TaskScheduler delegate, ProcessingSyncService syncs,
                                        MultiProcessingTaskStats stats, boolean applyToAll, Scope defaultScope) {
        this.delegate = delegate;
        this.syncs = syncs;
        this.stats = stats;
        this.applyToAll = applyToAll;
        this.defaultScope = defaultScope;
    }

    // ------------------------------------------------------------------
    // Wrapping
    // ------------------------------------------------------------------

    /**
     * Adds a layer where exclusion is wanted, and returns the task untouched otherwise.
     *
     * <p>Note that what goes inside is <b>the original task</b>, not the unwrapped one:
     * unwrapping serves only to read the annotation, and execution still goes through Spring's
     * own wrapping chain, error handling and outcome tracking intact. Taking it apart and
     * rebuilding it is what would change the existing behaviour.
     */
    private Runnable wrap(Runnable task) {
        return wrap(task, null);
    }

    /**
     * @param interval this task's scheduling interval; null for cron and Trigger tasks, whose
     *                 interval cannot be worked out
     */
    private Runnable wrap(Runnable task, Duration interval) {
        ScheduledMethodRunnable methodRunnable = findMethodRunnable(task);
        if (methodRunnable == null) {
            // A Runnable the application handed to the scheduler itself arrives here; there is
            // no method information, so it passes through untouched
            log.debug("Task {} is not a @Scheduled method; no cluster-wide exclusion applied",
                    task.getClass().getName());
            return task;
        }
        TaskSetting setting = settings.computeIfAbsent(
                methodRunnable.getMethod(), m -> resolve(m, methodRunnable, interval));
        return setting == TaskSetting.NONE ? task : new ExclusiveTask(task, setting);
    }

    /**
     * Digs the {@link ScheduledMethodRunnable} out of Spring's wrapping chain.
     *
     * <p>Spring does not hand a {@code @Scheduled} method to the scheduler directly: since 6.1
     * it first wraps it in {@code Task$OutcomeTrackingRunnable}, which records the outcome, and
     * outside that there may be an error-handling layer. These wrappers are all inner classes
     * with no public accessor, so the only way in is to follow "the one Runnable field" layer
     * by layer.
     *
     * <p>Finding nothing returns null and the caller treats the task as not exclusive -- far
     * better that the task keeps running on every instance than that a change in Spring's
     * internals stops the application from starting.
     */
    private ScheduledMethodRunnable findMethodRunnable(Runnable task) {
        Runnable current = task;
        for (int depth = 0; depth < MAX_UNWRAP_DEPTH && current != null; depth++) {
            if (current instanceof ScheduledMethodRunnable found) {
                return found;
            }
            current = unwrapOnce(current);
        }
        if (unwrapWarned.compareAndSet(false, true)) {
            log.warn("Could not extract the @Scheduled method information from {}, so "
                    + "cluster-wide exclusion will not apply to tasks of this kind. This is most "
                    + "likely a change in the Spring version; please report it",
                    task.getClass().getName());
        }
        return null;
    }

    /** Peels one layer of wrapping: finds the Runnable it holds inside. */
    private static Runnable unwrapOnce(Runnable task) {
        for (Field field : task.getClass().getDeclaredFields()) {
            if (!Runnable.class.isAssignableFrom(field.getType())) {
                continue;
            }
            try {
                ReflectionUtils.makeAccessible(field);
                if (field.get(task) instanceof Runnable inner && inner != task) {
                    return inner;
                }
            } catch (Throwable ignored) {
                // Unreadable means this path is closed; the caller treats it as not exclusive
            }
        }
        return null;
    }

    private TaskSetting resolve(Method method, ScheduledMethodRunnable runnable, Duration interval) {
        MultiProcessingScheduled annotation =
                AnnotatedElementUtils.findMergedAnnotation(method, MultiProcessingScheduled.class);
        if (annotation == null && !applyToAll) {
            return TaskSetting.NONE;
        }

        Scope scope = annotation == null ? defaultScope : annotation.scope();
        long waitMs = annotation == null ? 0L : annotation.waitMs();
        String key = annotation == null || annotation.value().isBlank()
                ? defaultKey(method, runnable)
                : annotation.value().trim();

        // -1 means automatic: take the scheduling interval, so that it runs once per period.
        // A cron task's interval cannot be worked out, so it falls back to 0 -- released as
        // soon as it finishes
        long configured = annotation == null ? -1L : annotation.lockAtLeastMs();
        long lockAtLeastMs = configured >= 0
                ? configured
                : (interval == null ? 0L : interval.toMillis());

        log.info("Cluster-wide exclusion enabled for scheduled task {}: scope={}, lock={}, "
                + "on failure to acquire it {}, held down for {}ms after running",
                method.getName(), scope, key,
                waitMs > 0 ? "waits at most " + waitMs + "ms" : "skips the round", lockAtLeastMs);
        return new TaskSetting(key, scope, waitMs, lockAtLeastMs);
    }

    /**
     * The default lock name: class name # method name.
     *
     * <p>It uses the target's real class rather than the proxy class, or the same method would
     * yield two different lock names depending on whether a proxy is in play, and the exclusion
     * would stop working.
     */
    private static String defaultKey(Method method, ScheduledMethodRunnable runnable) {
        Class<?> targetClass = ClassUtils.getUserClass(runnable.getTarget().getClass());
        return targetClass.getSimpleName() + "#" + method.getName();
    }

    /** A task wrapped in a lock. */
    private final class ExclusiveTask implements Runnable {

        private final Runnable delegate;
        private final TaskSetting setting;

        ExclusiveTask(Runnable delegate, TaskSetting setting) {
            this.delegate = delegate;
            this.setting = setting;
        }

        @Override
        public void run() {
            ProcessingMutex mutex = syncs.mutex(setting.scope(), setting.key());

            boolean acquired;
            try {
                acquired = setting.waitMs() > 0
                        ? mutex.acquire(setting.waitMs(), TimeUnit.MILLISECONDS)
                        : mutex.tryAcquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            if (!acquired) {
                stats.recordSkipped(mutex.name());
                log.debug("Scheduled task {} skips this round: another instance in the cluster "
                        + "is already running it", setting.key());
                return;
            }

            long start = System.currentTimeMillis();
            try {
                delegate.run();
                stats.recordExecuted(mutex.name(), System.currentTimeMillis() - start);
            } catch (RuntimeException | Error e) {
                stats.recordFailed(mutex.name());
                // Rethrown for Spring's own error handling; it must not be swallowed here
                throw e;
            } finally {
                // The lock goes back whether it succeeded or failed, or the task would stop
                // running anywhere in the cluster. The cooldown holds down what remains of this
                // round, so another instance cannot run the same round again
                long elapsed = System.currentTimeMillis() - start;
                mutex.release(setting.lockAtLeastMs() - elapsed);
            }
        }
    }

    /** One task's exclusion settings. */
    private record TaskSetting(String key, Scope scope, long waitMs, long lockAtLeastMs) {
        /** The marker meaning "this task needs no exclusion". */
        static final TaskSetting NONE = new TaskSetting("", Scope.CLUSTER, 0L, 0L);
    }

    // ------------------------------------------------------------------
    // TaskScheduler's methods, each passed on to the decorated scheduler
    // ------------------------------------------------------------------

    @Override
    public ScheduledFuture<?> schedule(Runnable task, Trigger trigger) {
        return delegate.schedule(wrap(task), trigger);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable task, Instant startTime) {
        return delegate.schedule(wrap(task), startTime);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Instant startTime, Duration period) {
        return delegate.scheduleAtFixedRate(wrap(task, period), startTime, period);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Duration period) {
        return delegate.scheduleAtFixedRate(wrap(task, period), period);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Instant startTime, Duration delay) {
        return delegate.scheduleWithFixedDelay(wrap(task, delay), startTime, delay);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Duration delay) {
        return delegate.scheduleWithFixedDelay(wrap(task, delay), delay);
    }

    @Override
    public Clock getClock() {
        return delegate.getClock();
    }

    /** The decorated scheduler, for when it is needed for something else. */
    public TaskScheduler getDelegate() {
        return delegate;
    }
}
