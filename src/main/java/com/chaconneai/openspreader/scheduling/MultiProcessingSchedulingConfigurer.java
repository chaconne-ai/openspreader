package com.chaconneai.openspreader.scheduling;

import com.chaconneai.openspreader.Scope;
import com.chaconneai.openspreader.sync.ProcessingSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

/**
 * Wires {@link MultiProcessingTaskScheduler} into Spring's scheduling.
 *
 * <h2>Pluggable: installing it changes no existing behaviour</h2>
 * <ul>
 *   <li><b>It registers no {@code TaskScheduler} bean.</b> The application's own scheduler,
 *       whether auto-configured by Spring Boot or written by hand, is untouched, and an
 *       {@code @Autowired TaskScheduler} elsewhere still receives that same one. All this
 *       does is <b>wrap</b> it at the point of scheduling registration.</li>
 *   <li><b>Tasks without {@link MultiProcessingScheduled} pass straight through</b> --
 *       not even a wrapper is added, and the execution path is exactly what it would be
 *       without this package.</li>
 *   <li><b>With no scheduler found, it stays out of the way</b>, letting Spring fall back to
 *       its own logic rather than creating one on its behalf.</li>
 *   <li><b>Switched off in configuration, nothing is wired at all</b> -- this class is never
 *       even created.</li>
 * </ul>
 *
 * <p>It runs last ({@link Ordered#LOWEST_PRECEDENCE}), so the application's own
 * {@code SchedulingConfigurer} goes first: whatever scheduler it set is what gets wrapped,
 * and the user's choice is never overridden.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
@Order(Ordered.LOWEST_PRECEDENCE)
public class MultiProcessingSchedulingConfigurer implements SchedulingConfigurer {

    private static final Logger log = LoggerFactory.getLogger(MultiProcessingSchedulingConfigurer.class);

    private final ProcessingSyncService syncs;
    private final MultiProcessingTaskStats stats;
    private final ObjectProvider<TaskScheduler> schedulerProvider;
    private final boolean applyToAll;
    private final Scope defaultScope;

    public MultiProcessingSchedulingConfigurer(ProcessingSyncService syncs,
                                               MultiProcessingTaskStats stats,
                                               ObjectProvider<TaskScheduler> schedulerProvider,
                                               boolean applyToAll,
                                               Scope defaultScope) {
        this.syncs = syncs;
        this.stats = stats;
        this.schedulerProvider = schedulerProvider;
        this.applyToAll = applyToAll;
        this.defaultScope = defaultScope;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        TaskScheduler target = registrar.getScheduler();
        if (target == null) {
            // With nothing set on the registrar, take the one from the container -- Spring
            // Boot usually auto-configures one. getIfUnique deliberately: several candidates
            // mean the application has arrangements of its own, and we do not guess which
            target = schedulerProvider.getIfUnique();
        }
        if (target == null) {
            // The container has none either. Spring will fall back to a single-threaded one
            // of its own, and we stay out of it -- intervening would mean choosing the
            // scheduler for the application, which is not what "pluggable" should do
            log.info("No usable TaskScheduler was found, so cluster-exclusive scheduling is not "
                    + "enabled (@MultiProcessingScheduled will have no effect)");
            return;
        }

        registrar.setTaskScheduler(new MultiProcessingTaskScheduler(
                target, syncs, stats, applyToAll, defaultScope));
        log.info("Cluster-exclusive scheduling enabled: decorating {}, applying to {}",
                target.getClass().getSimpleName(),
                applyToAll ? "every @Scheduled task" : "only tasks annotated @MultiProcessingScheduled");
    }
}
